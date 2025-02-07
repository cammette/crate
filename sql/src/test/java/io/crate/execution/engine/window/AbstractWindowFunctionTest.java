/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.window;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.FrameBoundDefinition;
import io.crate.analyze.OrderBy;
import io.crate.analyze.SymbolEvaluator;
import io.crate.analyze.WindowFrameDefinition;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.auth.user.User;
import io.crate.breaker.RamAccountingContext;
import io.crate.common.collections.Lists2;
import io.crate.data.BatchIterator;
import io.crate.data.BatchIterators;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.ExpressionsInput;
import io.crate.expression.InputFactory;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.QualifiedName;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.SqlExpressions;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.inject.AbstractModule;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.crate.data.SentinelRow.SENTINEL;
import static io.crate.execution.engine.sort.Comparators.createComparator;
import static io.crate.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.crate.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static io.crate.sql.tree.WindowFrame.Mode.RANGE;
import static org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.hamcrest.Matchers.instanceOf;

public abstract class AbstractWindowFunctionTest extends CrateDummyClusterServiceUnitTest {

    static final WindowFrameDefinition RANGE_CURRENT_ROW_UNBOUNDED_FOLLOWING = new WindowFrameDefinition(
        RANGE,
        new FrameBoundDefinition(CURRENT_ROW, Literal.NULL),
        new FrameBoundDefinition(UNBOUNDED_FOLLOWING, Literal.NULL)
    );

    private RamAccountingContext RAM_ACCOUNTING_CONTEXT = new RamAccountingContext
        ("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private AbstractModule[] additionalModules;
    private SqlExpressions sqlExpressions;
    private Functions functions;
    private TransactionContext txnCtx = CoordinatorTxnCtx.systemTransactionContext();
    private InputFactory inputFactory;

    public AbstractWindowFunctionTest(AbstractModule... additionalModules) {
        this.additionalModules = additionalModules;
    }

    @Before
    public void prepareFunctions() {
        final String tableName = "t1";
        DocTableInfo tableInfo = SQLExecutor.tableInfo(
            new RelationName("doc", "t1"),
            "create table doc.t1 (x int, y bigint, z string, d double)",
            clusterService);
        DocTableRelation tableRelation = new DocTableRelation(tableInfo);
        Map<QualifiedName, AnalyzedRelation> tableSources = ImmutableMap.of(new QualifiedName(tableName), tableRelation);
        sqlExpressions = new SqlExpressions(tableSources, tableRelation,
            null, User.CRATE_USER, additionalModules);
        functions = sqlExpressions.functions();
        inputFactory = new InputFactory(functions);
    }

    private static void performInputSanityChecks(Object[]... inputs) {
        List<Integer> inputSizes = Arrays.stream(inputs)
            .map(Array::getLength)
            .distinct()
            .collect(Collectors.toList());

        if (inputSizes.size() != 1) {
            throw new IllegalArgumentException("Inputs need to be of equal size");
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> void assertEvaluate(String functionExpression,
                                      Matcher<T> expectedValue,
                                      List<ColumnIdent> rowsColumnDescription,
                                      Object[]... inputRows) throws Throwable {
        performInputSanityChecks(inputRows);

        Symbol normalizedFunctionSymbol = sqlExpressions.normalize(sqlExpressions.asSymbol(functionExpression));
        assertThat(normalizedFunctionSymbol, instanceOf(io.crate.expression.symbol.WindowFunction.class));

        var windowFunctionSymbol = (io.crate.expression.symbol.WindowFunction) normalizedFunctionSymbol;
        ReferenceResolver<InputCollectExpression> referenceResolver =
            r -> new InputCollectExpression(rowsColumnDescription.indexOf(r.column()));

        var sourceSymbols = Lists2.map(rowsColumnDescription, x -> sqlExpressions.normalize(sqlExpressions.asSymbol(x.sqlFqn())));
        ensureInputRowsHaveCorrectType(sourceSymbols, inputRows);
        var argsCtx = inputFactory.ctxForRefs(txnCtx, referenceResolver);
        argsCtx.add(windowFunctionSymbol.arguments());

        FunctionImplementation impl = functions.getQualified(windowFunctionSymbol.info().ident());
        assert impl instanceof WindowFunction || impl instanceof AggregationFunction: "Got " + impl + " but expected a window function";
        WindowFunction windowFunctionImpl;
        if (impl instanceof AggregationFunction) {
            windowFunctionImpl = new AggregateToWindowFunctionAdapter(
                (AggregationFunction) impl,
                new ExpressionsInput<>(Literal.BOOLEAN_TRUE, List.of()),
                Version.CURRENT,
                NON_RECYCLING_INSTANCE,
                RAM_ACCOUNTING_CONTEXT
            );
        } else {
            windowFunctionImpl = (WindowFunction) impl;
        }

        int numCellsInSourceRows = inputRows[0].length;
        InputColumns.SourceSymbols inputColSources = new InputColumns.SourceSymbols(sourceSymbols);
        var windowDef = windowFunctionSymbol.windowDefinition();
        var partitionOrderBy = windowDef.partitions().isEmpty() ? null : new OrderBy(windowDef.partitions());
        Object startOffsetValue = SymbolEvaluator.evaluate(
            txnCtx, functions, windowDef.windowFrameDefinition().start().value(), Row.EMPTY, SubQueryResults.EMPTY);
        Object endOffsetValue = SymbolEvaluator.evaluate(
            txnCtx, functions, windowDef.windowFrameDefinition().end().value(), Row.EMPTY, SubQueryResults.EMPTY);
        BatchIterator<Row> iterator = WindowFunctionBatchIterator.of(
            InMemoryBatchIterator.of(Arrays.stream(inputRows).map(RowN::new).collect(Collectors.toList()), SENTINEL),
            new IgnoreRowAccounting(),
            windowDef.map(s -> InputColumns.create(s, inputColSources)),
            startOffsetValue,
            endOffsetValue,
            createComparator(() -> inputFactory.ctxForRefs(txnCtx, referenceResolver), partitionOrderBy),
            createComparator(() -> inputFactory.ctxForRefs(txnCtx, referenceResolver), windowDef.orderBy()),
            numCellsInSourceRows,
            () -> 1,
            Runnable::run,
            List.of(windowFunctionImpl),
            argsCtx.expressions(),
            argsCtx.topLevelInputs().toArray(new Input[0])
        );
        List<Object> actualResult;
        try {
            actualResult = BatchIterators.collect(
                iterator,
                Collectors.mapping(row -> row.get(numCellsInSourceRows), Collectors.toList())).get(5, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw e.getCause();
        }
        assertThat((T) actualResult, expectedValue);
    }

    private static void ensureInputRowsHaveCorrectType(List<Symbol> sourceSymbols, Object[][] inputRows) {
        for (int i = 0; i < sourceSymbols.size(); i++) {
            for (Object[] inputRow : inputRows) {
                inputRow[i] = sourceSymbols.get(i).valueType().value(inputRow[i]);
            }
        }
    }
}
