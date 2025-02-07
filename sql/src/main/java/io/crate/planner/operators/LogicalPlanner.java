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

package io.crate.planner.operators;

import io.crate.analyze.AnalyzedStatement;
import io.crate.analyze.AnalyzedStatementVisitor;
import io.crate.analyze.InsertFromSubQueryAnalyzedStatement;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AliasedAnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.relations.UnionSelect;
import io.crate.analyze.where.DocKeys;
import io.crate.data.Row;
import io.crate.data.RowConsumer;
import io.crate.execution.MultiPhaseExecutor;
import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.dsl.projection.builder.SplitPoints;
import io.crate.execution.dsl.projection.builder.SplitPointsBuilder;
import io.crate.execution.engine.NodeOperationTreeGenerator;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.FieldsVisitor;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefVisitor;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.ExecutionPlan;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.consumer.FetchMode;
import io.crate.planner.consumer.InsertFromSubQueryPlanner;
import io.crate.planner.optimizer.Optimizer;
import io.crate.planner.optimizer.rule.DeduplicateOrder;
import io.crate.planner.optimizer.rule.MergeAggregateAndCollectToCount;
import io.crate.planner.optimizer.rule.MergeFilterAndCollect;
import io.crate.planner.optimizer.rule.MergeFilters;
import io.crate.planner.optimizer.rule.MoveFilterBeneathBoundary;
import io.crate.planner.optimizer.rule.MoveFilterBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveFilterBeneathGroupBy;
import io.crate.planner.optimizer.rule.MoveFilterBeneathHashJoin;
import io.crate.planner.optimizer.rule.MoveFilterBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveFilterBeneathOrder;
import io.crate.planner.optimizer.rule.MoveFilterBeneathProjectSet;
import io.crate.planner.optimizer.rule.MoveFilterBeneathUnion;
import io.crate.planner.optimizer.rule.MoveFilterBeneathWindowAgg;
import io.crate.planner.optimizer.rule.MoveOrderBeneathBoundary;
import io.crate.planner.optimizer.rule.MoveOrderBeneathFetchOrEval;
import io.crate.planner.optimizer.rule.MoveOrderBeneathNestedLoop;
import io.crate.planner.optimizer.rule.MoveOrderBeneathUnion;
import io.crate.planner.optimizer.rule.RemoveRedundantFetchOrEval;
import io.crate.planner.optimizer.rule.RewriteFilterOnOuterJoinToInnerJoin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static io.crate.expression.symbol.SelectSymbol.ResultType.SINGLE_COLUMN_SINGLE_VALUE;

/**
 * Planner which can create a {@link ExecutionPlan} using intermediate {@link LogicalPlan} nodes.
 */
public class LogicalPlanner {

    public static final int NO_LIMIT = -1;

    private final Optimizer optimizer;
    private final TableStats tableStats;
    private final Visitor statementVisitor = new Visitor();
    private final Functions functions;
    private final RelationNormalizer relationNormalizer;

    public LogicalPlanner(Functions functions, TableStats tableStats) {
        this.optimizer = new Optimizer(List.of(
            new RemoveRedundantFetchOrEval(),
            new MergeAggregateAndCollectToCount(),
            new MergeFilters(),
            new MoveFilterBeneathBoundary(),
            new MoveFilterBeneathFetchOrEval(),
            new MoveFilterBeneathOrder(),
            new MoveFilterBeneathProjectSet(),
            new MoveFilterBeneathHashJoin(),
            new MoveFilterBeneathNestedLoop(),
            new MoveFilterBeneathUnion(),
            new MoveFilterBeneathGroupBy(),
            new MoveFilterBeneathWindowAgg(),
            new MergeFilterAndCollect(),
            new RewriteFilterOnOuterJoinToInnerJoin(functions),
            new MoveOrderBeneathUnion(),
            new MoveOrderBeneathNestedLoop(),
            new MoveOrderBeneathBoundary(),
            new MoveOrderBeneathFetchOrEval(),
            new DeduplicateOrder()
        ));
        this.tableStats = tableStats;
        this.functions = functions;
        this.relationNormalizer = new RelationNormalizer(functions);
    }

    public LogicalPlan plan(AnalyzedStatement statement, PlannerContext plannerContext) {
        return statementVisitor.process(statement, plannerContext);
    }

    public LogicalPlan planSubSelect(SelectSymbol selectSymbol, PlannerContext plannerContext) {
        AnalyzedRelation relation = relationNormalizer.normalize(
            selectSymbol.relation(), plannerContext.transactionContext());

        final int fetchSize;
        final java.util.function.Function<LogicalPlan, LogicalPlan> maybeApplySoftLimit;
        if (selectSymbol.getResultType() == SINGLE_COLUMN_SINGLE_VALUE) {
            // SELECT (SELECT foo FROM t)
            //         ^^^^^^^^^^^^^^^^^
            // The subquery must return at most 1 row, if more than 1 row is returned semantics require us to throw an error.
            // So we limit the query to 2 if there is no limit to avoid retrieval of many rows while being able to validate max1row
            fetchSize = 2;
            maybeApplySoftLimit = relation.limit() == null
                ? plan -> new Limit(plan, Literal.of(2L), Literal.of(0L))
                : plan -> plan;
        } else {
            fetchSize = 0;
            maybeApplySoftLimit = plan -> plan;
        }
        PlannerContext subSelectPlannerContext = PlannerContext.forSubPlan(plannerContext, fetchSize);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner(s -> planSubSelect(s, subSelectPlannerContext));
        LogicalPlan.Builder planBuilder = prePlan(
            relation,
            FetchMode.NEVER_CLEAR,
            subqueryPlanner,
            true,
            functions,
            plannerContext.transactionContext());

        planBuilder = tryOptimizeForInSubquery(selectSymbol, relation, planBuilder);
        LogicalPlan optimizedPlan = optimizer.optimize(maybeApplySoftLimit.apply(planBuilder.build(tableStats,
                                                                                                   Set.of(),
                                                                                                   Collections.emptySet())));
        return new RootRelationBoundary(optimizedPlan);
    }

    // In case the subselect is inside an IN() or = ANY() apply a "natural" OrderBy to optimize
    // the building of TermInSetQuery which does a sort on the collection of values.
    // See issue https://github.com/crate/crate/issues/6755
    // If the output values are already sorted (even in desc order) no optimization is needed
    private LogicalPlan.Builder tryOptimizeForInSubquery(SelectSymbol selectSymbol, AnalyzedRelation relation, LogicalPlan.Builder planBuilder) {
        if (selectSymbol.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
            OrderBy relationOrderBy = relation.orderBy();
            if (relationOrderBy == null ||
                relationOrderBy.orderBySymbols().get(0).equals(relation.outputs().get(0)) == false) {
                return Order.create(planBuilder, new OrderBy(relation.outputs()));
            }
        }
        return planBuilder;
    }


    public LogicalPlan normalizeAndPlan(AnalyzedRelation analyzedRelation,
                                        PlannerContext plannerContext,
                                        SubqueryPlanner subqueryPlanner,
                                        FetchMode fetchMode,
                                        Set<PlanHint> hints) {
        CoordinatorTxnCtx coordinatorTxnCtx = plannerContext.transactionContext();
        AnalyzedRelation relation = relationNormalizer.normalize(analyzedRelation, coordinatorTxnCtx);
        LogicalPlan logicalPlan = plan(relation, fetchMode, subqueryPlanner, true, functions, coordinatorTxnCtx)
            .build(tableStats, hints, new HashSet<>(relation.outputs()));

        return optimizer.optimize(logicalPlan);
    }

    static LogicalPlan.Builder plan(AnalyzedRelation relation,
                                    FetchMode fetchMode,
                                    SubqueryPlanner subqueryPlanner,
                                    boolean isLastFetch,
                                    Functions functions,
                                    CoordinatorTxnCtx txnCtx) {
        LogicalPlan.Builder builder = prePlan(relation, fetchMode, subqueryPlanner, isLastFetch, functions, txnCtx);
        if (isLastFetch) {
            return builder;
        }
        return RelationBoundary.create(builder, relation);
    }

    private static LogicalPlan.Builder prePlan(AnalyzedRelation relation,
                                               FetchMode fetchMode,
                                               SubqueryPlanner subqueryPlanner,
                                               boolean isLastFetch,
                                               Functions functions,
                                               CoordinatorTxnCtx txnCtx) {
        SplitPoints splitPoints = SplitPointsBuilder.create(relation);
        return MultiPhase.createIfNeeded(
            FetchOrEval.create(
                Limit.create(
                    Order.create(
                        Distinct.create(
                            ProjectSet.create(
                                WindowAgg.create(
                                    Filter.create(
                                        groupByOrAggregate(
                                            collectAndFilter(
                                                relation,
                                                splitPoints.toCollect(),
                                                relation.where(),
                                                subqueryPlanner,
                                                fetchMode,
                                                functions,
                                                txnCtx
                                            ),
                                            relation.groupBy(),
                                            splitPoints.aggregates()
                                        ),
                                        relation.having()
                                    ),
                                    splitPoints.windowFunctions()
                                ),
                                splitPoints.tableFunctions()
                            ),
                            relation.isDistinct(),
                            relation.outputs()
                        ),
                        relation.orderBy()
                    ),
                    relation.limit(),
                    relation.offset()
                ),
                relation.outputs(),
                fetchMode,
                isLastFetch,
                relation.limit() != null
            ),
            relation,
            subqueryPlanner
        );
    }

    private static LogicalPlan.Builder groupByOrAggregate(LogicalPlan.Builder source,
                                                          List<Symbol> groupKeys,
                                                          List<Function> aggregates) {
        if (!groupKeys.isEmpty()) {
            return GroupHashAggregate.create(source, groupKeys, aggregates);
        }
        if (!aggregates.isEmpty()) {
            return (tableStats, hints, usedColumns) ->
                new HashAggregate(source.build(tableStats, hints, extractColumns(aggregates)), aggregates);
        }
        return source;
    }

    private static LogicalPlan.Builder collectAndFilter(AnalyzedRelation analyzedRelation,
                                                        List<Symbol> toCollect,
                                                        WhereClause where,
                                                        SubqueryPlanner subqueryPlanner,
                                                        FetchMode fetchMode,
                                                        Functions functions,
                                                        CoordinatorTxnCtx txnCtx) {
        if (analyzedRelation instanceof AnalyzedView) {
            return plan(((AnalyzedView) analyzedRelation).relation(), fetchMode, subqueryPlanner, false, functions, txnCtx);
        }
        if (analyzedRelation instanceof AliasedAnalyzedRelation) {
            return plan(((AliasedAnalyzedRelation) analyzedRelation).relation(), fetchMode, subqueryPlanner, false, functions, txnCtx);
        }
        if (analyzedRelation instanceof AbstractTableRelation) {
            return Collect.create(((AbstractTableRelation) analyzedRelation), toCollect, where);
        }
        if (analyzedRelation instanceof MultiSourceSelect) {
            return JoinPlanBuilder.createNodes((MultiSourceSelect) analyzedRelation, where, subqueryPlanner, functions, txnCtx);
        }
        if (analyzedRelation instanceof UnionSelect) {
            return Union.create((UnionSelect) analyzedRelation, subqueryPlanner, functions, txnCtx);
        }
        if (analyzedRelation instanceof QueriedSelectRelation) {
            QueriedSelectRelation<?> selectRelation = (QueriedSelectRelation) analyzedRelation;

            AnalyzedRelation subRelation = selectRelation.subRelation();
            if (subRelation instanceof DocTableRelation) {
                DocTableRelation docTableRelation = (DocTableRelation) subRelation;
                EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
                    functions, RowGranularity.CLUSTER, null, docTableRelation);

                WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
                    normalizer,
                    where.queryOrFallback(),
                    docTableRelation.tableInfo(),
                    txnCtx
                );

                Optional<DocKeys> docKeys = detailedQuery.docKeys();
                if (docKeys.isPresent()) {
                    return (tableStats, hints, usedBeforeNextFetch) ->
                        new Get(docTableRelation, docKeys.get(), toCollect, tableStats);
                }
                return Collect.create(docTableRelation, toCollect, new WhereClause(
                    detailedQuery.query(),
                    where.partitions(),
                    detailedQuery.clusteredBy()
                ));
            } else if (subRelation instanceof TableRelation) {
                return Collect.create(((TableRelation) subRelation), toCollect, where);
            }
            return Filter.create(
                plan(subRelation, fetchMode, subqueryPlanner, false, functions, txnCtx),
                where
            );
        }
        throw new UnsupportedOperationException("Cannot create LogicalPlan from: " + analyzedRelation);
    }

    public static Set<Symbol> extractColumns(Symbol symbol) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        RefVisitor.visitRefs(symbol, columns::add);
        FieldsVisitor.visitFields(symbol, columns::add);
        return columns;
    }

    public static Set<Symbol> extractColumns(Collection<? extends Symbol> symbols) {
        LinkedHashSet<Symbol> columns = new LinkedHashSet<>();
        for (Symbol symbol : symbols) {
            RefVisitor.visitRefs(symbol, columns::add);
            FieldsVisitor.visitFields(symbol, columns::add);
        }
        return columns;
    }

    public static void execute(LogicalPlan logicalPlan,
                               DependencyCarrier executor,
                               PlannerContext plannerContext,
                               RowConsumer consumer,
                               Row params,
                               SubQueryResults subQueryResults,
                               boolean enableProfiling) {
        if (logicalPlan.dependencies().isEmpty()) {
            doExecute(logicalPlan, executor, plannerContext, consumer, params, subQueryResults, enableProfiling);
        } else {
            MultiPhaseExecutor.execute(logicalPlan.dependencies(), executor, plannerContext, params)
                .whenComplete((valueBySubQuery, failure) -> {
                    if (failure == null) {
                        doExecute(logicalPlan, executor, plannerContext, consumer, params, valueBySubQuery, false);
                    } else {
                        consumer.accept(null, failure);
                    }
                });
        }
    }

    private static void doExecute(LogicalPlan logicalPlan,
                                  DependencyCarrier dependencies,
                                  PlannerContext plannerContext,
                                  RowConsumer consumer,
                                  Row params,
                                  SubQueryResults subQueryResults,
                                  boolean enableProfiling) {
        NodeOperationTree nodeOpTree = getNodeOperationTree(logicalPlan, dependencies, plannerContext, params, subQueryResults);
        executeNodeOpTree(
            dependencies,
            plannerContext.transactionContext(),
            plannerContext.jobId(),
            consumer,
            enableProfiling,
            nodeOpTree
        );
    }

    public static NodeOperationTree getNodeOperationTree(LogicalPlan logicalPlan,
                                                         DependencyCarrier executor,
                                                         PlannerContext plannerContext,
                                                         Row params,
                                                         SubQueryResults subQueryResults) {
        ExecutionPlan executionPlan = logicalPlan.build(
            plannerContext, executor.projectionBuilder(), -1, 0, null, null, params, subQueryResults);
        return NodeOperationTreeGenerator.fromPlan(executionPlan, executor.localNodeId());
    }

    public static void executeNodeOpTree(DependencyCarrier dependencies,
                                         TransactionContext txnCtx,
                                         UUID jobId,
                                         RowConsumer consumer,
                                         boolean enableProfiling,
                                         NodeOperationTree nodeOpTree) {
        dependencies.phasesTaskFactory()
            .create(jobId, Collections.singletonList(nodeOpTree), enableProfiling)
            .execute(consumer, txnCtx);
    }

    private class Visitor extends AnalyzedStatementVisitor<PlannerContext, LogicalPlan> {

        @Override
        protected LogicalPlan visitAnalyzedStatement(AnalyzedStatement analyzedStatement, PlannerContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "Cannot create LogicalPlan from AnalyzedStatement \"%s\"  - not supported.", analyzedStatement));
        }

        @Override
        public LogicalPlan visitSelectStatement(AnalyzedRelation relation, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            LogicalPlan logicalPlan = normalizeAndPlan(relation, context, subqueryPlanner, FetchMode.MAYBE_CLEAR, Set.of());
            return new RootRelationBoundary(logicalPlan);
        }

        @Override
        protected LogicalPlan visitInsertFromSubQueryStatement(InsertFromSubQueryAnalyzedStatement statement, PlannerContext context) {
            SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> planSubSelect(s, context));
            return InsertFromSubQueryPlanner.plan(
                relationNormalizer, statement, context, LogicalPlanner.this, subqueryPlanner);
        }
    }
}
