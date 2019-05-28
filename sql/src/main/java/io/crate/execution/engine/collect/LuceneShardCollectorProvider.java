/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect;

import io.crate.data.BatchIterator;
import io.crate.data.CompositeBatchIterator;
import io.crate.data.Row;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.engine.collect.collectors.LuceneOrderedDocCollector;
import io.crate.execution.engine.collect.collectors.OptimizeQueryForSearchAfter;
import io.crate.execution.engine.collect.collectors.OrderedDocCollector;
import io.crate.execution.engine.collect.collectors.SegmentBatchIterator;
import io.crate.execution.engine.sort.LuceneSortGenerator;
import io.crate.execution.jobs.NodeJobsCounter;
import io.crate.execution.jobs.SharedShardContext;
import io.crate.expression.InputFactory;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.LuceneReferenceResolver;
import io.crate.expression.reference.sys.shard.ShardRowContext;
import io.crate.expression.symbol.Symbols;
import io.crate.lucene.FieldTypeLookup;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocSysColumns;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public class LuceneShardCollectorProvider extends ShardCollectorProvider {

    private static final Logger LOGGER = LogManager.getLogger(LuceneShardCollectorProvider.class);

    private final Supplier<String> localNodeId;
    private final LuceneQueryBuilder luceneQueryBuilder;
    private final Functions functions;
    private final IndexShard indexShard;
    private final DocInputFactory docInputFactory;
    private final BigArrays bigArrays;
    private final FieldTypeLookup fieldTypeLookup;

    public LuceneShardCollectorProvider(Schemas schemas,
                                        LuceneQueryBuilder luceneQueryBuilder,
                                        ClusterService clusterService,
                                        NodeJobsCounter nodeJobsCounter,
                                        Functions functions,
                                        ThreadPool threadPool,
                                        Settings settings,
                                        TransportActionProvider transportActionProvider,
                                        IndexShard indexShard,
                                        BigArrays bigArrays) {
        super(clusterService, schemas, nodeJobsCounter, functions, threadPool, settings, transportActionProvider, indexShard,
            new ShardRowContext(indexShard, clusterService), bigArrays);
        this.luceneQueryBuilder = luceneQueryBuilder;
        this.functions = functions;
        this.indexShard = indexShard;
        this.localNodeId = () -> clusterService.localNode().getId();
        fieldTypeLookup = indexShard.mapperService()::fullName;
        this.docInputFactory = new DocInputFactory(
            functions,
            fieldTypeLookup,
            new LuceneReferenceResolver(fieldTypeLookup)
        );
        this.bigArrays = bigArrays;
    }

    @Override
    protected BatchIterator<Row> getUnorderedIterator(RoutedCollectPhase collectPhase,
                                                      boolean requiresScroll,
                                                      CollectTask collectTask) {
        ShardId shardId = indexShard.shardId();
        SharedShardContext sharedShardContext = collectTask.sharedShardContexts().getOrCreateContext(shardId);
        Engine.Searcher searcher = sharedShardContext.acquireSearcher();
        IndexShard indexShard = sharedShardContext.indexShard();
        try {
            QueryShardContext queryShardContext =
                sharedShardContext.indexService().newQueryShardContext(System::currentTimeMillis);
            LuceneQueryBuilder.Context queryContext = luceneQueryBuilder.convert(
                collectPhase.where(),
                collectTask.txnCtx(),
                indexShard.mapperService(),
                queryShardContext,
                sharedShardContext.indexService().cache()
            );
            collectTask.addSearcher(sharedShardContext.readerId(), searcher);

            boolean doScores = Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE);
            ScoreMode scoreMode = doScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;

            IndexSearcher indexSearcher = searcher.searcher();
            CollectorContext collectorContext = getCollectorContext(
                sharedShardContext.readerId(), queryShardContext::getForField);
            Weight weight = indexSearcher.createWeight(indexSearcher.rewrite(queryContext.query()), scoreMode, 1f);

            ArrayList<BatchIterator<Row>> segmentBatchIterator = new ArrayList<>();
            for (LeafReaderContext leaf : indexSearcher.getTopReaderContext().leaves()) {
                Scorer scorer = weight.scorer(leaf);
                if (scorer == null) {
                    continue;
                }
                InputFactory.Context<? extends LuceneCollectorExpression<?>> docCtx =
                    docInputFactory.extractImplementations(collectTask.txnCtx(), collectPhase);
                InputRow inputRow = new InputRow(docCtx.topLevelInputs());
                List<? extends LuceneCollectorExpression<?>> expressions = docCtx.expressions();
                for (LuceneCollectorExpression<?> expression : expressions) {
                    expression.startCollect(collectorContext);
                    expression.setScorer(scorer);
                    expression.setNextReader(leaf);
                }
                IntFunction<Row> rowFromDocId = docId -> {
                    for (int i = 0; i < expressions.size(); i++) {
                        try {
                            expressions.get(i).setNextDocId(docId);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                    return inputRow;
                };
                segmentBatchIterator.add(new SegmentBatchIterator(
                    scorer,
                    leaf,
                    rowFromDocId,
                    queryContext.minScore()
                ));
            }
            //noinspection unchecked
            return CompositeBatchIterator.seqComposite(segmentBatchIterator.toArray(new BatchIterator[0]));
        } catch (IOException e) {
            searcher.close();
            throw new UncheckedIOException(e);
        } catch (Throwable t) {
            searcher.close();
            throw t;
        }
    }

    @Nullable
    @Override
    protected BatchIterator<Row> getProjectionFusedIterator(RoutedCollectPhase normalizedPhase, CollectTask collectTask) {
        return GroupByOptimizedIterator.tryOptimizeSingleStringKey(
            indexShard,
            luceneQueryBuilder,
            fieldTypeLookup,
            bigArrays,
            new InputFactory(functions),
            docInputFactory,
            normalizedPhase,
            collectTask
        );
    }

    @Override
    public OrderedDocCollector getOrderedCollector(RoutedCollectPhase phase,
                                                   SharedShardContext sharedShardContext,
                                                   CollectTask collectTask,
                                                   boolean requiresRepeat) {
        RoutedCollectPhase collectPhase = phase.normalize(shardNormalizer, null);

        CollectorContext collectorContext;
        InputFactory.Context<? extends LuceneCollectorExpression<?>> ctx;
        Engine.Searcher searcher = null;
        LuceneQueryBuilder.Context queryContext;
        try {
            searcher = sharedShardContext.acquireSearcher();
            IndexService indexService = sharedShardContext.indexService();
            QueryShardContext queryShardContext = indexService.newQueryShardContext(System::currentTimeMillis);
            queryContext = luceneQueryBuilder.convert(
                collectPhase.where(),
                collectTask.txnCtx(),
                indexService.mapperService(),
                queryShardContext,
                indexService.cache()
            );
            collectTask.addSearcher(sharedShardContext.readerId(), searcher);
            ctx = docInputFactory.extractImplementations(collectTask.txnCtx(), collectPhase);
            collectorContext = getCollectorContext(sharedShardContext.readerId(), queryShardContext::getForField);
        } catch (Throwable t) {
            if (searcher != null) {
                searcher.close();
            }
            throw t;
        }
        int batchSize = collectPhase.shardQueueSize(localNodeId.get());
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("[{}][{}] creating LuceneOrderedDocCollector. Expected number of rows to be collected: {}",
                sharedShardContext.indexShard().routingEntry().currentNodeId(),
                sharedShardContext.indexShard().shardId(),
                batchSize);
        }
        OptimizeQueryForSearchAfter optimizeQueryForSearchAfter = new OptimizeQueryForSearchAfter(
            collectPhase.orderBy(),
            queryContext.queryShardContext(),
            fieldTypeLookup
        );
        return new LuceneOrderedDocCollector(
            indexShard.shardId(),
            searcher.searcher(),
            queryContext.query(),
            queryContext.minScore(),
            Symbols.containsColumn(collectPhase.toCollect(), DocSysColumns.SCORE),
            batchSize,
            collectorContext,
            optimizeQueryForSearchAfter,
            LuceneSortGenerator.generateLuceneSort(collectTask.txnCtx(), collectorContext, collectPhase.orderBy(), docInputFactory, fieldTypeLookup),
            ctx.topLevelInputs(),
            ctx.expressions()
        );
    }

    static CollectorContext getCollectorContext(int readerId,
                                                Function<MappedFieldType, IndexFieldData<?>> getFieldData) {
        return new CollectorContext(getFieldData, readerId);
    }
}
