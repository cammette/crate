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

package io.crate.data;

import com.google.common.collect.Iterables;
import io.crate.concurrent.CompletableFutures;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.function.IntSupplier;

import static io.crate.concurrent.CompletableFutures.supplyAsync;

/**
 * BatchIterator implementations backed by multiple other BatchIterators.
 */
public final class CompositeBatchIterator {

    /**
     * Composite batchIterator that consumes each individual iterator fully before moving to the next.
     */
    @SafeVarargs
    public static <T> BatchIterator<T> seqComposite(BatchIterator<T> ... iterators) {
        if (iterators.length == 0) {
            return InMemoryBatchIterator.empty(null);
        }
        if (iterators.length == 1) {
            return iterators[0];
        }
        // prefer loaded iterators over unloaded to improve performance in case only a subset of data is consumed
        Comparator<BatchIterator<T>> comparing = Comparator.comparing(BatchIterator::allLoaded);
        Arrays.sort(iterators, comparing.reversed());
        return new SeqCompositeBI<>(iterators);
    }

    /**
     * Composite batchIterator that eagerly loads the individual iterators on `loadNext` multi-threaded
     */
    @SafeVarargs
    public static <T> BatchIterator<T> asyncComposite(Executor executor,
                                                      IntSupplier availableThreads,
                                                      BatchIterator<T> ... iterators) {
        if (iterators.length == 1) {
            return iterators[0];
        }
        return new AsyncCompositeBI<>(executor, availableThreads, iterators);
    }

    private abstract static class AbstractCompositeBI<T> implements BatchIterator<T> {

        protected final BatchIterator<T>[] iterators;
        protected int idx = 0;

        AbstractCompositeBI(BatchIterator<T>[] iterators) {
            assert iterators.length > 0 : "Must have at least 1 iterator";
            this.iterators = iterators;
        }

        @Override
        public T currentElement() {
            return iterators[idx].currentElement();
        }

        @Override
        public void moveToStart() {
            for (BatchIterator iterator : iterators) {
                iterator.moveToStart();
            }
            idx = 0;
        }

        @Override
        public void close() {
            for (BatchIterator iterator : iterators) {
                iterator.close();
            }
        }

        @Override
        public boolean allLoaded() {
            for (BatchIterator iterator : iterators) {
                if (iterator.allLoaded() == false) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void kill(@Nonnull Throwable throwable) {
            for (BatchIterator iterator : iterators) {
                iterator.kill(throwable);
            }
        }

        @Override
        public boolean involvesIO() {
            for (BatchIterator iterator : iterators) {
                if (iterator.involvesIO()) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class SeqCompositeBI<T> extends AbstractCompositeBI<T> {

        SeqCompositeBI(BatchIterator<T>[] iterators) {
            super(iterators);
        }

        @Override
        public boolean moveNext() {
            while (idx < iterators.length) {
                BatchIterator iterator = iterators[idx];
                if (iterator.moveNext()) {
                    return true;
                }
                if (iterator.allLoaded() == false) {
                    return false;
                }
                idx++;
            }
            idx = 0;
            return false;
        }

        @Override
        public CompletionStage<?> loadNextBatch() {
            for (BatchIterator iterator : iterators) {
                if (iterator.allLoaded()) {
                    continue;
                }
                return iterator.loadNextBatch();
            }
            return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already fully loaded"));
        }
    }

    private static class AsyncCompositeBI<T> extends AbstractCompositeBI<T> {

        private final Executor executor;
        private final IntSupplier availableThreads;

        AsyncCompositeBI(Executor executor, IntSupplier availableThreads, BatchIterator<T>[] iterators) {
            super(iterators);
            this.executor = executor;
            this.availableThreads = availableThreads;
        }

        @Override
        public boolean moveNext() {
            while (idx < iterators.length) {
                BatchIterator iterator = iterators[idx];
                if (iterator.moveNext()) {
                    return true;
                }
                idx++;
            }
            idx = 0;
            return false;
        }

        @Override
        public CompletionStage<?> loadNextBatch() {
            if (allLoaded()) {
                return CompletableFutures.failedFuture(new IllegalStateException("BatchIterator already loaded"));
            }
            int availableThreads = this.availableThreads.getAsInt();
            List<BatchIterator<T>> itToLoad = getIteratorsToLoad(iterators);

            List<CompletableFuture<CompletableFuture>> nestedFutures = new ArrayList<>();
            if (availableThreads < itToLoad.size()) {
                Iterable<List<BatchIterator<T>>> iteratorsPerThread = Iterables.partition(
                    itToLoad, itToLoad.size() / availableThreads);

                for (List<BatchIterator<T>> batchIterators: iteratorsPerThread) {
                    CompletableFuture<CompletableFuture> future = supplyAsync(() -> {
                        ArrayList<CompletableFuture<?>> futures = new ArrayList<>(batchIterators.size());
                        for (BatchIterator<T> batchIterator: batchIterators) {
                            futures.add(batchIterator.loadNextBatch().toCompletableFuture());
                        }
                        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
                    }, executor);
                    nestedFutures.add(future);
                }
            } else {
                for (BatchIterator<T> iterator: itToLoad) {
                    nestedFutures.add(supplyAsync(() -> iterator.loadNextBatch().toCompletableFuture(), executor));
                }
            }
            return CompletableFutures.allAsList(nestedFutures)
                .thenCompose(innerFutures -> CompletableFuture.allOf(innerFutures.toArray(new CompletableFuture[0])));
        }

        private static <T> List<BatchIterator<T>> getIteratorsToLoad(BatchIterator<T>[] allIterators) {
            ArrayList<BatchIterator<T>> itToLoad = new ArrayList<>(allIterators.length);
            for (BatchIterator<T> iterator: allIterators) {
                if (!iterator.allLoaded()) {
                    itToLoad.add(iterator);
                }
            }
            return itToLoad;
        }
    }
}
