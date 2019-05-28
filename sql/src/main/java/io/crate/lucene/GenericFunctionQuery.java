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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * Query implementation which filters docIds by evaluating {@code condition} on each docId to verify if it matches.
 *
 * This query is very slow.
 */
class GenericFunctionQuery extends Query {

    private final Function function;
    private final java.util.function.Function<LeafReaderContext, IntPredicate> docIdMatches;

    GenericFunctionQuery(Function function, java.util.function.Function<LeafReaderContext, IntPredicate> docIdMatches) {
        this.function = function;
        this.docIdMatches = docIdMatches;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericFunctionQuery that = (GenericFunctionQuery) o;

        return function.equals(that.function);
    }

    @Override
    public int hashCode() {
        return function.hashCode();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public void extractTerms(Set<Term> terms) {
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                final Scorer s = scorer(context);
                final boolean match;
                final TwoPhaseIterator twoPhase = s.twoPhaseIterator();
                if (twoPhase == null) {
                    match = s.iterator().advance(doc) == doc;
                } else {
                    match = twoPhase.approximation().advance(doc) == doc && twoPhase.matches();
                }
                if (match) {
                    assert s.score() == 0f : "score must be 0";
                    return Explanation.match(0f, "Match on id " + doc);
                } else {
                    return Explanation.match(0f, "No match on id " + doc);
                }
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new ConstantScoreScorer(this, 0f, scoreMode, getTwoPhaseIterator(context));
            }
        };
    }

    private FilteredTwoPhaseIterator getTwoPhaseIterator(final LeafReaderContext context) throws IOException {
        return new FilteredTwoPhaseIterator(context.reader(), docIdMatches.apply(context));
    }

    @Override
    public String toString(String field) {
        return function.toString();
    }

    private static class FilteredTwoPhaseIterator extends TwoPhaseIterator {

        private final IntPredicate docIdMatches;

        FilteredTwoPhaseIterator(LeafReader reader, IntPredicate docIdMatches) {
            super(DocIdSetIterator.all(reader.maxDoc()));
            this.docIdMatches = docIdMatches;
        }

        @Override
        public boolean matches() throws IOException {
            return docIdMatches.test(approximation.docID());
        }

        @Override
        public float matchCost() {
            // Arbitrary number, we don't have a way to get the cost of the condition
            return 10;
        }
    }
}
