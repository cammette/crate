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

package io.crate.metadata.table;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public class ColumnRegistrar<T> {

    private final SortedMap<ColumnIdent, Reference> infos;
    private final ImmutableSortedSet.Builder<Reference> columnsBuilder;
    private final ImmutableSortedMap.Builder<ColumnIdent, RowCollectExpressionFactory<T>> expressionBuilder;

    private final RelationName relationName;
    private final RowGranularity rowGranularity;

    private int position = 1;

    public ColumnRegistrar(RelationName relationName, RowGranularity rowGranularity) {
        this.relationName = relationName;
        this.rowGranularity = rowGranularity;
        this.infos = new TreeMap<>();
        this.columnsBuilder = ImmutableSortedSet.orderedBy(Reference.COMPARE_BY_COLUMN_IDENT);
        this.expressionBuilder = ImmutableSortedMap.naturalOrder();
    }

    public ColumnRegistrar<T> register(String column, DataType type) {
        return register(column, type, true, null);
    }

    public <R> ColumnRegistrar<T> register(ColumnIdent column,
                                           DataType<R> type,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        return register(column, type, true, expression);
    }

    public <R> ColumnRegistrar<T> register(String column,
                                           String child,
                                           DataType<R> type,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        return register(new ColumnIdent(column, child), type, true, expression);
    }

    public <R> ColumnRegistrar<T> register(String column,
                                           List<String> children,
                                           DataType<R> type,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        return register(new ColumnIdent(column, children), type, true, expression);
    }

    public <R> ColumnRegistrar<T> register(String column,
                                           DataType<R> type,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        return register(new ColumnIdent(column), type, true, expression);
    }

    public <R> ColumnRegistrar<T> register(String column,
                                           DataType<R> type,
                                           boolean nullable,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        return register(new ColumnIdent(column), type, nullable, expression);
    }

    public ColumnRegistrar<T> register(ColumnIdent column,
                                           DataType type,
                                           boolean nullable,
                                           @Nullable RowCollectExpressionFactory<T> expression) {
        Reference ref = new Reference(
            new ReferenceIdent(relationName, column),
            rowGranularity,
            type,
            ColumnPolicy.STRICT,
            Reference.IndexType.PLAIN,
            nullable,
            position,
            null
        );
        position++;
        if (ref.column().isTopLevel()) {
            columnsBuilder.add(ref);
        }
        infos.put(ref.column(), ref);
        registerPossibleObjectInnerTypes(column.name(), column.path(), type);

        if (expression != null) {
            expressionBuilder.put(column, expression);
        }
        return this;
    }

    private void registerPossibleObjectInnerTypes(String topLevelName, List<String> path, DataType<?> dataType) {
        if (DataTypes.isArray(dataType)) {
            dataType = ((ArrayType) dataType).innerType();
        }
        if (dataType.id() != ObjectType.ID) {
            return;
        }
        Map<String, DataType> innerTypes = ((ObjectType) dataType).innerTypes();
        int pos = 0;
        for (Map.Entry<String, DataType> entry : innerTypes.entrySet()) {
            List<String> subPath = new ArrayList<>(path);
            subPath.add(entry.getKey());
            ColumnIdent ci = new ColumnIdent(topLevelName, subPath);
            DataType innerType = entry.getValue();
            Reference ref = new Reference(
                new ReferenceIdent(relationName, ci),
                rowGranularity,
                innerType,
                ColumnPolicy.STRICT,
                Reference.IndexType.PLAIN,
                true,
                pos,
                null
            );
            pos++;
            infos.putIfAbsent(ref.column(), ref);
            registerPossibleObjectInnerTypes(ci.name(), ci.path(), innerType);
        }
    }

    public ColumnRegistrar<T> putInfoOnly(ColumnIdent columnIdent, Reference reference) {
        infos.putIfAbsent(columnIdent, reference);
        return this;
    }

    public Map<ColumnIdent, Reference> infos() {
        return infos;
    }

    public Set<Reference> columns() {
        return columnsBuilder.build();
    }

    public Map<ColumnIdent, RowCollectExpressionFactory<T>> expressions() {
        return expressionBuilder.build();
    }
}
