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

package io.crate.expression.symbol;

import com.google.common.base.Preconditions;
import io.crate.data.Input;
import io.crate.exceptions.ConversionException;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Literal<ReturnType> extends Symbol implements Input<ReturnType>, Comparable<Literal> {

    private final Object value;
    private final DataType type;

    public static final Literal<Void> NULL = new Literal<>(DataTypes.UNDEFINED, null);
    public static final Literal<Boolean> BOOLEAN_TRUE = new Literal<>(DataTypes.BOOLEAN, true);
    public static final Literal<Boolean> BOOLEAN_FALSE = new Literal<>(DataTypes.BOOLEAN, false);
    public static final Literal<Map<String, Object>> EMPTY_OBJECT = Literal.of(Collections.emptyMap());

    public static Collection<Literal> explodeCollection(Literal collectionLiteral) {
        Preconditions.checkArgument(DataTypes.isArray(collectionLiteral.valueType()));
        Iterable values;
        int size;
        Object literalValue = collectionLiteral.value();
        if (literalValue instanceof Collection) {
            values = (Iterable) literalValue;
            size = ((Collection) literalValue).size();
        } else {
            values = Arrays.asList((Object[]) literalValue);
            size = ((Object[]) literalValue).length;
        }

        List<Literal> literals = new ArrayList<>(size);
        for (Object value : values) {
            literals.add(new Literal<>(
                ((ArrayType) collectionLiteral.valueType()).innerType(),
                value
            ));
        }
        return literals;
    }

    public Literal(StreamInput in) throws IOException {
        type = DataTypes.fromStream(in);
        value = type.streamer().readValueFrom(in);
    }

    private Literal(DataType type, ReturnType value) {
        assert typeMatchesValue(type, value) :
            String.format(Locale.ENGLISH, "value %s is not of type %s", value, type.getName());
        this.type = type;
        this.value = value;
    }

    private static boolean typeMatchesValue(DataType type, Object value) {
        if (value == null) {
            return true;
        }
        if (type.id() == ObjectType.ID) {
            //noinspection unchecked
            Map<String, Object> mapValue = (Map<String, Object>) value;
            ObjectType objectType = ((ObjectType) type);
            for (String key : mapValue.keySet()) {
                DataType innerType = objectType.innerType(key);
                if (typeMatchesValue(innerType, mapValue.get(key)) == false) {
                    return false;
                }
            }
            // lets do the expensive "deep" map value conversion only after everything else succeeded
            Map<String, Object> safeValue = objectType.value(value);
            return safeValue.size() == mapValue.size();
        }

        return Objects.equals(type.value(value), value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compareTo(Literal o) {
        return type.compareValueTo(value, o.value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public ReturnType value() {
        return (ReturnType) value;
    }

    @Override
    public DataType valueType() {
        return type;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.LITERAL;
    }

    @Override
    public Symbol cast(DataType newDataType, boolean tryCast) {
        return Literal.convert(this, newDataType);
    }

    /**
     * Literals always may be casted if required.
     */
    @Override
    public boolean canBeCasted() {
        return true;
    }

    @Override
    public boolean isValueSymbol() {
        return true;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitLiteral(this, context);
    }

    @Override
    public int hashCode() {
        if (value == null) {
            return 0;
        }
        if (value.getClass().isArray()) {
            return Arrays.deepHashCode(((Object[]) value));
        }
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Literal literal = (Literal) obj;
        if (valueType().equals(literal.valueType())) {
            if (valueType().compareValueTo(value, literal.value) == 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return "Literal{" + stringRepresentation(value) + ", type=" + type + '}';
    }

    private static String stringRepresentation(Object value) {
        if (value == null) {
            return null;
        }
        if (value.getClass().isArray()) {
            return '[' + Stream.of((Object[]) value).map(Literal::stringRepresentation).collect(Collectors.joining(", ")) + ']';
        }
        if (value instanceof BytesRef) {
            return "'" + ((BytesRef) value).utf8ToString() + "'";
        }
        return value.toString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataTypes.toStream(type, out);
        type.streamer().writeValueTo(out, value);
    }

    public static Literal<Map<String, Object>> of(Map<String, Object> value) {
        return new Literal<>(ObjectType.untyped(), value);
    }

    public static <T> Literal<List<T>> of(List<T> value, DataType<List<T>> dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Double[]> of(Double[] value, DataType<Double[]> dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Set> of(Set value, DataType dataType) {
        return new Literal<>(dataType, value);
    }

    public static Literal<Long> of(Long value) {
        return new Literal<>(DataTypes.LONG, value);
    }

    public static Literal<Object> of(DataType type, Object value) {
        return new Literal<>(type, value);
    }

    public static Literal<Integer> of(Integer value) {
        return new Literal<>(DataTypes.INTEGER, value);
    }

    public static Literal<String> of(String value) {
        return new Literal<>(DataTypes.STRING, value);
    }

    public static Literal<Boolean> of(Boolean value) {
        if (value == null) {
            return new Literal<>(DataTypes.BOOLEAN, null);
        }
        return value ? BOOLEAN_TRUE : BOOLEAN_FALSE;
    }

    public static Literal<Double> of(Double value) {
        return new Literal<>(DataTypes.DOUBLE, value);
    }

    public static Literal<Float> of(Float value) {
        return new Literal<>(DataTypes.FLOAT, value);
    }

    public static Literal<Point> newGeoPoint(Object point) {
        return new Literal<>(DataTypes.GEO_POINT, DataTypes.GEO_POINT.value(point));
    }

    public static Literal<Map<String, Object>> newGeoShape(String value) {
        return new Literal<>(DataTypes.GEO_SHAPE, DataTypes.GEO_SHAPE.value(value));
    }

    /**
     * convert the given symbol to a literal with the given type, unless the type already matches,
     * in which case the symbol will be returned as is.
     *
     * @param symbol that is expected to be a literal
     * @param type   type that the literal should have
     * @return converted literal
     * @throws ConversionException if symbol cannot be converted to the given type
     */
    public static Literal convert(Symbol symbol, DataType type) throws ConversionException {
        assert symbol instanceof Literal : "expected a parameter or literal symbol";
        Literal literal = (Literal) symbol;
        if (literal.valueType().equals(type)) {
            return literal;
        }
        try {
            return of(type, type.value(literal.value()));
        } catch (IllegalArgumentException | ClassCastException e) {
            throw new ConversionException(symbol, type);
        }
    }

    @Override
    public String representation() {
        return stringRepresentation(value);
    }
}
