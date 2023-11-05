package com.timeplus.proton.client.data;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wrapper class of Nested.
 */
public class ProtonNestedValue extends ProtonObjectValue<Object[][]> {
    /**
     * Creates an empty nested value.
     *
     * @param columns non-null columns
     * @return empty nested value
     */
    public static ProtonNestedValue ofEmpty(List<ProtonColumn> columns) {
        return of(null, columns, new Object[0][]);
    }

    /**
     * Wrap the given value.
     *
     * @param columns columns
     * @param values  values
     * @return object representing the value
     */
    public static ProtonNestedValue of(List<ProtonColumn> columns, Object[][] values) {
        return of(null, columns, values);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref     object to update, could be null
     * @param columns columns
     * @param values  values
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonNestedValue of(ProtonValue ref, List<ProtonColumn> columns, Object[][] values) {
        return ref instanceof ProtonNestedValue
                ? (ProtonNestedValue) ((ProtonNestedValue) ref).update(values)
                : new ProtonNestedValue(columns, values);
    }

    protected static Object[][] check(List<ProtonColumn> columns, Object[][] value) {
        if (columns == null || value == null) {
            throw new IllegalArgumentException("Non-null columns and value are required");
        }

        if (columns.isEmpty()) {
            throw new IllegalArgumentException("At least one column must be specified for nested type");
        }

        if (value.length != 0 && value.length != columns.size()) {
            throw new IllegalArgumentException("Columns and values should have same length");
        }

        return value;
    }

    private final List<ProtonColumn> columns;

    protected ProtonNestedValue(List<ProtonColumn> columns, Object[][] values) {
        super(check(columns, values));
        this.columns = columns;
    }

    protected Object getSingleValue() {
        Object[][] value = getValue();

        if (value == null || value.length != 1 || value[0] == null || value[0].length != 1) {
            throw new UnsupportedOperationException(
                    "Only nested object containing only one value(one column and one row) supports type conversion");
        }

        return value[0][0];
    }

    @Override
    protected ProtonNestedValue set(Object[][] value) {
        if (columns == null && getValue() == null) { // must be called from constructor
            super.set(value);
        } else {
            super.set(check(columns, value));
        }
        return this;
    }

    /**
     * Gets immutable list of nested columns.
     *
     * @return immutable list of columns
     */
    public List<ProtonColumn> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public ProtonNestedValue copy(boolean deep) {
        if (!deep) {
            return new ProtonNestedValue(columns, getValue());
        }

        Object[][] value = getValue();
        int len = value.length;
        Object[][] newValue = new Object[len][];
        for (int i = 0; i < len; i++) {
            Object[] v = value[i];
            newValue[i] = Arrays.copyOf(v, v.length);
        }
        return new ProtonNestedValue(columns, newValue);
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        Object[][] v = getValue();
        T[] array = (T[]) Array.newInstance(ProtonChecker.nonNull(clazz, ProtonValues.TYPE_CLASS), v.length);
        int index = 0;
        for (Object[] o : v) {
            array[index++] = clazz.cast(o);
        }
        return array;
    }

    @Override
    public Map<Object, Object> asMap() {
        Map<Object, Object> map = new LinkedHashMap<>();
        int index = 0;
        for (Object[] o : getValue()) {
            map.put(columns.get(index++).getColumnName(), o);
        }

        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        Map<K, V> map = new LinkedHashMap<>();
        int index = 0;
        for (Object[] o : getValue()) {
            map.put(keyClass.cast(columns.get(index++).getColumnName()), valueClass.cast(o));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString(int length, Charset charset) {
        String str = Arrays.deepToString(getValue());
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public boolean isNullOrEmpty() {
        Object[][] value = getValue();
        return value == null || value.length == 0;
    }

    @Override
    public ProtonNestedValue resetToNullOrEmpty() {
        set(new Object[0][]);
        return this;
    }

    @Override
    public String toSqlExpression() {
        Object[][] value = getValue();
        if (value == null || value.length == 0) {
            return ProtonValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder();
        for (Object[] v : value) {
            if (v == null || v.length == 0) {
                builder.append(ProtonValues.EMPTY_ARRAY_EXPR);
            } else {
                builder.append('[');
                for (Object o : v) {
                    builder.append(ProtonValues.convertToSqlExpression(o)).append(',');
                }
                builder.setLength(builder.length() - 1);
                builder.append(']');
            }
            builder.append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }

    @Override
    public ProtonNestedValue update(boolean value) {
        set(new Object[][] { new Byte[] { value ? (byte) 1 : (byte) 0 } });
        return this;
    }

    @Override
    public ProtonNestedValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        Byte[] v = new Byte[len];
        if (len > 0) {
            int index = 0;
            for (boolean b : value) {
                v[index++] = b ? (byte) 1 : (byte) 0;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(char value) {
        set(new Object[][] { new Integer[] { (int) value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        Integer[] v = new Integer[len];
        if (len > 0) {
            int index = 0;
            for (char c : value) {
                v[index++] = (int) c;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(byte value) {
        set(new Object[][] { new Object[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        Byte[] v = new Byte[len];
        if (len > 0) {
            int index = 0;
            for (byte b : value) {
                v[index++] = b;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(short value) {
        set(new Object[][] { new Short[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        Short[] v = new Short[len];
        if (len > 0) {
            int index = 0;
            for (short s : value) {
                v[index++] = s;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(int value) {
        set(new Object[][] { new Integer[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        Integer[] v = new Integer[len];
        if (len > 0) {
            int index = 0;
            for (int i : value) {
                v[index++] = i;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(long value) {
        set(new Object[][] { new Long[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        Long[] v = new Long[len];
        if (len > 0) {
            int index = 0;
            for (long l : value) {
                v[index++] = l;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(float value) {
        set(new Object[][] { new Float[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        Float[] v = new Float[len];
        if (len > 0) {
            int index = 0;
            for (float f : value) {
                v[index++] = f;
            }
        }
        set(new Object[][] { v });
        return this;
    }

    @Override
    public ProtonNestedValue update(double value) {
        set(new Object[][] { new Double[] { value } });
        return this;
    }

    @Override
    public ProtonNestedValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        Double[] v = new Double[len];
        if (len > 0) {
            int index = 0;
            for (double d : value) {
                v[index++] = d;
            }
        }
        return set(new Object[][] { v });
    }

    @Override
    public ProtonNestedValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new BigInteger[] { value } });
    }

    @Override
    public ProtonNestedValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new BigDecimal[] { value } });
    }

    @Override
    public ProtonNestedValue update(Enum<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Enum[] { value } });
    }

    @Override
    public ProtonNestedValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Inet4Address[] { value } });
    }

    @Override
    public ProtonNestedValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Inet6Address[] { value } });
    }

    @Override
    public ProtonNestedValue update(LocalDate value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalDate[] { value } });
    }

    @Override
    public ProtonNestedValue update(LocalTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalTime[] { value } });
    }

    @Override
    public ProtonNestedValue update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new LocalDateTime[] { value } });
    }

    @Override
    public ProtonNestedValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o;
        }
        return set(new Object[][] { v });
    }

    @Override
    public ProtonNestedValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Object> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add(value.nextElement());
        }
        return set(new Object[][] { v.toArray(new Object[v.size()]) });
    }

    @Override
    public ProtonNestedValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        Object[] v = new Object[size];
        int index = 0;
        for (Object o : value.values()) {
            v[index++] = o;
        }
        return set(new Object[][] { v });
    }

    @Override
    public ProtonNestedValue update(String value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new String[] { value } });
    }

    @Override
    public ProtonNestedValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new UUID[] { value } });
    }

    @Override
    public ProtonNestedValue update(ProtonValue value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonNestedValue) {
            set(((ProtonNestedValue) value).getValue());
        } else {
            set(new Object[][] { value.asArray() });
        }
        return this;
    }

    @Override
    public ProtonNestedValue update(Object[] value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Object[][]) {
            set((Object[][]) value);
        } else {
            set(new Object[][] { value });
        }
        return this;
    }

    @Override
    public ProtonValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(new Object[][] { new Object[] { value } });
    }

    @Override
    public ProtonNestedValue update(Object value) {
        if (value instanceof Object[][]) {
            set((Object[][]) value);
        } else {
            super.update(value);
        }
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        return Arrays.deepEquals(getValue(), ((ProtonArrayValue<?>) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(getValue());
    }
}
