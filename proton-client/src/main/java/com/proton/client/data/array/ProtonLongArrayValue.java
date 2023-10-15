package com.proton.client.data.array;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;
import com.proton.client.data.ProtonObjectValue;

/**
 * Wrapper of {@code long[]}.
 */
public class ProtonLongArrayValue extends ProtonObjectValue<long[]> {
    private static final String TYPE_NAME = "long[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ProtonLongArrayValue ofEmpty() {
        return of(ProtonValues.EMPTY_LONG_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonLongArrayValue of(long[] value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */

    public static ProtonLongArrayValue of(ProtonValue ref, long[] value) {
        return ref instanceof ProtonLongArrayValue ? ((ProtonLongArrayValue) ref).set(value)
                : new ProtonLongArrayValue(value);
    }

    protected ProtonLongArrayValue(long[] value) {
        super(value);
    }

    @Override
    protected ProtonLongArrayValue set(long[] value) {
        super.set(ProtonChecker.nonNull(value, ProtonValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        long[] v = getValue();
        int len = v.length;
        Long[] array = new Long[len];
        for (int i = 0; i < len; i++) {
            array[i] = Long.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        long[] v = getValue();
        int len = v.length;
        E[] array = ProtonValues.createObjectArray(clazz, len, 1);
        for (int i = 0; i < len; i++) {
            array[i] = clazz.cast(v[i]);
        }
        return array;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        long[] v = getValue();
        Map<K, V> map = new LinkedHashMap<>();
        for (int i = 0; i < v.length; i++) {
            map.put(keyClass.cast(i + 1), valueClass.cast(v[i]));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString(int length, Charset charset) {
        String str = Arrays.toString(getValue());
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ProtonLongArrayValue copy(boolean deep) {
        if (!deep) {
            return new ProtonLongArrayValue(getValue());
        }

        long[] value = getValue();
        return new ProtonLongArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ProtonLongArrayValue resetToNullOrEmpty() {
        set(ProtonValues.EMPTY_LONG_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        long[] value = getValue();
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return ProtonValues.EMPTY_ARRAY_EXPR;
        }

        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0; i < len; i++) {
            builder.append(value[i]).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.append(']').toString();
    }

    @Override
    public ProtonLongArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1L : 0L;
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(byte value) {
        return set(new long[] { value });
    }

    @Override
    public ProtonLongArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(short value) {
        return set(new long[] { value });
    }

    @Override
    public ProtonLongArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(int value) {
        return set(new long[] { value });
    }

    @Override
    public ProtonLongArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(long value) {
        return set(new long[] { value });
    }

    @Override
    public ProtonLongArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ProtonLongArrayValue update(float value) {
        return set(new long[] { (long) value });
    }

    @Override
    public ProtonLongArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = (long) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(double value) {
        return set(new long[] { (long) value });
    }

    @Override
    public ProtonLongArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[len];
        for (int i = 0; i < len; i++) {
            v[i] = (long) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(BigInteger value) {
        return set(value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.longValue() });
    }

    @Override
    public ProtonLongArrayValue update(BigDecimal value) {
        return set(value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.longValue() });
    }

    @Override
    public ProtonLongArrayValue update(Enum<?> value) {
        return set(value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.ordinal() });
    }

    @Override
    public ProtonLongArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ProtonLongArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ProtonLongArrayValue update(LocalDate value) {
        return set(value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.toEpochDay() });
    }

    @Override
    public ProtonLongArrayValue update(LocalTime value) {
        return set(value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.toSecondOfDay() });
    }

    @Override
    public ProtonLongArrayValue update(LocalDateTime value) {
        return set(
                value == null ? ProtonValues.EMPTY_LONG_ARRAY : new long[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ProtonLongArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).longValue();
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        long[] values = new long[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.longValue();
        }
        return set(values);
    }

    @Override
    public ProtonLongArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        long[] v = new long[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).longValue();
        }
        return set(v);
    }

    @Override
    public ProtonLongArrayValue update(String value) {
        if (ProtonChecker.isNullOrBlank(value)) {
            set(ProtonValues.EMPTY_LONG_ARRAY);
        } else {
            List<String> list = ProtonUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ProtonValues.EMPTY_LONG_ARRAY);
            } else {
                long[] arr = new long[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0L : Long.parseLong(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ProtonLongArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ProtonLongArrayValue update(ProtonValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonLongArrayValue) {
            set(((ProtonLongArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonLongArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            long[] values = new long[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).longValue();
            }
            set(values);
        }

        return this;
    }

    @Override
    public ProtonValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (value instanceof Number) {
            return set(new long[] { ((Number) value).longValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ProtonLongArrayValue update(Object value) {
        if (value instanceof long[]) {
            set((long[]) value);
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

        return Arrays.equals(getValue(), ((ProtonLongArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}
