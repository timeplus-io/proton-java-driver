package com.timeplus.proton.client.data.array;

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

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonUtils;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;
import com.timeplus.proton.client.data.ProtonObjectValue;

/**
 * Wrapper of {@code int[]}.
 */
public class ProtonIntArrayValue extends ProtonObjectValue<int[]> {
    private final static String TYPE_NAME = "int[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ProtonIntArrayValue ofEmpty() {
        return of(ProtonValues.EMPTY_INT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonIntArrayValue of(int[] value) {
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

    public static ProtonIntArrayValue of(ProtonValue ref, int[] value) {
        return ref instanceof ProtonIntArrayValue ? ((ProtonIntArrayValue) ref).set(value)
                : new ProtonIntArrayValue(value);
    }

    protected ProtonIntArrayValue(int[] value) {
        super(value);
    }

    @Override
    protected ProtonIntArrayValue set(int[] value) {
        super.set(ProtonChecker.nonNull(value, ProtonValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        int[] v = getValue();
        int len = v.length;
        Integer[] array = new Integer[len];
        for (int i = 0; i < len; i++) {
            array[i] = Integer.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        int[] v = getValue();
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
        int[] v = getValue();
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
    public ProtonIntArrayValue copy(boolean deep) {
        if (!deep) {
            return new ProtonIntArrayValue(getValue());
        }

        int[] value = getValue();
        return new ProtonIntArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ProtonIntArrayValue resetToNullOrEmpty() {
        set(ProtonValues.EMPTY_INT_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        int[] value = getValue();
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
    public ProtonIntArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1 : 0;
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(byte value) {
        return set(new int[] { value });
    }

    @Override
    public ProtonIntArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(short value) {
        return set(new int[] { value });
    }

    @Override
    public ProtonIntArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(int value) {
        return set(new int[] { value });
    }

    @Override
    public ProtonIntArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ProtonIntArrayValue update(long value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ProtonIntArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(float value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ProtonIntArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(double value) {
        return set(new int[] { (int) value });
    }

    @Override
    public ProtonIntArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[len];
        for (int i = 0; i < len; i++) {
            v[i] = (int) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(BigInteger value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY : new int[] { value.intValue() });
    }

    @Override
    public ProtonIntArrayValue update(BigDecimal value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY : new int[] { value.intValue() });
    }

    @Override
    public ProtonIntArrayValue update(Enum<?> value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY : new int[] { value.ordinal() });
    }

    @Override
    public ProtonIntArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ProtonIntArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ProtonIntArrayValue update(LocalDate value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY : new int[] { (int) value.toEpochDay() });
    }

    @Override
    public ProtonIntArrayValue update(LocalTime value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY : new int[] { value.toSecondOfDay() });
    }

    @Override
    public ProtonIntArrayValue update(LocalDateTime value) {
        return set(value == null ? ProtonValues.EMPTY_INT_ARRAY
                : new int[] { (int) value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ProtonIntArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).intValue();
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        int[] values = new int[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.intValue();
        }
        return set(values);
    }

    @Override
    public ProtonIntArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        int[] v = new int[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).intValue();
        }
        return set(v);
    }

    @Override
    public ProtonIntArrayValue update(String value) {
        if (ProtonChecker.isNullOrBlank(value)) {
            set(ProtonValues.EMPTY_INT_ARRAY);
        } else {
            List<String> list = ProtonUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ProtonValues.EMPTY_INT_ARRAY);
            } else {
                int[] arr = new int[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0 : Integer.parseInt(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ProtonIntArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ProtonIntArrayValue update(ProtonValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonIntArrayValue) {
            set(((ProtonIntArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonIntArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            int[] values = new int[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).intValue();
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
            return set(new int[] { ((Number) value).intValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ProtonIntArrayValue update(Object value) {
        if (value instanceof int[]) {
            set((int[]) value);
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

        return Arrays.equals(getValue(), ((ProtonIntArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}
