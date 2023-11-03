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
 * Wrapper of {@code double[]}.
 */
public class ProtonDoubleArrayValue extends ProtonObjectValue<double[]> {
    private static final String TYPE_NAME = "double[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ProtonDoubleArrayValue ofEmpty() {
        return of(ProtonValues.EMPTY_DOUBLE_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonDoubleArrayValue of(double[] value) {
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

    public static ProtonDoubleArrayValue of(ProtonValue ref, double[] value) {
        return ref instanceof ProtonDoubleArrayValue ? ((ProtonDoubleArrayValue) ref).set(value)
                : new ProtonDoubleArrayValue(value);
    }

    protected ProtonDoubleArrayValue(double[] value) {
        super(value);
    }

    @Override
    protected ProtonDoubleArrayValue set(double[] value) {
        super.set(ProtonChecker.nonNull(value, ProtonValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        double[] v = getValue();
        int len = v.length;
        Double[] array = new Double[len];
        for (int i = 0; i < len; i++) {
            array[i] = Double.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        double[] v = getValue();
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
        double[] v = getValue();
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
    public ProtonDoubleArrayValue copy(boolean deep) {
        if (!deep) {
            return new ProtonDoubleArrayValue(getValue());
        }

        double[] value = getValue();
        return new ProtonDoubleArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ProtonDoubleArrayValue resetToNullOrEmpty() {
        set(ProtonValues.EMPTY_DOUBLE_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        double[] value = getValue();
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
    public ProtonDoubleArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1D : 0D;
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(byte value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(short value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(int value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(long value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(float value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(double value) {
        return set(new double[] { value });
    }

    @Override
    public ProtonDoubleArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ProtonDoubleArrayValue update(BigInteger value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY : new double[] { value.doubleValue() });
    }

    @Override
    public ProtonDoubleArrayValue update(BigDecimal value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY : new double[] { value.doubleValue() });
    }

    @Override
    public ProtonDoubleArrayValue update(Enum<?> value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY : new double[] { value.ordinal() });
    }

    @Override
    public ProtonDoubleArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ProtonDoubleArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ProtonDoubleArrayValue update(LocalDate value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY : new double[] { value.toEpochDay() });
    }

    @Override
    public ProtonDoubleArrayValue update(LocalTime value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY : new double[] { value.toSecondOfDay() });
    }

    @Override
    public ProtonDoubleArrayValue update(LocalDateTime value) {
        return set(value == null ? ProtonValues.EMPTY_DOUBLE_ARRAY
                : new double[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ProtonDoubleArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).doubleValue();
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        double[] values = new double[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.doubleValue();
        }
        return set(values);
    }

    @Override
    public ProtonDoubleArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        double[] v = new double[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).doubleValue();
        }
        return set(v);
    }

    @Override
    public ProtonDoubleArrayValue update(String value) {
        if (ProtonChecker.isNullOrBlank(value)) {
            set(ProtonValues.EMPTY_DOUBLE_ARRAY);
        } else {
            List<String> list = ProtonUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ProtonValues.EMPTY_DOUBLE_ARRAY);
            } else {
                double[] arr = new double[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0D : Double.parseDouble(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ProtonDoubleArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ProtonDoubleArrayValue update(ProtonValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonDoubleArrayValue) {
            set(((ProtonDoubleArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonDoubleArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            double[] values = new double[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).doubleValue();
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
            return set(new double[] { ((Number) value).doubleValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ProtonDoubleArrayValue update(Object value) {
        if (value instanceof double[]) {
            set((double[]) value);
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

        return Arrays.equals(getValue(), ((ProtonDoubleArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}
