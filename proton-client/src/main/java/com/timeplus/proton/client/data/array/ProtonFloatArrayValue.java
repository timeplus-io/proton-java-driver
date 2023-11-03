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
 * Wrapper of {@code float[]}.
 */
public class ProtonFloatArrayValue extends ProtonObjectValue<float[]> {
    private static final String TYPE_NAME = "float[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ProtonFloatArrayValue ofEmpty() {
        return of(ProtonValues.EMPTY_FLOAT_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonFloatArrayValue of(float[] value) {
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

    public static ProtonFloatArrayValue of(ProtonValue ref, float[] value) {
        return ref instanceof ProtonFloatArrayValue ? ((ProtonFloatArrayValue) ref).set(value)
                : new ProtonFloatArrayValue(value);
    }

    protected ProtonFloatArrayValue(float[] value) {
        super(value);
    }

    @Override
    protected ProtonFloatArrayValue set(float[] value) {
        super.set(ProtonChecker.nonNull(value, ProtonValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        float[] v = getValue();
        int len = v.length;
        Float[] array = new Float[len];
        for (int i = 0; i < len; i++) {
            array[i] = Float.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        float[] v = getValue();
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
        float[] v = getValue();
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
    public ProtonFloatArrayValue copy(boolean deep) {
        if (!deep) {
            return new ProtonFloatArrayValue(getValue());
        }

        float[] value = getValue();
        return new ProtonFloatArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ProtonFloatArrayValue resetToNullOrEmpty() {
        set(ProtonValues.EMPTY_FLOAT_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        float[] value = getValue();
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
    public ProtonFloatArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? 1F : (float) 0;
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(byte value) {
        return set(new float[] { value });
    }

    @Override
    public ProtonFloatArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(short value) {
        return set(new float[] { value });
    }

    @Override
    public ProtonFloatArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(int value) {
        return set(new float[] { value });
    }

    @Override
    public ProtonFloatArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(long value) {
        return set(new float[] { value });
    }

    @Override
    public ProtonFloatArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(float value) {
        return set(new float[] { value });
    }

    @Override
    public ProtonFloatArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ProtonFloatArrayValue update(double value) {
        return set(new float[] { (float) value });
    }

    @Override
    public ProtonFloatArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[len];
        for (int i = 0; i < len; i++) {
            v[i] = (float) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(BigInteger value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY : new float[] { value.floatValue() });
    }

    @Override
    public ProtonFloatArrayValue update(BigDecimal value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY : new float[] { value.floatValue() });
    }

    @Override
    public ProtonFloatArrayValue update(Enum<?> value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY : new float[] { value.ordinal() });
    }

    @Override
    public ProtonFloatArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ProtonFloatArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ProtonFloatArrayValue update(LocalDate value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY : new float[] { value.toEpochDay() });
    }

    @Override
    public ProtonFloatArrayValue update(LocalTime value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY : new float[] { value.toSecondOfDay() });
    }

    @Override
    public ProtonFloatArrayValue update(LocalDateTime value) {
        return set(value == null ? ProtonValues.EMPTY_FLOAT_ARRAY
                : new float[] { value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ProtonFloatArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).floatValue();
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        float[] values = new float[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.floatValue();
        }
        return set(values);
    }

    @Override
    public ProtonFloatArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        float[] v = new float[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).floatValue();
        }
        return set(v);
    }

    @Override
    public ProtonFloatArrayValue update(String value) {
        if (ProtonChecker.isNullOrBlank(value)) {
            set(ProtonValues.EMPTY_FLOAT_ARRAY);
        } else {
            List<String> list = ProtonUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ProtonValues.EMPTY_FLOAT_ARRAY);
            } else {
                float[] arr = new float[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? 0F : Float.parseFloat(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ProtonFloatArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        throw newUnsupportedException(ProtonValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ProtonFloatArrayValue update(ProtonValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonFloatArrayValue) {
            set(((ProtonFloatArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonFloatArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            float[] values = new float[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).floatValue();
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
            return set(new float[] { ((Number) value).floatValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ProtonFloatArrayValue update(Object value) {
        if (value instanceof float[]) {
            set((float[]) value);
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

        return Arrays.equals(getValue(), ((ProtonFloatArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}
