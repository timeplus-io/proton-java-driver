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
 * Wrapper of {@code byte[]}.
 */
public class ProtonByteArrayValue extends ProtonObjectValue<byte[]> {
    private static final String TYPE_NAME = "byte[]";

    /**
     * Creates an empty array.
     *
     * @return empty array
     */

    public static ProtonByteArrayValue ofEmpty() {
        return of(ProtonValues.EMPTY_BYTE_ARRAY);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonByteArrayValue of(byte[] value) {
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

    public static ProtonByteArrayValue of(ProtonValue ref, byte[] value) {
        return ref instanceof ProtonByteArrayValue ? ((ProtonByteArrayValue) ref).set(value)
                : new ProtonByteArrayValue(value);
    }

    protected ProtonByteArrayValue(byte[] value) {
        super(value);
    }

    @Override
    protected ProtonByteArrayValue set(byte[] value) {
        super.set(ProtonChecker.nonNull(value, ProtonValues.TYPE_ARRAY));
        return this;
    }

    @Override
    public Object[] asArray() {
        byte[] v = getValue();
        int len = v.length;
        Byte[] array = new Byte[len];
        for (int i = 0; i < len; i++) {
            array[i] = Byte.valueOf(v[i]);
        }
        return array;
    }

    @Override
    public <E> E[] asArray(Class<E> clazz) {
        byte[] v = getValue();
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
        byte[] v = getValue();
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
    public ProtonByteArrayValue copy(boolean deep) {
        if (!deep) {
            return new ProtonByteArrayValue(getValue());
        }

        byte[] value = getValue();
        return new ProtonByteArrayValue(Arrays.copyOf(value, value.length));
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override

    public ProtonByteArrayValue resetToNullOrEmpty() {
        set(ProtonValues.EMPTY_BYTE_ARRAY);
        return this;
    }

    @Override
    public String toSqlExpression() {
        byte[] value = getValue();
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
    public ProtonByteArrayValue update(boolean[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = value[i] ? (byte) 1 : (byte) 0;
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(char[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(byte value) {
        return set(new byte[] { value });
    }

    @Override
    public ProtonByteArrayValue update(byte[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        return set(Arrays.copyOf(value, len));
    }

    @Override
    public ProtonByteArrayValue update(short value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ProtonByteArrayValue update(short[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(int value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ProtonByteArrayValue update(int[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(long value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ProtonByteArrayValue update(long[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(float value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ProtonByteArrayValue update(float[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(double value) {
        return set(new byte[] { (byte) value });
    }

    @Override
    public ProtonByteArrayValue update(double[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[len];
        for (int i = 0; i < len; i++) {
            v[i] = (byte) value[i];
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(BigInteger value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY : new byte[] { value.byteValue() });
    }

    @Override
    public ProtonByteArrayValue update(BigDecimal value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY : new byte[] { value.byteValue() });
    }

    @Override
    public ProtonByteArrayValue update(Enum<?> value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.ordinal() });
    }

    @Override
    public ProtonByteArrayValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ProtonValues.TYPE_IPV4, TYPE_NAME);
    }

    @Override
    public ProtonByteArrayValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY :
        // value.getAddress());
        throw newUnsupportedException(ProtonValues.TYPE_IPV6, TYPE_NAME);
    }

    @Override
    public ProtonByteArrayValue update(LocalDate value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.toEpochDay() });
    }

    @Override
    public ProtonByteArrayValue update(LocalTime value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY : new byte[] { (byte) value.toSecondOfDay() });
    }

    @Override
    public ProtonByteArrayValue update(LocalDateTime value) {
        return set(value == null ? ProtonValues.EMPTY_BYTE_ARRAY
                : new byte[] { (byte) value.toEpochSecond(ZoneOffset.UTC) });
    }

    @Override
    public ProtonByteArrayValue update(Collection<?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[size];
        int index = 0;
        for (Object o : value) {
            v[index++] = o == null ? 0 : ((Number) o).byteValue();
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(Enumeration<?> value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        List<Number> v = new LinkedList<>();
        while (value.hasMoreElements()) {
            v.add((Number) value.nextElement());
        }

        byte[] values = new byte[v.size()];
        int index = 0;
        for (Number n : v) {
            values[index++] = n == null ? 0 : n.byteValue();
        }
        return set(values);
    }

    @Override
    public ProtonByteArrayValue update(Map<?, ?> value) {
        int size = value == null ? 0 : value.size();
        if (size == 0) {
            return resetToNullOrEmpty();
        }

        byte[] v = new byte[size];
        int index = 0;
        for (Entry<?, ?> e : value.entrySet()) {
            Object o = e.getValue();
            v[index++] = o == null ? 0 : ((Number) e.getValue()).byteValue();
        }
        return set(v);
    }

    @Override
    public ProtonByteArrayValue update(String value) {
        if (ProtonChecker.isNullOrBlank(value)) {
            set(ProtonValues.EMPTY_BYTE_ARRAY);
        } else {
            List<String> list = ProtonUtils.readValueArray(value, 0, value.length());
            if (list.isEmpty()) {
                set(ProtonValues.EMPTY_BYTE_ARRAY);
            } else {
                byte[] arr = new byte[list.size()];
                int index = 0;
                for (String v : list) {
                    arr[index++] = v == null ? (byte) 0 : Byte.parseByte(v);
                }
                set(arr);
            }
        }
        return this;
    }

    @Override
    public ProtonByteArrayValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        // ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        // buffer.putLong(value.getMostSignificantBits());
        // buffer.putLong(value.getLeastSignificantBits());
        // return set(buffer.array());
        throw newUnsupportedException(ProtonValues.TYPE_UUID, TYPE_NAME);
    }

    @Override
    public ProtonByteArrayValue update(ProtonValue value) {
        if (value == null || value.isNullOrEmpty()) {
            return resetToNullOrEmpty();
        } else if (value instanceof ProtonByteArrayValue) {
            set(((ProtonByteArrayValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonByteArrayValue update(Object[] value) {
        int len = value == null ? 0 : value.length;
        if (len == 0) {
            return resetToNullOrEmpty();
        } else {
            byte[] values = new byte[len];
            for (int i = 0; i < len; i++) {
                Object o = value[i];
                values[i] = o == null ? 0 : ((Number) o).byteValue();
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
            return set(new byte[] { ((Number) value).byteValue() });
        } else {
            throw newUnsupportedException(value.getClass().getName(), TYPE_NAME);
        }
    }

    @Override
    public ProtonByteArrayValue update(Object value) {
        if (value instanceof byte[]) {
            set((byte[]) value);
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

        return Arrays.equals(getValue(), ((ProtonByteArrayValue) obj).getValue());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(getValue());
    }
}
