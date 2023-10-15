package com.proton.client.data;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of Polygon.
 */
public class ProtonGeoPolygonValue extends ProtonObjectValue<double[][][]> {
    static final double[][][] EMPTY_VALUE = new double[0][][];

    /**
     * Creates an empty polygon.
     *
     * @return empty polygon
     */
    public static ProtonGeoPolygonValue ofEmpty() {
        return of(null, EMPTY_VALUE);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonGeoPolygonValue of(double[][][] value) {
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
    public static ProtonGeoPolygonValue of(ProtonValue ref, double[][][] value) {
        return ref instanceof ProtonGeoPolygonValue ? ((ProtonGeoPolygonValue) ref).set(value)
                : new ProtonGeoPolygonValue(value);
    }

    protected static double[][][] check(double[][][] value) {
        for (int i = 0, len = ProtonChecker.nonNull(value, ProtonValues.TYPE_POLYGON).length; i < len; i++) {
            ProtonGeoRingValue.check(value[i]);
        }

        return value;
    }

    protected static String convert(double[][][] value, int length) {
        StringBuilder builder = new StringBuilder().append('[');
        for (int i = 0, len = value.length; i < len; i++) {
            builder.append(ProtonGeoRingValue.convert(value[i], 0)).append(',');
        }

        if (builder.length() > 1) {
            builder.setLength(builder.length() - 1);
        }
        String str = builder.append(']').toString();
        return length > 0 ? ProtonChecker.notWithDifferentLength(str, length) : str;
    }

    protected ProtonGeoPolygonValue(double[][][] value) {
        super(value);
    }

    @Override
    protected ProtonGeoPolygonValue set(double[][][] value) {
        return (ProtonGeoPolygonValue) super.set(check(value));
    }

    @Override
    public ProtonGeoPolygonValue copy(boolean deep) {
        if (!deep) {
            return new ProtonGeoPolygonValue(getValue());
        }

        double[][][] value = getValue();
        double[][][] newValue = new double[value.length][][];
        int index = 0;
        for (double[][] v : value) {
            double[][] nv = new double[v.length][];
            int i = 0;
            for (double[] d : v) {
                nv[i++] = Arrays.copyOf(d, d.length);
            }
            newValue[index++] = nv;
        }
        return new ProtonGeoPolygonValue(newValue);
    }

    @Override
    public Object[] asArray() {
        return getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T[] asArray(Class<T> clazz) {
        double[][][] v = getValue();
        T[] array = (T[]) Array.newInstance(ProtonChecker.nonNull(clazz, ProtonValues.TYPE_CLASS), v.length);
        int index = 0;
        for (double[][] d : v) {
            array[index++] = clazz.cast(d);
        }
        return array;
    }

    @Override
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (keyClass == null || valueClass == null) {
            throw new IllegalArgumentException("Non-null key and value classes are required");
        }
        Map<K, V> map = new LinkedHashMap<>();
        int index = 1;
        for (double[][] d : getValue()) {
            map.put(keyClass.cast(index++), valueClass.cast(d));
        }
        // why not use Collections.unmodifiableMap(map) here?
        return map;
    }

    @Override
    public String asString(int length, Charset charset) {
        return convert(getValue(), length);
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().length == 0;
    }

    @Override
    public ProtonGeoPolygonValue resetToNullOrEmpty() {
        set(EMPTY_VALUE);
        return this;
    }

    @Override
    public String toSqlExpression() {
        return convert(getValue(), 0);
    }

    @Override
    public ProtonGeoPolygonValue update(boolean value) {
        throw newUnsupportedException(ProtonValues.TYPE_BOOLEAN, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(boolean[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0] ? 1 : 0, value[1] ? 0 : 1 } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(char value) {
        throw newUnsupportedException(ProtonValues.TYPE_CHAR, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(char[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(byte value) {
        throw newUnsupportedException(ProtonValues.TYPE_BYTE, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(byte[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(short value) {
        throw newUnsupportedException(ProtonValues.TYPE_SHORT, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(short[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(int value) {
        throw newUnsupportedException(ProtonValues.TYPE_INT, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(int[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(long value) {
        throw newUnsupportedException(ProtonValues.TYPE_LONG, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(long[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(float value) {
        throw newUnsupportedException(ProtonValues.TYPE_FLOAT, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(float[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { new double[] { value[0], value[1] } } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(double value) {
        throw newUnsupportedException(ProtonValues.TYPE_DOUBLE, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[][][] { new double[][] { value } });
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(BigInteger value) {
        throw newUnsupportedException(ProtonValues.TYPE_BIG_INTEGER, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(BigDecimal value) {
        throw newUnsupportedException(ProtonValues.TYPE_BIG_DECIMAL, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(Enum<?> value) {
        throw newUnsupportedException(ProtonValues.TYPE_ENUM, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(Inet4Address value) {
        throw newUnsupportedException(ProtonValues.TYPE_IPV4, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(Inet6Address value) {
        throw newUnsupportedException(ProtonValues.TYPE_IPV6, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(LocalDate value) {
        throw newUnsupportedException(ProtonValues.TYPE_DATE, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(LocalTime value) {
        throw newUnsupportedException(ProtonValues.TYPE_TIME, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(LocalDateTime value) {
        throw newUnsupportedException(ProtonValues.TYPE_DATE_TIME, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(Collection<?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(Enumeration<?> value) {
        if (value == null || !value.hasMoreElements()) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Object v1 = value.nextElement();
        if (!value.hasMoreElements()) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Object v2 = value.nextElement();
        if (value.hasMoreElements()) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }

        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(Map<?, ?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.values().iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(String value) {
        throw newUnsupportedException(ProtonValues.TYPE_STRING, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(UUID value) {
        throw newUnsupportedException(ProtonValues.TYPE_UUID, ProtonValues.TYPE_POLYGON);
    }

    @Override
    public ProtonGeoPolygonValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value instanceof ProtonGeoPolygonValue) {
            set(((ProtonGeoPolygonValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(Object[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Object v1 = value[0];
        Object v2 = value[1];
        if (v1 instanceof Number) {
            set(new double[][][] {
                    new double[][] { new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() } } });
        } else {
            set(new double[][][] { new double[][] {
                    new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) } } });
        }
        return this;
    }

    @Override
    public ProtonGeoPolygonValue update(Object value) {
        if (value instanceof double[][][]) {
            update((double[]) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
