package com.proton.client.data;

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
import java.util.Map;
import java.util.UUID;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of Point.
 */
public class ProtonGeoPointValue extends ProtonObjectValue<double[]> {
    /**
     * Creats a point of origin.
     *
     * @return point of origin
     */
    public static ProtonGeoPointValue ofOrigin() {
        return of(null, new double[] { 0D, 0D });
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonGeoPointValue of(double[] value) {
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
    public static ProtonGeoPointValue of(ProtonValue ref, double[] value) {
        return ref instanceof ProtonGeoPointValue ? ((ProtonGeoPointValue) ref).set(value)
                : new ProtonGeoPointValue(value);
    }

    protected static double[] check(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException("Non-null X and Y coordinates are required");
        }

        return value;
    }

    protected static String convert(double[] value, int length) {
        String str = new StringBuilder().append('(').append(value[0]).append(',').append(value[1]).append(')')
                .toString();
        return length > 0 ? ProtonChecker.notWithDifferentLength(str, length) : str;
    }

    protected ProtonGeoPointValue(double[] value) {
        super(value);
    }

    @Override
    protected ProtonGeoPointValue set(double[] value) {
        super.set(check(value));
        return this;
    }

    @Override
    public ProtonGeoPointValue copy(boolean deep) {
        if (!deep) {
            return new ProtonGeoPointValue(getValue());
        }

        double[] value = getValue();
        double[] newValue = new double[value.length];
        System.arraycopy(value, 0, newValue, 0, value.length);
        return new ProtonGeoPointValue(newValue);
    }

    @Override
    public String asString(int length, Charset charset) {
        return convert(getValue(), length);
    }

    @Override
    public boolean isNullOrEmpty() {
        return false;
    }

    @Override
    public ProtonGeoPointValue resetToNullOrEmpty() {
        set(new double[] { 0D, 0D });
        return this;
    }

    @Override
    public String toSqlExpression() {
        return convert(getValue(), 0);
    }

    @Override
    public ProtonGeoPointValue update(boolean value) {
        throw newUnsupportedException(ProtonValues.TYPE_BOOLEAN, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(boolean[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0] ? 1 : 0, value[1] ? 0 : 1 });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(char value) {
        throw newUnsupportedException(ProtonValues.TYPE_CHAR, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(char[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(byte value) {
        throw newUnsupportedException(ProtonValues.TYPE_BYTE, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(byte[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(short value) {
        throw newUnsupportedException(ProtonValues.TYPE_SHORT, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(short[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(int value) {
        throw newUnsupportedException(ProtonValues.TYPE_INT, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(int[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(long value) {
        throw newUnsupportedException(ProtonValues.TYPE_LONG, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(long[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(float value) {
        throw newUnsupportedException(ProtonValues.TYPE_FLOAT, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(float[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(new double[] { value[0], value[1] });
        return this;
    }

    @Override
    public ProtonGeoPointValue update(double value) {
        throw newUnsupportedException(ProtonValues.TYPE_DOUBLE, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(double[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + Arrays.toString(value));
        }
        set(value);
        return this;
    }

    @Override
    public ProtonGeoPointValue update(BigInteger value) {
        throw newUnsupportedException(ProtonValues.TYPE_BIG_INTEGER, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(BigDecimal value) {
        throw newUnsupportedException(ProtonValues.TYPE_BIG_DECIMAL, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(Enum<?> value) {
        throw newUnsupportedException(ProtonValues.TYPE_ENUM, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(Inet4Address value) {
        throw newUnsupportedException(ProtonValues.TYPE_IPV4, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(Inet6Address value) {
        throw newUnsupportedException(ProtonValues.TYPE_IPV6, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(LocalDate value) {
        throw newUnsupportedException(ProtonValues.TYPE_DATE, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(LocalTime value) {
        throw newUnsupportedException(ProtonValues.TYPE_TIME, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(LocalDateTime value) {
        throw newUnsupportedException(ProtonValues.TYPE_DATE_TIME, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(Collection<?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ProtonGeoPointValue update(Enumeration<?> value) {
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
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ProtonGeoPointValue update(Map<?, ?> value) {
        if (value == null || value.size() != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Iterator<?> i = value.values().iterator();
        Object v1 = i.next();
        Object v2 = i.next();
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ProtonGeoPointValue update(String value) {
        throw newUnsupportedException(ProtonValues.TYPE_STRING, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(UUID value) {
        throw newUnsupportedException(ProtonValues.TYPE_UUID, ProtonValues.TYPE_POINT);
    }

    @Override
    public ProtonGeoPointValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value instanceof ProtonGeoPointValue) {
            set(((ProtonGeoPointValue) value).getValue());
        } else {
            update(value.asArray());
        }
        return this;
    }

    @Override
    public ProtonGeoPointValue update(Object[] value) {
        if (value == null || value.length != 2) {
            throw new IllegalArgumentException(ProtonValues.ERROR_INVALID_POINT + value);
        }
        Object v1 = value[0];
        Object v2 = value[1];
        if (v1 instanceof Number) {
            set(new double[] { ((Number) v1).doubleValue(), ((Number) v2).doubleValue() });
        } else {
            set(new double[] { Double.parseDouble(v1.toString()), Double.parseDouble(v2.toString()) });
        }
        return this;
    }

    @Override
    public ProtonGeoPointValue update(Object value) {
        if (value instanceof double[]) {
            update((double[]) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
