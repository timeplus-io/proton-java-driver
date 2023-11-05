package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of double.
 */
public class ProtonDoubleValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonDoubleValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is
     * null.
     *
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDoubleValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonDoubleValue ?
                ((ProtonDoubleValue) ref).set(true, 0D)
                : new ProtonDoubleValue(true, 0D);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonDoubleValue of(double value) {
        return of(null, value);
    }

    /**
     * Update value of the given object or create a new instance if {@code
     * ref} is null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDoubleValue of(ProtonValue ref, double value) {
        return ref instanceof ProtonDoubleValue ?
                ((ProtonDoubleValue) ref).set(false, value)
                : new ProtonDoubleValue(false, value);
    }

    private boolean isNull;
    private double value;

    protected ProtonDoubleValue(boolean isNull, double value) {
        set(isNull, value);
    }

    protected ProtonDoubleValue set(boolean isNull, double value) {
        this.isNull = isNull;
        this.value = isNull ? 0L : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public double getValue() {
        return value;
    }

    @Override
    public ProtonDoubleValue copy(boolean deep) {
        return new ProtonDoubleValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInteger() {
        return (int) value;
    }

    @Override
    public long asLong() {
        return (long) value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigDecimal.valueOf(value).toBigInteger();
    }

    @Override
    public float asFloat() {
        return (float) value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        }

        BigDecimal dec = BigDecimal.valueOf(value);
        if (value == 0D) {
            dec = dec.setScale(scale);
        } else {
            int diff = scale - dec.scale();
            if (diff > 0) {
                dec = dec.divide(BigDecimal.TEN.pow(diff + 1));
            } else if (diff < 0) {
                dec = dec.setScale(scale, RoundingMode.DOWN);
            }
        }
        return dec;
    }

    @Override
    public Object asObject() {
        return isNull ? null : getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNull) {
            return null;
        }
        String str = String.valueOf(value);
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset), length);
        }
        return str;
    }

    @Override
    public ProtonDoubleValue resetToNullOrEmpty() {
        return set(true, 0D);
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        } else if (isNaN()) {
            return ProtonValues.NAN_EXPR;
        } else if (value == Float.POSITIVE_INFINITY) {
            return ProtonValues.INF_EXPR;
        } else if (value == Float.NEGATIVE_INFINITY) {
            return ProtonValues.NINF_EXPR;
        }
        return String.valueOf(value);
    }

    @Override
    public ProtonDoubleValue update(boolean value) {
        return set(false, value ? 1 : 0);
    }

    @Override
    public ProtonDoubleValue update(char value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(short value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(int value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(long value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(float value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(double value) {
        return set(false, value);
    }

    @Override
    public ProtonDoubleValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.doubleValue());
    }

    @Override
    public ProtonDoubleValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.doubleValue());
    }

    @Override
    public ProtonDoubleValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ProtonDoubleValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Double.parseDouble(value));
    }

    @Override
    public ProtonDoubleValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asDouble());
    }

    @Override
    public ProtonDoubleValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).doubleValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asDouble());
        }
        ProtonValue.super.update(value);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ProtonDoubleValue v = (ProtonDoubleValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        long l = Double.doubleToLongBits(value);
        return (31 + (isNull ? 1231 : 1237)) * 31 + (int) (l ^ (l >>> 32));
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
