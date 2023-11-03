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
 * Wraper class of float.
 */
public class ProtonFloatValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonFloatValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonFloatValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonFloatValue ? ((ProtonFloatValue) ref).set(true, 0F)
                : new ProtonFloatValue(true, 0F);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonFloatValue of(float value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonFloatValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.floatValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonFloatValue of(ProtonValue ref, float value) {
        return ref instanceof ProtonFloatValue ? ((ProtonFloatValue) ref).set(false, value)
                : new ProtonFloatValue(false, value);
    }

    private boolean isNull;
    private float value;

    protected ProtonFloatValue(boolean isNull, float value) {
        set(isNull, value);
    }

    protected ProtonFloatValue set(boolean isNull, float value) {
        this.isNull = isNull;
        this.value = isNull ? 0F : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public float getValue() {
        return value;
    }

    @Override
    public ProtonFloatValue copy(boolean deep) {
        return new ProtonFloatValue(isNull, value);
    }

    @Override
    public boolean isInfinity() {
        return value == Float.POSITIVE_INFINITY || value == Float.NEGATIVE_INFINITY;
    }

    @Override
    public boolean isNaN() {
        return value != value;
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
        return value;
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

        BigDecimal dec = new BigDecimal(Float.toString(value));
        if (value == 0F) {
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
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ProtonFloatValue resetToNullOrEmpty() {
        return set(true, 0F);
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
    public ProtonFloatValue update(boolean value) {
        return set(false, value ? 1F : 0F);
    }

    @Override
    public ProtonFloatValue update(char value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(short value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(int value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(long value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(float value) {
        return set(false, value);
    }

    @Override
    public ProtonFloatValue update(double value) {
        return set(false, (float) value);
    }

    @Override
    public ProtonFloatValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.floatValue());
    }

    @Override
    public ProtonFloatValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.floatValue());
    }

    @Override
    public ProtonFloatValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ProtonFloatValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Float.parseFloat(value));
    }

    @Override
    public ProtonFloatValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asFloat());
    }

    @Override
    public ProtonFloatValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).floatValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asFloat());
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

        ProtonFloatValue v = (ProtonFloatValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + Float.floatToIntBits(value);
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
