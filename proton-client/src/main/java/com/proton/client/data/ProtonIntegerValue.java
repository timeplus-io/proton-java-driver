package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of int.
 */
public class ProtonIntegerValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonIntegerValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonIntegerValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonIntegerValue ? ((ProtonIntegerValue) ref).set(true, 0)
                : new ProtonIntegerValue(true, 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonIntegerValue of(int value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonIntegerValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.intValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonIntegerValue of(ProtonValue ref, int value) {
        return ref instanceof ProtonIntegerValue ? ((ProtonIntegerValue) ref).set(false, value)
                : new ProtonIntegerValue(false, value);
    }

    private boolean isNull;
    private int value;

    protected ProtonIntegerValue(boolean isNull, int value) {
        set(isNull, value);
    }

    protected ProtonIntegerValue set(boolean isNull, int value) {
        this.isNull = isNull;
        this.value = isNull ? 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public int getValue() {
        return value;
    }

    @Override
    public ProtonIntegerValue copy(boolean deep) {
        return new ProtonIntegerValue(isNull, value);
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
        return value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigInteger.valueOf(value);
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
        return isNull ? null : BigDecimal.valueOf(value, scale);
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
    public ProtonIntegerValue resetToNullOrEmpty() {
        return set(true, 0);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : String.valueOf(value);
    }

    @Override
    public ProtonIntegerValue update(boolean value) {
        return set(false, value ? 1 : 0);
    }

    @Override
    public ProtonIntegerValue update(char value) {
        return set(false, value);
    }

    @Override
    public ProtonIntegerValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonIntegerValue update(short value) {
        return set(false, value);
    }

    @Override
    public ProtonIntegerValue update(int value) {
        return set(false, value);
    }

    @Override
    public ProtonIntegerValue update(long value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonIntegerValue update(float value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonIntegerValue update(double value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonIntegerValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ProtonIntegerValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ProtonIntegerValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ProtonIntegerValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Integer.parseInt(value));
    }

    @Override
    public ProtonIntegerValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asInteger());
    }

    @Override
    public ProtonIntegerValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).intValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asInteger());
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

        ProtonIntegerValue v = (ProtonIntegerValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + value;
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
