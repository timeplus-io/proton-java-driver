package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of short.
 */
public class ProtonShortValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonShortValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonShortValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonShortValue ? ((ProtonShortValue) ref).set(true, (short) 0)
                : new ProtonShortValue(true, (short) 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonShortValue of(short value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonShortValue of(int value) {
        return of(null, (short) value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonShortValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.shortValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonShortValue of(ProtonValue ref, short value) {
        return ref instanceof ProtonShortValue ? ((ProtonShortValue) ref).set(false, value)
                : new ProtonShortValue(false, value);
    }

    private boolean isNull;
    private short value;

    protected ProtonShortValue(boolean isNull, short value) {
        set(isNull, value);
    }

    protected ProtonShortValue set(boolean isNull, short value) {
        this.isNull = isNull;
        this.value = isNull ? (short) 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public short getValue() {
        return value;
    }

    @Override
    public ProtonShortValue copy(boolean deep) {
        return new ProtonShortValue(isNull, value);
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
        return value;
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
    public ProtonShortValue resetToNullOrEmpty() {
        return set(true, (short) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : String.valueOf(value);
    }

    @Override
    public ProtonShortValue update(boolean value) {
        return set(false, value ? (short) 1 : (short) 0);
    }

    @Override
    public ProtonShortValue update(char value) {
        return set(false, (short) value);
    }

    @Override
    public ProtonShortValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonShortValue update(short value) {
        return set(false, value);
    }

    @Override
    public ProtonShortValue update(int value) {
        return set(false, (short) value);
    }

    @Override
    public ProtonShortValue update(long value) {
        return set(false, (short) value);
    }

    @Override
    public ProtonShortValue update(float value) {
        return set(false, (short) value);
    }

    @Override
    public ProtonShortValue update(double value) {
        return set(false, (short) value);
    }

    @Override
    public ProtonShortValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.shortValueExact());
    }

    @Override
    public ProtonShortValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.shortValueExact());
    }

    @Override
    public ProtonShortValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, (short) value.ordinal());
    }

    @Override
    public ProtonShortValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Short.parseShort(value));
    }

    @Override
    public ProtonShortValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asShort());
    }

    @Override
    public ProtonShortValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).shortValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asShort());
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

        ProtonShortValue v = (ProtonShortValue) obj;
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
