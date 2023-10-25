package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of byte.
 */
public class ProtonByteValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonByteValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonByteValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonByteValue ? ((ProtonByteValue) ref).set(true, (byte) 0)
                : new ProtonByteValue(true, (byte) 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonByteValue of(byte value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonByteValue of(int value) {
        return of(null, (byte) value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonByteValue of(Number value) {
        return value == null ? ofNull(null) : of(null, value.byteValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonByteValue of(ProtonValue ref, byte value) {
        return ref instanceof ProtonByteValue ? ((ProtonByteValue) ref).set(false, value)
                : new ProtonByteValue(false, value);
    }

    private boolean isNull;
    private byte value;

    protected ProtonByteValue(boolean isNull, byte value) {
        set(isNull, value);
    }

    protected ProtonByteValue set(boolean isNull, byte value) {
        this.isNull = isNull;
        this.value = isNull ? (byte) 0 : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public byte getValue() {
        return value;
    }

    @Override
    public ProtonByteValue copy(boolean deep) {
        return new ProtonByteValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return value;
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
        return isNull ? null : Byte.valueOf(value);
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
    public ProtonByteValue resetToNullOrEmpty() {
        return set(true, (byte) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ProtonValues.NULL_EXPR : String.valueOf(value);
    }

    @Override
    public ProtonByteValue update(char value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonByteValue update(short value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(int value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(long value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(float value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(double value) {
        return set(false, (byte) value);
    }

    @Override
    public ProtonByteValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.byteValueExact());
    }

    @Override
    public ProtonByteValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.byteValueExact());
    }

    @Override
    public ProtonByteValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, (byte) value.ordinal());
    }

    @Override
    public ProtonByteValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, Byte.parseByte(value));
    }

    @Override
    public ProtonByteValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asByte());
    }

    @Override
    public ProtonByteValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).byteValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asByte());
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

        ProtonByteValue v = (ProtonByteValue) obj;
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
