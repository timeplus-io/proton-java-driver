package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of bool.
 */
public class ProtonBoolValue implements ProtonValue {
    private static final String ERROR_INVALID_NUMBER = "Boolean value can be only 1(true) or 0(false).";

    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonBoolValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonBoolValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonBoolValue ? ((ProtonBoolValue) ref).set(true, false)
                : new ProtonBoolValue(true, false);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonBoolValue of(boolean value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonBoolValue of(int value) {
        if (value == 1) {
            return new ProtonBoolValue(false, true);
        } else if (value == 0) {
            return new ProtonBoolValue(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonBoolValue of(ProtonValue ref, boolean value) {
        return ref instanceof ProtonBoolValue ? ((ProtonBoolValue) ref).set(false, value)
                : new ProtonBoolValue(false, value);
    }

    private boolean isNull;
    private boolean value;

    protected ProtonBoolValue(boolean isNull, boolean value) {
        set(isNull, value);
    }

    protected ProtonBoolValue set(boolean isNull, boolean value) {
        this.isNull = isNull;
        this.value = !isNull && value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public boolean getValue() {
        return value;
    }

    @Override
    public ProtonBoolValue copy(boolean deep) {
        return new ProtonBoolValue(isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return value ? (byte) 1 : (byte) 0;
    }

    @Override
    public short asShort() {
        return value ? (short) 1 : (short) 0;
    }

    @Override
    public int asInteger() {
        return value ? 1 : 0;
    }

    @Override
    public long asLong() {
        return value ? 1L : 0L;
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull) {
            return null;
        }

        return value ? BigInteger.ONE : BigInteger.ZERO;
    }

    @Override
    public float asFloat() {
        return value ? 1F : 0F;
    }

    @Override
    public double asDouble() {
        return value ? 1D : 0D;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNull) {
            return null;
        }

        return BigDecimal.valueOf(value ? 1L : 0L, scale);
    }

    @Override
    public Object asObject() {
        return isNull ? null : Boolean.valueOf(value);
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
    public ProtonBoolValue resetToNullOrEmpty() {
        return set(true, false);
    }

    @Override
    public String toSqlExpression() {
        if (isNull) {
            return ProtonValues.NULL_EXPR;
        }

        // prefer to use number format to ensure backward compatibility
        return value ? "1" : "0";
    }

    @Override
    public ProtonBoolValue update(char value) {
        return set(false, ProtonValues.convertToBoolean(value));
    }

    @Override
    public ProtonBoolValue update(byte value) {
        if (value == (byte) 1) {
            return set(false, true);
        } else if (value == (byte) 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(short value) {
        if (value == (short) 1) {
            return set(false, true);
        } else if (value == (short) 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(int value) {
        if (value == 1) {
            return set(false, true);
        } else if (value == 0) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(long value) {
        if (value == 1L) {
            return set(false, true);
        } else if (value == 0L) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(float value) {
        if (value == 1F) {
            return set(false, true);
        } else if (value == 0F) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(double value) {
        if (value == 1D) {
            return set(false, true);
        } else if (value == 0D) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (BigInteger.ONE.equals(value)) {
            return set(false, true);
        } else if (BigInteger.ZERO.equals(value)) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        } else if (BigDecimal.valueOf(1L, value.scale()).equals(value)) {
            return set(false, true);
        } else if (BigDecimal.valueOf(0L, value.scale()).equals(value)) {
            return set(false, false);
        } else {
            throw new IllegalArgumentException(ERROR_INVALID_NUMBER);
        }
    }

    @Override
    public ProtonBoolValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : update(value.ordinal());
    }

    @Override
    public ProtonBoolValue update(String value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set(false, ProtonValues.convertToBoolean(value));
    }

    @Override
    public ProtonBoolValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asBoolean());
    }

    @Override
    public ProtonBoolValue update(Object value) {
        if (value instanceof Boolean) {
            return set(false, (boolean) value);
        } else if (value instanceof Number) {
            return update(((Number) value).byteValue());
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asBoolean());
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

        ProtonBoolValue v = (ProtonBoolValue) obj;
        return isNull == v.isNull && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return (31 + (isNull ? 1231 : 1237)) * 31 + (value ? 1231 : 1237);
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
