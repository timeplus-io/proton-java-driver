package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of long.
 */
public class ProtonLongValue implements ProtonValue {
    private static final BigInteger MASK = BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    /**
     * Create a new instance representing null value of long.
     *
     * @param unsigned true if the long is unsigned; false otherwise
     * @return new instance representing null value
     */
    public static ProtonLongValue ofNull(boolean unsigned) {
        return ofNull(null, unsigned);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref      object to update, could be null
     * @param unsigned true if the value is unsigned; false otherwise
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonLongValue ofNull(ProtonValue ref, boolean unsigned) {
        return ref instanceof ProtonLongValue ? ((ProtonLongValue) ref).set(true, unsigned, 0L)
                : new ProtonLongValue(true, unsigned, 0L);
    }

    /**
     * Wrap the given value.
     *
     * @param value    value
     * @param unsigned true if the value is unsigned; false otherwise
     * @return object representing the value
     */
    public static ProtonLongValue of(long value, boolean unsigned) {
        return of(null, unsigned, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonLongValue of(Number value) {
        return value == null ? ofNull(null, false) : of(null, false, value.longValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref      object to update, could be null
     * @param unsigned true if the value is unsigned; false otherwise
     * @param value    value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonLongValue of(ProtonValue ref, boolean unsigned, long value) {
        return ref instanceof ProtonLongValue ? ((ProtonLongValue) ref).set(false, unsigned, value)
                : new ProtonLongValue(false, unsigned, value);
    }

    private boolean isNull;
    // UInt64 is used in many places, so we prefer to use long instead of BigInteger
    // for better performance and less memory footprints. We can still use
    // asBigInteger() to avoid negative value.
    private boolean unsigned;
    private long value;

    protected ProtonLongValue(boolean isNull, boolean unsigned, long value) {
        set(isNull, unsigned, value);
    }

    protected ProtonLongValue set(boolean isNull, boolean unsigned, long value) {
        this.isNull = isNull;
        this.unsigned = unsigned;
        this.value = isNull ? 0L : value;

        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public long getValue() {
        return value;
    }

    @Override
    public ProtonLongValue copy(boolean deep) {
        return new ProtonLongValue(isNull, unsigned, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    public boolean isUnsigned() {
        return unsigned;
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
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNull) {
            return null;
        }

        BigInteger v = BigInteger.valueOf(value);
        if (unsigned && value < 0L) {
            v = v.and(MASK);
        }
        return v;
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
        if (isNullOrEmpty()) {
            return null;
        }

        return unsigned && value < 0L ? new BigDecimal(asBigInteger(), scale) : BigDecimal.valueOf(value, scale);
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

        String str = unsigned && value < 0L ? asBigInteger().toString() : String.valueOf(value);
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ProtonLongValue resetToNullOrEmpty() {
        return set(true, false, 0L);
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        }

        return unsigned && value < 0L ? asBigInteger().toString() : String.valueOf(value);
    }

    @Override
    public ProtonLongValue update(boolean value) {
        return set(false, unsigned, value ? 1L : 0L);
    }

    @Override
    public ProtonLongValue update(char value) {
        return set(false, unsigned, value);
    }

    @Override
    public ProtonLongValue update(byte value) {
        return set(false, unsigned, value);
    }

    @Override
    public ProtonLongValue update(short value) {
        return set(false, unsigned, value);
    }

    @Override
    public ProtonLongValue update(int value) {
        return set(false, unsigned, value);
    }

    @Override
    public ProtonLongValue update(long value) {
        return set(false, unsigned, value);
    }

    @Override
    public ProtonLongValue update(float value) {
        return set(false, unsigned, (long) value);
    }

    @Override
    public ProtonLongValue update(double value) {
        return set(false, unsigned, (long) value);
    }

    @Override
    public ProtonLongValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, unsigned, value.longValueExact());
    }

    @Override
    public ProtonLongValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, unsigned, value.longValueExact());
    }

    @Override
    public ProtonLongValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, unsigned, value.ordinal());
    }

    @Override
    public ProtonLongValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, unsigned, new BigInteger(value).longValue());
    }

    @Override
    public ProtonLongValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, unsigned, value.asLong());
    }

    @Override
    public ProtonLongValue update(Object value) {
        if (value instanceof Number) {
            return set(false, unsigned, ((Number) value).longValue());
        } else if (value instanceof ProtonValue) {
            return set(false, unsigned, ((ProtonValue) value).asLong());
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

        ProtonLongValue v = (ProtonLongValue) obj;
        return isNull == v.isNull && unsigned == v.unsigned && value == v.value;
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        return ((31 + (isNull ? 1231 : 1237)) * 31 + (unsigned ? 1231 : 1237) * 31) + (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
