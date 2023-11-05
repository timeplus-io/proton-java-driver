package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of BigInteger.
 */
public class ProtonBigIntegerValue extends ProtonObjectValue<BigInteger> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonBigIntegerValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonBigIntegerValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonBigIntegerValue
                ? (ProtonBigIntegerValue) ((ProtonBigIntegerValue) ref).set(null)
                : new ProtonBigIntegerValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonBigIntegerValue of(BigInteger value) {
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
    public static ProtonBigIntegerValue of(ProtonValue ref, BigInteger value) {
        return ref instanceof ProtonBigIntegerValue
                ? (ProtonBigIntegerValue) ((ProtonBigIntegerValue) ref).set(value)
                : new ProtonBigIntegerValue(value);
    }

    protected ProtonBigIntegerValue(BigInteger value) {
        super(value);
    }

    @Override
    public ProtonBigIntegerValue copy(boolean deep) {
        return new ProtonBigIntegerValue(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : getValue().byteValueExact();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : getValue().shortValueExact();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : getValue().intValueExact();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().longValueExact();
    }

    @Override
    public BigInteger asBigInteger() {
        return getValue();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : getValue().floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : getValue().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(getValue(), scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ProtonBigIntegerValue update(boolean value) {
        set(value ? BigInteger.ONE : BigInteger.ZERO);
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(char value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(byte value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(short value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(int value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(long value) {
        set(BigInteger.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(float value) {
        set(BigDecimal.valueOf(value).toBigInteger());
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(double value) {
        set(BigDecimal.valueOf(value).toBigInteger());
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(BigInteger value) {
        set(value);
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toBigIntegerExact());
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigInteger(value));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonValues.convertToBigInteger(value));
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asBigInteger());
        }
        return this;
    }

    @Override
    public ProtonBigIntegerValue update(Object value) {
        if (value instanceof BigInteger) {
            set((BigInteger) value);
            return this;
        }

        super.update(value);
        return this;
    }
}
