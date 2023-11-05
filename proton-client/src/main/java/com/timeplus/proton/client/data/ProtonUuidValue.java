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
 * Wraper class of string.
 */
public class ProtonUuidValue extends ProtonObjectValue<UUID> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonUuidValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonUuidValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonUuidValue ? (ProtonUuidValue) ((ProtonUuidValue) ref).set(null)
                : new ProtonUuidValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonUuidValue of(UUID value) {
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
    public static ProtonUuidValue of(ProtonValue ref, UUID value) {
        return ref instanceof ProtonUuidValue ? (ProtonUuidValue) ((ProtonUuidValue) ref).set(value)
                : new ProtonUuidValue(value);
    }

    protected ProtonUuidValue(UUID value) {
        super(value);
    }

    @Override
    public ProtonUuidValue copy(boolean deep) {
        return new ProtonUuidValue(getValue());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : asBigInteger().byteValueExact();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : asBigInteger().shortValueExact();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : asBigInteger().intValueExact();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : asBigInteger().longValueExact();
    }

    @Override
    public BigInteger asBigInteger() {
        return ProtonValues.convertToBigInteger(getValue());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : asBigInteger().floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : asBigInteger().doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public UUID asUuid() {
        return getValue();
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(getValue().toString()).append('\'').toString();
    }

    @Override
    public ProtonUuidValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonUuidValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonUuidValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonUuidValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonUuidValue update(float value) {
        return update(BigDecimal.valueOf(value).toBigIntegerExact());
    }

    @Override
    public ProtonUuidValue update(double value) {
        return update(BigDecimal.valueOf(value).toBigIntegerExact());
    }

    @Override
    public ProtonUuidValue update(BigInteger value) {
        set(ProtonValues.convertToUuid(value));
        return this;
    }

    @Override
    public ProtonUuidValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.toBigIntegerExact());
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(Inet4Address value) {
        return update(ProtonValues.convertToBigInteger(value));
    }

    @Override
    public ProtonUuidValue update(Inet6Address value) {
        return update(ProtonValues.convertToBigInteger(value));
    }

    @Override
    public ProtonUuidValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(UUID.fromString(value));
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(UUID value) {
        set(value);
        return this;
    }

    @Override
    public ProtonUuidValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asUuid());
        }
        return this;
    }

    @Override
    public ProtonUuidValue update(Object value) {
        if (value instanceof UUID) {
            set((UUID) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
