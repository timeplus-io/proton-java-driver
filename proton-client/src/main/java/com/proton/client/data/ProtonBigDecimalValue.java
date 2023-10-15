package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of BigDecimal.
 */
public class ProtonBigDecimalValue extends ProtonObjectValue<BigDecimal> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonBigDecimalValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonBigDecimalValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonBigDecimalValue
                ? (ProtonBigDecimalValue) ((ProtonBigDecimalValue) ref).set(null)
                : new ProtonBigDecimalValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonBigDecimalValue of(BigDecimal value) {
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
    public static ProtonBigDecimalValue of(ProtonValue ref, BigDecimal value) {
        return ref instanceof ProtonBigDecimalValue
                ? (ProtonBigDecimalValue) ((ProtonBigDecimalValue) ref).set(value)
                : new ProtonBigDecimalValue(value);
    }

    protected ProtonBigDecimalValue(BigDecimal value) {
        super(value);
    }

    @Override
    public ProtonBigDecimalValue copy(boolean deep) {
        return new ProtonBigDecimalValue(getValue());
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
        if (isNullOrEmpty()) {
            return null;
        }

        BigDecimal value = getValue();
        if (value.remainder(BigDecimal.ONE) != BigDecimal.ZERO) {
            throw new IllegalArgumentException("Failed to convert BigDecimal to BigInteger: " + value);
        }

        return value.toBigInteger();
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
    public BigDecimal asBigDecimal() {
        return getValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        BigDecimal v = getValue();
        if (v != null && v.scale() != scale) {
            v = v.setScale(scale, RoundingMode.DOWN);
        }

        return v;
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    public int getScale() {
        return isNullOrEmpty() ? 0 : getValue().scale();
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ProtonBigDecimalValue update(boolean value) {
        set(value ? BigDecimal.ONE : BigDecimal.ZERO);
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(char value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(byte value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(short value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(int value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(long value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(float value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(double value) {
        set(BigDecimal.valueOf(value));
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(value));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(BigDecimal value) {
        set(value);
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ProtonValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ProtonValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(BigDecimal.valueOf(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(value));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(new BigDecimal(ProtonValues.convertToBigInteger(value)));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asBigDecimal(getScale()));
        }
        return this;
    }

    @Override
    public ProtonBigDecimalValue update(Object value) {
        if (value instanceof BigDecimal) {
            set((BigDecimal) value);
            return this;
        }

        super.update(value);
        return this;
    }
}
