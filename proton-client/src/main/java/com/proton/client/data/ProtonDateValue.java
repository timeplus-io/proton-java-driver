package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of LocalDate.
 */
public class ProtonDateValue extends ProtonObjectValue<LocalDate> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonDateValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDateValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonDateValue ? (ProtonDateValue) ((ProtonDateValue) ref).set(null)
                : new ProtonDateValue(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonDateValue of(LocalDate value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param epochDay epoch day
     * @return object representing the value
     */
    public static ProtonDateValue of(long epochDay) {
        return of(null, LocalDate.ofEpochDay(epochDay));
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDateValue of(ProtonValue ref, LocalDate value) {
        return ref instanceof ProtonDateValue ? (ProtonDateValue) ((ProtonDateValue) ref).update(value)
                : new ProtonDateValue(value);
    }

    protected ProtonDateValue(LocalDate value) {
        super(value);
    }

    @Override
    public ProtonDateValue copy(boolean deep) {
        return new ProtonDateValue(getValue());
    }

    @Override
    public byte asByte() {
        return (byte) asLong();
    }

    @Override
    public short asShort() {
        return (short) asLong();
    }

    @Override
    public int asInteger() {
        return (int) asLong();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toEpochDay();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toEpochDay());
    }

    @Override
    public float asFloat() {
        return asLong();
    }

    @Override
    public double asDouble() {
        return asLong();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        return getValue();
    }

    @Override
    public final LocalTime asTime(int scale) {
        return isNullOrEmpty() ? null : LocalTime.ofSecondOfDay(0L);
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return LocalDateTime.of(getValue(), ProtonValues.TIME_ZERO);
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        String str = asDate().format(ProtonValues.DATE_FORMATTER);
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        }
        return new StringBuilder().append('\'').append(asDate().format(ProtonValues.DATE_FORMATTER)).append('\'')
                .toString();
    }

    @Override
    public ProtonDateValue update(byte value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ProtonDateValue update(short value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ProtonDateValue update(int value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ProtonDateValue update(long value) {
        set(LocalDate.ofEpochDay(value));
        return this;
    }

    @Override
    public ProtonDateValue update(float value) {
        set(LocalDate.ofEpochDay((long) value));
        return this;
    }

    @Override
    public ProtonDateValue update(double value) {
        set(LocalDate.ofEpochDay((long) value));
        return this;
    }

    @Override
    public ProtonDateValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.longValueExact()));
        }
        return this;
    }

    @Override
    public ProtonDateValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.longValueExact()));
        }
        return this;
    }

    @Override
    public ProtonDateValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.ofEpochDay(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonDateValue update(LocalDate value) {
        set(value);
        return this;
    }

    @Override
    public ProtonDateValue update(LocalTime value) {
        return this;
    }

    @Override
    public ProtonDateValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toLocalDate());
        }
        return this;
    }

    @Override
    public ProtonDateValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDate.parse(value, ProtonValues.DATE_FORMATTER));
        }
        return this;
    }

    @Override
    public ProtonDateValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asDate());
        }
        return this;
    }

    @Override
    public ProtonDateValue update(Object value) {
        if (value instanceof LocalDate) {
            set((LocalDate) value);
        } else if (value instanceof LocalDateTime) {
            set(((LocalDateTime) value).toLocalDate());
        } else if (value instanceof LocalTime) {
            set(LocalDate.now());
        } else {
            super.update(value);
        }
        return this;
    }
}
