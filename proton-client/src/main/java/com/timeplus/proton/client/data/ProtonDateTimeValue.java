package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of LocalDateTime.
 */
public class ProtonDateTimeValue extends ProtonObjectValue<LocalDateTime> {
    static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ProtonDateTimeValue ofNull(int scale, TimeZone tz) {
        return ofNull(null, scale, tz);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param scale scale, only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDateTimeValue ofNull(ProtonValue ref, int scale, TimeZone tz) {
        return ref instanceof ProtonDateTimeValue
                ? (ProtonDateTimeValue) ((ProtonDateTimeValue) ref).set(null)
                : new ProtonDateTimeValue(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ProtonDateTimeValue of(LocalDateTime value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value UTC date time in string
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ProtonDateTimeValue of(String value, int scale, TimeZone tz) {
        return of(null, value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @param scale scale, only used when {@code ref} is null
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDateTimeValue of(ProtonValue ref, LocalDateTime value, int scale, TimeZone tz) {
        return ref instanceof ProtonDateTimeValue
                ? (ProtonDateTimeValue) ((ProtonDateTimeValue) ref).set(value)
                : new ProtonDateTimeValue(value, scale, tz);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     * 
     * @param ref   object to update, could be null
     * @param value UTC date time in string
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonDateTimeValue of(ProtonValue ref, String value, int scale, TimeZone tz) {
        LocalDateTime dateTime = value == null || value.isEmpty() ? null
                : LocalDateTime.parse(value, ProtonValues.DATETIME_FORMATTER);
        return of(ref, dateTime, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;

    protected ProtonDateTimeValue(LocalDateTime value, int scale, TimeZone tz) {
        super(value);
        this.scale = ProtonChecker.between(scale, ProtonValues.PARAM_SCALE, 0, 9);
        this.tz = tz != null ? tz : ProtonValues.UTC_TIMEZONE;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ProtonDateTimeValue copy(boolean deep) {
        return new ProtonDateTimeValue(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().atZone(tz.toZoneId()).toEpochSecond();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().atZone(tz.toZoneId()).toEpochSecond()
                        + getValue().getNano() / ProtonValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().atZone(tz.toZoneId()).toEpochSecond()
                        + getValue().getNano() / ProtonValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().atZone(tz.toZoneId()).toEpochSecond());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        LocalDateTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.atZone(tz.toZoneId()).toEpochSecond()), scale);
            if (scale != 0 && nanoSeconds != 0) {
                v = v.add(BigDecimal.valueOf(nanoSeconds).divide(ProtonValues.NANOS).setScale(scale,
                        RoundingMode.HALF_UP));
            }
        }
        return v;
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : asDateTime(0).toLocalDate();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return getValue();
    }

    @Override
    public Instant asInstant(int scale) {
        return isNullOrEmpty() ? null : getValue().atZone(tz.toZoneId()).toInstant();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().atZone(tz.toZoneId()).toOffsetDateTime();
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().atZone(tz.toZoneId());
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNullOrEmpty()) {
            return null;
        }

        // different formatter for each scale?
        String str = getValue().format(scale > 0 ? ProtonValues.DATETIME_FORMATTER : dateTimeFormatter);
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

        return new StringBuilder().append('\'')
                .append(getValue().format(scale > 0 ? ProtonValues.DATETIME_FORMATTER : dateTimeFormatter))
                .append('\'').toString();
    }

    @Override
    public ProtonDateTimeValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(float value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(double value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ProtonDateTimeValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (scale == 0) {
            set(ProtonValues.convertToDateTime(new BigDecimal(value, 0)));
        } else {
            set(ProtonValues.convertToDateTime(new BigDecimal(value, scale)));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            if (value.scale() != scale) {
                value = value.setScale(scale, RoundingMode.HALF_UP);
            }
            set(ProtonValues.convertToDateTime(value));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(value, LocalTime.MIN));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.of(LocalDate.now(), value));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(LocalDateTime value) {
        set(value);
        return this;
    }

    @Override
    public ProtonDateTimeValue update(Instant value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.ofInstant(value, tz.toZoneId()));
        }
        return this;
    }

    @Override
    public ProtonValue update(OffsetDateTime value) {
        return update(value != null ? value.atZoneSameInstant(tz.toZoneId()).toLocalDateTime() : null);
    }

    @Override
    public ProtonValue update(ZonedDateTime value) {
        return update(value != null ? LocalDateTime.ofInstant(value.toInstant(), tz.toZoneId()) : null);
    }

    @Override
    public ProtonDateTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.parse(value, ProtonValues.DATETIME_FORMATTER));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asDateTime(scale));
        }
        return this;
    }

    @Override
    public ProtonDateTimeValue update(Object value) {
        if (value instanceof LocalDateTime) {
            set((LocalDateTime) value);
        } else if (value instanceof String) {
            update((String) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
