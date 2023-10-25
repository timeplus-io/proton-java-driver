package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.TimeZone;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of OffsetDateTime.
 */
public class ProtonOffsetDateTimeValue extends ProtonObjectValue<OffsetDateTime> {
    /**
     * Create a new instance representing null getValue().
     *
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return new instance representing null value
     */
    public static ProtonOffsetDateTimeValue ofNull(int scale, TimeZone tz) {
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
    public static ProtonOffsetDateTimeValue ofNull(ProtonValue ref, int scale, TimeZone tz) {
        return ref instanceof ProtonOffsetDateTimeValue
                ? (ProtonOffsetDateTimeValue) ((ProtonOffsetDateTimeValue) ref).set(null)
                : new ProtonOffsetDateTimeValue(null, scale, tz);
    }

    /**
     * Wrap the given getValue().
     *
     * @param value value
     * @param scale scale
     * @param tz    time zone, null is treated as {@code UTC}
     * @return object representing the value
     */
    public static ProtonOffsetDateTimeValue of(LocalDateTime value, int scale, TimeZone tz) {
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
    public static ProtonOffsetDateTimeValue of(ProtonValue ref, LocalDateTime value, int scale, TimeZone tz) {
        OffsetDateTime v = null;
        if (value != null) {
            v = value.atZone(
                    tz == null || tz.equals(ProtonValues.UTC_TIMEZONE) ? ProtonValues.UTC_ZONE : tz.toZoneId())
                    .toOffsetDateTime();
        }
        return ref instanceof ProtonOffsetDateTimeValue
                ? (ProtonOffsetDateTimeValue) ((ProtonOffsetDateTimeValue) ref).set(v)
                : new ProtonOffsetDateTimeValue(v, scale, tz);
    }

    private final int scale;
    private final TimeZone tz;

    protected ProtonOffsetDateTimeValue(OffsetDateTime value, int scale, TimeZone tz) {
        super(value);
        this.scale = ProtonChecker.between(scale, ProtonValues.PARAM_SCALE, 0, 9);
        this.tz = tz == null || tz.equals(ProtonValues.UTC_TIMEZONE) ? ProtonValues.UTC_TIMEZONE : tz;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public ProtonOffsetDateTimeValue copy(boolean deep) {
        return new ProtonOffsetDateTimeValue(getValue(), scale, tz);
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : (byte) getValue().toEpochSecond();
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : (short) getValue().toEpochSecond();
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : (int) getValue().toEpochSecond();
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : getValue().toEpochSecond();
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F
                : getValue().toEpochSecond() + getValue().getNano() / ProtonValues.NANOS.floatValue();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D
                : getValue().toEpochSecond() + getValue().getNano() / ProtonValues.NANOS.doubleValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : BigInteger.valueOf(getValue().toEpochSecond());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        OffsetDateTime value = getValue();
        BigDecimal v = null;
        if (value != null) {
            int nanoSeconds = value.getNano();
            v = new BigDecimal(BigInteger.valueOf(value.toEpochSecond()), scale);
            if (scale != 0 && nanoSeconds != 0) {
                v = v.add(BigDecimal.valueOf(nanoSeconds).divide(ProtonValues.NANOS).setScale(scale,
                        RoundingMode.HALF_UP));
            }
        }
        return v;
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : asOffsetDateTime(0).toLocalDate();
    }

    @Override
    public LocalTime asTime(int scale) {
        return isNullOrEmpty() ? null : asOffsetDateTime(scale).toLocalTime();
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toLocalDateTime();
    }

    @Override
    public Instant asInstant(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toInstant();
    }

    @Override
    public OffsetDateTime asOffsetDateTime(int scale) {
        return getValue();
    }

    @Override
    public ZonedDateTime asZonedDateTime(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        return getValue().toZonedDateTime();
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
        String str = asDateTime(scale)
                .format(scale > 0 ? ProtonValues.DATETIME_FORMATTER : ProtonDateTimeValue.dateTimeFormatter);
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(
                    str.getBytes(charset == null ? StandardCharsets.US_ASCII : charset), length);
        }

        return str;
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        }

        return new StringBuilder().append('\'')
                .append(asDateTime(scale).format(
                        scale > 0 ? ProtonValues.DATETIME_FORMATTER : ProtonDateTimeValue.dateTimeFormatter))
                .append('\'').toString();
    }

    @Override
    public ProtonOffsetDateTimeValue update(byte value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(short value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(int value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(long value) {
        return update(BigInteger.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(float value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(double value) {
        return update(BigDecimal.valueOf(value));
    }

    @Override
    public ProtonOffsetDateTimeValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (scale == 0) {
            set(ProtonValues.convertToDateTime(new BigDecimal(value, 0), tz).toOffsetDateTime());
        } else {
            set(ProtonValues.convertToDateTime(new BigDecimal(value, scale), tz).toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            if (value.scale() != scale) {
                value = value.setScale(scale, RoundingMode.HALF_UP);
            }
            set(ProtonValues.convertToDateTime(value, tz).toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(BigInteger.valueOf(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            LocalDateTime dateTime = LocalDateTime.of(value, LocalTime.MIN);
            set(tz != null ? dateTime.atZone(tz.toZoneId()).toOffsetDateTime() : dateTime.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            LocalDateTime dateTime = LocalDateTime.of(LocalDate.now(), value);
            set(tz != null ? dateTime.atZone(tz.toZoneId()).toOffsetDateTime() : dateTime.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(tz != null && !tz.equals(ProtonValues.UTC_TIMEZONE) ? value.atZone(tz.toZoneId()).toOffsetDateTime()
                    : value.atOffset(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(Instant value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(OffsetDateTime.ofInstant(value, tz.toZoneId()));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(OffsetDateTime value) {
        set(value);
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(ZonedDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(LocalDateTime.parse(value, ProtonValues.DATETIME_FORMATTER).atZone(tz.toZoneId())
                    .toOffsetDateTime());
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asOffsetDateTime(scale));
        }
        return this;
    }

    @Override
    public ProtonOffsetDateTimeValue update(Object value) {
        if (value instanceof OffsetDateTime) {
            set((OffsetDateTime) value);
        } else if (value instanceof String) {
            update((String) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
