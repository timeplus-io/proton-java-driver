package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of string.
 */
public class ProtonStringValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonStringValue ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonStringValue ofNull(ProtonValue ref) {
        return ref instanceof ProtonStringValue ? ((ProtonStringValue) ref).set((String) null)
                : new ProtonStringValue((String) null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonStringValue of(String value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param bytes bytes
     * @return object representing the value
     */
    public static ProtonStringValue of(byte[] bytes) {
        return of(null, bytes);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonStringValue of(ProtonValue ref, String value) {
        return ref instanceof ProtonStringValue ? ((ProtonStringValue) ref).set(value)
                : new ProtonStringValue(value);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param bytes bytes
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonStringValue of(ProtonValue ref, byte[] bytes) {
        return ref instanceof ProtonStringValue ? ((ProtonStringValue) ref).set(bytes)
                : new ProtonStringValue(bytes);
    }

    private boolean binary;
    private byte[] bytes;
    private String value;

    protected ProtonStringValue(String value) {
        update(value);
    }

    protected ProtonStringValue(byte[] bytes) {
        update(bytes);
    }

    protected ProtonStringValue set(String value) {
        this.binary = false;
        this.bytes = null;
        this.value = value;
        return this;
    }

    protected ProtonStringValue set(byte[] bytes) {
        this.binary = true;
        this.bytes = bytes;
        this.value = null;
        return this;
    }

    @Override
    public ProtonStringValue copy(boolean deep) {
        if (bytes == null || !binary) {
            return new ProtonStringValue(value);
        }

        byte[] b = bytes;
        if (deep) {
            b = new byte[bytes.length];
            System.arraycopy(bytes, 0, b, 0, bytes.length);
        }
        return new ProtonStringValue(b);
    }

    @Override
    public boolean isNullOrEmpty() {
        return bytes == null && value == null;
    }

    @Override
    public boolean asBoolean() {
        return ProtonValues.convertToBoolean(asString());
    }

    @Override
    public byte asByte() {
        return isNullOrEmpty() ? (byte) 0 : Byte.parseByte(asString());
    }

    @Override
    public short asShort() {
        return isNullOrEmpty() ? (short) 0 : Short.parseShort(asString());
    }

    @Override
    public int asInteger() {
        return isNullOrEmpty() ? 0 : Integer.parseInt(asString());
    }

    @Override
    public long asLong() {
        return isNullOrEmpty() ? 0L : Long.parseLong(asString());
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : new BigInteger(asString());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : Float.parseFloat(asString());
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : Double.parseDouble(asString());
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger(), scale);
    }

    @Override
    public LocalDate asDate() {
        return isNullOrEmpty() ? null : LocalDate.parse(asString(), ProtonValues.DATE_FORMATTER);
    }

    @Override
    public LocalTime asTime() {
        return isNullOrEmpty() ? null : LocalTime.parse(asString(), ProtonValues.TIME_FORMATTER);
    }

    @Override
    public LocalDateTime asDateTime(int scale) {
        return isNullOrEmpty() ? null : LocalDateTime.parse(asString(), ProtonValues.DATETIME_FORMATTER);
    }

    @Override
    public <T extends Enum<T>> T asEnum(Class<T> enumType) {
        return isNullOrEmpty() ? null : Enum.valueOf(enumType, asString());
    }

    @Override
    public Inet4Address asInet4Address() {
        return ProtonValues.convertToIpv4(asString());
    }

    @Override
    public Inet6Address asInet6Address() {
        return ProtonValues.convertToIpv6(asString());
    }

    @Override
    public Object asObject() {
        return asString(); // bytes != null ? bytes : value;
    }

    @Override
    public byte[] asBinary() {
        if (value != null && bytes == null) {
            bytes = value.getBytes(StandardCharsets.UTF_8);
        }

        return bytes;
    }

    @Override
    public byte[] asBinary(int length, Charset charset) {
        if (value != null && bytes == null) {
            bytes = value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset);
        }

        if (bytes != null && length > 0) {
            return ProtonChecker.notWithDifferentLength(bytes, length);
        } else {
            return bytes;
        }
    }

    @Override
    public String asString() {
        if (bytes != null && value == null) {
            value = new String(bytes, StandardCharsets.UTF_8);
        }

        return value;
    }

    @Override
    public String asString(int length, Charset charset) {
        if (value != null && length > 0) {
            if (bytes == null) {
                bytes = value.getBytes(charset == null ? StandardCharsets.UTF_8 : charset);
            }
            ProtonChecker.notWithDifferentLength(bytes, length);
        }

        return value;
    }

    @Override
    public UUID asUuid() {
        return isNullOrEmpty() ? null : UUID.fromString(asString());
    }

    @Override
    public ProtonStringValue resetToNullOrEmpty() {
        return set((String) null);
    }

    @Override
    public String toSqlExpression() {
        if (isNullOrEmpty()) {
            return ProtonValues.NULL_EXPR;
        } else if (binary) {
            return ProtonValues.convertToUnhexExpression(bytes);
        }
        return ProtonValues.convertToQuotedString(asString());
    }

    @Override
    public ProtonStringValue update(boolean value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(char value) {
        // consistent with asCharacter()
        return set(String.valueOf((int) value));
    }

    @Override
    public ProtonStringValue update(byte value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(byte[] value) {
        return set(value);
    }

    @Override
    public ProtonStringValue update(short value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(int value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(long value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(float value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(double value) {
        return set(String.valueOf(value));
    }

    @Override
    public ProtonValue update(BigInteger value) {
        return set(value == null ? null : String.valueOf(value));
    }

    @Override
    public ProtonValue update(BigDecimal value) {
        return set(value == null ? null : String.valueOf(value));
    }

    @Override
    public ProtonStringValue update(Enum<?> value) {
        return set(value == null ? null : value.name());
    }

    @Override
    public ProtonStringValue update(Inet4Address value) {
        return set(value == null ? null : value.getHostAddress());
    }

    @Override
    public ProtonStringValue update(Inet6Address value) {
        return set(value == null ? null : value.getHostAddress());
    }

    @Override
    public ProtonStringValue update(LocalDate value) {
        return set(value == null ? null : value.format(ProtonValues.DATE_FORMATTER));
    }

    @Override
    public ProtonStringValue update(LocalTime value) {
        return set(value == null ? null : value.format(ProtonValues.TIME_FORMATTER));
    }

    @Override
    public ProtonStringValue update(LocalDateTime value) {
        return set(value == null ? null : value.format(ProtonValues.DATETIME_FORMATTER));
    }

    @Override
    public ProtonStringValue update(String value) {
        return set(value);
    }

    @Override
    public ProtonStringValue update(UUID value) {
        return set(value == null ? null : value.toString());
    }

    @Override
    public ProtonStringValue update(ProtonValue value) {
        return set(value == null ? null : value.asString());
    }

    @Override
    public ProtonStringValue update(Object value) {
        return update(value == null ? null : value.toString());
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (binary ? 1231 : 1237);
        result = prime * result + Arrays.hashCode(bytes);
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ProtonStringValue v = (ProtonStringValue) obj;
        return binary == v.binary && Objects.equals(bytes, v.bytes) && Objects.equals(value, v.value);
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
