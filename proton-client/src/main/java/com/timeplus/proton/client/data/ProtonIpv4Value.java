package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of Inet4Address.
 */
public class ProtonIpv4Value extends ProtonObjectValue<Inet4Address> {
    /**
     * Create a new instance representing null value.
     *
     * @return new instance representing null value
     */
    public static ProtonIpv4Value ofNull() {
        return ofNull(null);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref object to update, could be null
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonIpv4Value ofNull(ProtonValue ref) {
        return ref instanceof ProtonIpv4Value ? (ProtonIpv4Value) ((ProtonIpv4Value) ref).set(null)
                : new ProtonIpv4Value(null);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonIpv4Value of(Inet4Address value) {
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
    public static ProtonIpv4Value of(ProtonValue ref, Inet4Address value) {
        return ref instanceof ProtonIpv4Value ? (ProtonIpv4Value) ((ProtonIpv4Value) ref).set(value)
                : new ProtonIpv4Value(value);
    }

    protected ProtonIpv4Value(Inet4Address value) {
        super(value);
    }

    @Override
    public ProtonIpv4Value copy(boolean deep) {
        return new ProtonIpv4Value(getValue());
    }

    @Override
    public byte asByte() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? (byte) 0 : bigInt.byteValueExact();
    }

    @Override
    public short asShort() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? (short) 0 : bigInt.shortValueExact();
    }

    @Override
    public int asInteger() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0 : bigInt.intValueExact();
    }

    @Override
    public long asLong() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0L : bigInt.longValue();
    }

    @Override
    public BigInteger asBigInteger() {
        return isNullOrEmpty() ? null : new BigInteger(1, getValue().getAddress());
    }

    @Override
    public float asFloat() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0F : bigInt.floatValue();
    }

    @Override
    public double asDouble() {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? 0D : bigInt.doubleValue();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        BigInteger bigInt = asBigInteger();
        return bigInt == null ? null : new BigDecimal(bigInt, scale);
    }

    @Override
    public Inet4Address asInet4Address() {
        return getValue();
    }

    @Override
    public Inet6Address asInet6Address() {
        return ProtonValues.convertToIpv6(getValue());
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

        String str = String.valueOf(getValue().getHostAddress());
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
        return new StringBuilder().append('\'').append(getValue().getHostAddress()).append('\'').toString();
    }

    @Override
    public ProtonIpv4Value update(byte value) {
        return update((int) value);
    }

    @Override
    public ProtonIpv4Value update(short value) {
        return update((int) value);
    }

    @Override
    public ProtonIpv4Value update(int value) {
        set(ProtonValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ProtonIpv4Value update(long value) {
        return update((int) value);
    }

    @Override
    public ProtonIpv4Value update(float value) {
        return update((int) value);
    }

    @Override
    public ProtonIpv4Value update(double value) {
        return update((int) value);
    }

    @Override
    public ProtonIpv4Value update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.intValueExact());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.intValueExact());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.ordinal());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(Inet4Address value) {
        set(value);
        return this;
    }

    @Override
    public ProtonIpv4Value update(Inet6Address value) {
        set(ProtonValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ProtonIpv4Value update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update((int) value.toEpochDay());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update(value.toSecondOfDay());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            update((int) value.toEpochSecond(ZoneOffset.UTC));
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(String value) {
        set(ProtonValues.convertToIpv4(value));
        return this;
    }

    @Override
    public ProtonIpv4Value update(UUID value) {
        BigInteger v = ProtonValues.convertToBigInteger(value);
        if (v == null) {
            resetToNullOrEmpty();
        } else {
            update(v.intValueExact());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(value.asInet4Address());
        }
        return this;
    }

    @Override
    public ProtonIpv4Value update(Object value) {
        if (value instanceof Inet4Address) {
            set((Inet4Address) value);
        } else if (value instanceof Inet6Address) {
            set(ProtonValues.convertToIpv4((Inet6Address) value));
        } else {
            super.update(value);
        }
        return this;
    }
}
