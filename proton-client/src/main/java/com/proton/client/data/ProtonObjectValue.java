package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

public abstract class ProtonObjectValue<T> implements ProtonValue {
    // a nested structure like Map might not be always serializable
    @SuppressWarnings("squid:S1948")
    private T value;

    protected ProtonObjectValue(T value) {
        set(value);
    }

    protected ProtonObjectValue<T> set(T value) {
        this.value = value;
        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public T getValue() {
        return value;
    }

    @Override
    public boolean isNullOrEmpty() {
        return value == null;
    }

    @Override
    public byte asByte() {
        if (isNullOrEmpty()) {
            return (byte) 0;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_BYTE);
    }

    @Override
    public short asShort() {
        if (isNullOrEmpty()) {
            return (short) 0;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_SHORT);
    }

    @Override
    public int asInteger() {
        if (isNullOrEmpty()) {
            return 0;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_INT);
    }

    @Override
    public long asLong() {
        if (isNullOrEmpty()) {
            return 0L;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_LONG);
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNullOrEmpty()) {
            return null;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_BIG_INTEGER);
    }

    @Override
    public float asFloat() {
        if (isNullOrEmpty()) {
            return 0F;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_FLOAT);
    }

    @Override
    public double asDouble() {
        if (isNullOrEmpty()) {
            return 0D;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_DOUBLE);
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        if (isNullOrEmpty()) {
            return null;
        }

        throw newUnsupportedException(ProtonValues.TYPE_OBJECT, ProtonValues.TYPE_BIG_DECIMAL);
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

        String str = value.toString();
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ProtonObjectValue<T> resetToNullOrEmpty() {
        return set(null);
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : asString();
    }

    @Override
    public ProtonValue update(Object value) {
        return ProtonValue.super.update(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ProtonObjectValue<?> v = (ProtonObjectValue<?>) obj;
        return value == v.value || (value != null && value.equals(v.value));
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
