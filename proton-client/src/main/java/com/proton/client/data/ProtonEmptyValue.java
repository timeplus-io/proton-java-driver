package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wrapper class of Nothing.
 */
public final class ProtonEmptyValue implements ProtonValue {
    /**
     * Singleton.
     */
    public static final ProtonEmptyValue INSTANCE = new ProtonEmptyValue();

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return null;
    }

    @Override
    public BigInteger asBigInteger() {
        return null;
    }

    @Override
    public byte asByte() {
        return (byte) 0;
    }

    @Override
    public double asDouble() {
        return 0D;
    }

    @Override
    public float asFloat() {
        return 0F;
    }

    @Override
    public int asInteger() {
        return 0;
    }

    @Override
    public long asLong() {
        return 0L;
    }

    @Override
    public Object asObject() {
        return null;
    }

    @Override
    public short asShort() {
        return (short) 0;
    }

    @Override
    public ProtonValue copy(boolean deep) {
        return INSTANCE;
    }

    @Override
    public boolean isNullOrEmpty() {
        return true;
    }

    @Override
    public ProtonValue resetToNullOrEmpty() {
        return INSTANCE;
    }

    @Override
    public String toSqlExpression() {
        return ProtonValues.NULL_EXPR;
    }

    @Override
    public ProtonValue update(byte value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(short value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(int value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(long value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(float value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(double value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(BigInteger value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(BigDecimal value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(String value) {
        return INSTANCE;
    }

    @Override
    public ProtonValue update(ProtonValue value) {
        return INSTANCE;
    }

    @Override
    public String toString() {
        return "";
    }

    private ProtonEmptyValue() {
    }
}
