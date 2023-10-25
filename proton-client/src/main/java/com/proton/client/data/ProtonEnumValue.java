package com.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonEnum;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;

/**
 * Wraper class of enum.
 */
public class ProtonEnumValue implements ProtonValue {
    /**
     * Create a new instance representing null value.
     *
     * @param clazz enum class
     * @return new instance representing null value
     */
    public static ProtonEnumValue ofNull(Class<? extends Enum> clazz) {
        return ofNull(null, ProtonEnum.of(clazz));
    }

    /**
     * Create a new instance representing null value.
     *
     * @param type enum type, null is same as {@link ProtonEnum#EMPTY}
     * @return new instance representing null value
     */
    public static ProtonEnumValue ofNull(ProtonEnum type) {
        return ofNull(null, type);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref   object to update, could be null
     * @param clazz enum class
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonEnumValue ofNull(ProtonValue ref, Class<? extends Enum> clazz) {
        return ref instanceof ProtonEnumValue ? ((ProtonEnumValue) ref).set(true, 0)
                : new ProtonEnumValue(ProtonEnum.of(clazz), true, 0);
    }

    /**
     * Update given value to null or create a new instance if {@code ref} is null.
     * 
     * @param ref  object to update, could be null
     * @param type enum type, null is same as {@link ProtonEnum#EMPTY}
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonEnumValue ofNull(ProtonValue ref, ProtonEnum type) {
        return ref instanceof ProtonEnumValue ? ((ProtonEnumValue) ref).set(true, 0)
                : new ProtonEnumValue(type, true, 0);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonEnumValue of(Enum<?> value) {
        return of(null, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @param type  enum type
     * @return object representing the value
     */
    public static ProtonEnumValue of(ProtonEnum type, int value) {
        return of(null, type, value);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @param type  enum type
     * @return object representing the value
     */
    public static ProtonEnumValue of(ProtonEnum type, Number value) {
        return value == null ? ofNull(null, type) : of(null, type, value.intValue());
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonEnumValue of(ProtonValue ref, Enum<?> value) {
        ProtonEnumValue v;
        if (ref instanceof ProtonEnumValue) {
            v = (ProtonEnumValue) ref;
            if (value != null) {
                v.set(false, value.ordinal());
            } else {
                v.resetToNullOrEmpty();
            }
        } else {
            if (value != null) {
                v = new ProtonEnumValue(ProtonEnum.of(value.getClass()), false, value.ordinal());
            } else {
                v = new ProtonEnumValue(ProtonEnum.EMPTY, true, 0);
            }
        }
        return v;
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref   object to update, could be null
     * @param type  enum type, null is same as {@link ProtonEnum#EMPTY}
     * @param value value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonEnumValue of(ProtonValue ref, ProtonEnum type, int value) {
        return ref instanceof ProtonEnumValue ? ((ProtonEnumValue) ref).set(false, value)
                : new ProtonEnumValue(type, false, value);
    }

    private final ProtonEnum type;

    private boolean isNull;
    private int value;

    protected ProtonEnumValue(ProtonEnum type, boolean isNull, int value) {
        this.type = type != null ? type : ProtonEnum.EMPTY;

        set(isNull, value);
    }

    protected ProtonEnumValue set(boolean isNull, int value) {
        this.isNull = isNull;
        this.value = isNull ? 0 : type.validate(value);
        return this;
    }

    /**
     * Gets value.
     *
     * @return value
     */
    public int getValue() {
        return value;
    }

    @Override
    public ProtonEnumValue copy(boolean deep) {
        return new ProtonEnumValue(type, isNull, value);
    }

    @Override
    public boolean isNullOrEmpty() {
        return isNull;
    }

    @Override
    public byte asByte() {
        return (byte) value;
    }

    @Override
    public short asShort() {
        return (short) value;
    }

    @Override
    public int asInteger() {
        return value;
    }

    @Override
    public long asLong() {
        return value;
    }

    @Override
    public BigInteger asBigInteger() {
        return isNull ? null : BigInteger.valueOf(value);
    }

    @Override
    public float asFloat() {
        return value;
    }

    @Override
    public double asDouble() {
        return value;
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNull ? null : BigDecimal.valueOf(value, scale);
    }

    @Override
    public Object asObject() {
        return isNull ? null : type.name(value);
    }

    @Override
    public String asString(int length, Charset charset) {
        if (isNull) {
            return null;
        }

        String str = type.name(value);
        if (length > 0) {
            ProtonChecker.notWithDifferentLength(str.getBytes(charset == null ? StandardCharsets.UTF_8 : charset),
                    length);
        }

        return str;
    }

    @Override
    public ProtonEnumValue resetToNullOrEmpty() {
        return set(true, (byte) 0);
    }

    @Override
    public String toSqlExpression() {
        return isNull ? ProtonValues.NULL_EXPR
                : new StringBuilder().append('\'').append(ProtonUtils.escape(type.name(value), '\'')).append('\'')
                        .toString();
    }

    @Override
    public ProtonEnumValue update(char value) {
        return set(false, value);
    }

    @Override
    public ProtonEnumValue update(byte value) {
        return set(false, value);
    }

    @Override
    public ProtonEnumValue update(short value) {
        return set(false, value);
    }

    @Override
    public ProtonEnumValue update(int value) {
        return set(false, value);
    }

    @Override
    public ProtonEnumValue update(long value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonEnumValue update(float value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonEnumValue update(double value) {
        return set(false, (int) value);
    }

    @Override
    public ProtonEnumValue update(BigInteger value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ProtonEnumValue update(BigDecimal value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.intValueExact());
    }

    @Override
    public ProtonEnumValue update(Enum<?> value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.ordinal());
    }

    @Override
    public ProtonEnumValue update(String value) {
        return value == null ? resetToNullOrEmpty() : set(false, type.value(value));
    }

    @Override
    public ProtonEnumValue update(ProtonValue value) {
        return value == null ? resetToNullOrEmpty() : set(false, value.asInteger());
    }

    @Override
    public ProtonEnumValue update(Object value) {
        if (value instanceof Number) {
            return set(false, ((Number) value).intValue());
        } else if (value instanceof String) {
            return set(false, type.value((String) value));
        } else if (value instanceof ProtonValue) {
            return set(false, ((ProtonValue) value).asInteger());
        }

        ProtonValue.super.update(value);
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) { // too bad this is a mutable class :<
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ProtonEnumValue v = (ProtonEnumValue) obj;
        return isNull == v.isNull && value == v.value && type.equals(v.type);
    }

    @Override
    public int hashCode() {
        // not going to use Objects.hash(isNull, value) due to autoboxing
        final int prime = 31;
        int result = prime + (isNull ? 1231 : 1237);
        result = prime * result + value;
        result = prime * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return ProtonValues.convertToString(this);
    }
}
