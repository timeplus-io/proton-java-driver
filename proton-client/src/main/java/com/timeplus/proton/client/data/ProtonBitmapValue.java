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

import com.timeplus.proton.client.ProtonDataType;
import com.timeplus.proton.client.ProtonUtils;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Wraper class of Bitmap.
 */
public class ProtonBitmapValue extends ProtonObjectValue<ProtonBitmap> {
    /**
     * Create a new instance representing empty value.
     *
     * @param valueType value type, must be native integer
     * @return new instance representing empty value
     */
    public static ProtonBitmapValue ofEmpty(ProtonDataType valueType) {
        return ofEmpty(null, valueType);
    }

    /**
     * Update given value to empty or create a new instance if {@code ref} is null.
     * 
     * @param ref       object to update, could be null
     * @param valueType value type, must be native integer
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonBitmapValue ofEmpty(ProtonValue ref, ProtonDataType valueType) {
        ProtonBitmap v = ProtonBitmap.empty(valueType);
        return ref instanceof ProtonBitmapValue ? (ProtonBitmapValue) ((ProtonBitmapValue) ref).set(v)
                : new ProtonBitmapValue(v);
    }

    /**
     * Wrap the given value.
     *
     * @param value value
     * @return object representing the value
     */
    public static ProtonBitmapValue of(ProtonBitmap value) {
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
    public static ProtonBitmapValue of(ProtonValue ref, ProtonBitmap value) {
        if (value == null) {
            value = ProtonBitmap.empty();
        }

        return ref instanceof ProtonBitmapValue ? (ProtonBitmapValue) ((ProtonBitmapValue) ref).set(value)
                : new ProtonBitmapValue(value);
    }

    protected ProtonBitmapValue(ProtonBitmap value) {
        super(value);
    }

    @Override
    public ProtonBitmapValue copy(boolean deep) {
        return new ProtonBitmapValue(getValue());
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public byte asByte() {
        if (isNullOrEmpty()) {
            return (byte) 0;
        }

        ProtonBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ProtonUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (byte) v.toLongArray()[0] : (byte) v.toIntArray()[0];
    }

    @Override
    public short asShort() {
        if (isNullOrEmpty()) {
            return (short) 0;
        }

        ProtonBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ProtonUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (short) v.toLongArray()[0] : (short) v.toIntArray()[0];
    }

    @Override
    public int asInteger() {
        if (isNullOrEmpty()) {
            return 0;
        }

        ProtonBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ProtonUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? (int) v.toLongArray()[0] : v.toIntArray()[0];
    }

    @Override
    public long asLong() {
        if (isNullOrEmpty()) {
            return 0;
        }

        ProtonBitmap v = getValue();
        if (v.getCardinality() != 1) {
            throw new IllegalArgumentException(
                    ProtonUtils.format("Expect only one element but we got %d", v.getLongCardinality()));
        }
        return v.innerType.getByteLength() > 4 ? v.toLongArray()[0] : v.toIntArray()[0];
    }

    @Override
    public BigInteger asBigInteger() {
        if (isNullOrEmpty()) {
            return null;
        }

        return BigInteger.valueOf(asLong());
    }

    @Override
    public float asFloat() {
        return isNullOrEmpty() ? 0F : (float) asInteger();
    }

    @Override
    public double asDouble() {
        return isNullOrEmpty() ? 0D : (double) asLong();
    }

    @Override
    public BigDecimal asBigDecimal(int scale) {
        return isNullOrEmpty() ? null : new BigDecimal(asBigInteger()).setScale(scale);
    }

    @Override
    public Object asObject() {
        return getValue();
    }

    public long getCardinality() {
        return isNullOrEmpty() ? 0L : getValue().getLongCardinality();
    }

    @Override
    public String toSqlExpression() {
        return isNullOrEmpty() ? ProtonValues.NULL_EXPR : String.valueOf(getValue());
    }

    @Override
    public ProtonBitmapValue update(boolean value) {
        set(ProtonBitmap.wrap(value ? (byte) 1 : (byte) 0));
        return this;
    }

    @Override
    public ProtonBitmapValue update(char value) {
        set(ProtonBitmap.wrap((short) value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(byte value) {
        set(ProtonBitmap.wrap(value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(short value) {
        set(ProtonBitmap.wrap(value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(int value) {
        set(ProtonBitmap.wrap(value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(long value) {
        set(ProtonBitmap.wrap(value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(float value) {
        set(ProtonBitmap.wrap((int) value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(double value) {
        set(ProtonBitmap.wrap((long) value));
        return this;
    }

    @Override
    public ProtonBitmapValue update(BigInteger value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.longValue()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(BigDecimal value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.longValue()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(Enum<?> value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.ordinal()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(Inet4Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(ProtonValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(Inet6Address value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(ProtonValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(LocalDate value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.toEpochDay()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(LocalTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.toSecondOfDay()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(LocalDateTime value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(value.toEpochSecond(ZoneOffset.UTC)));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(String value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(Long.parseLong(value)));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(UUID value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else {
            set(ProtonBitmap.wrap(ProtonValues.convertToBigInteger(value).longValue()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value instanceof ProtonBitmapValue) {
            set(((ProtonBitmapValue) value).getValue());
        } else {
            set(ProtonBitmap.wrap(value.asInteger()));
        }
        return this;
    }

    @Override
    public ProtonBitmapValue update(Object value) {
        if (value instanceof ProtonBitmap) {
            set((ProtonBitmap) value);
            return this;
        }

        super.update(value);
        return this;
    }
}
