package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.function.Function;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

public class ProtonMapValue extends ProtonObjectValue<Map<?, ?>> {
    private static final String DEFAULT_STRING_KEY = "1";
    private static final String DEFAULT_UUID_KEY = "00000000-0000-0000-0000-000000000000";

    /**
     * Creates an empty map.
     *
     * @param keyType   non-null class of key
     * @param valueType non-null class of value
     * @return empty map
     */
    public static ProtonMapValue ofEmpty(Class<?> keyType, Class<?> valueType) {
        return new ProtonMapValue(Collections.emptyMap(), keyType, valueType);
    }

    /**
     * Wrap the given value.
     *
     * @param value     value
     * @param keyType   non-null class of key
     * @param valueType non-null class of value
     * @return object representing the value
     */
    public static ProtonMapValue of(Map<?, ?> value, Class<?> keyType, Class<?> valueType) {
        return of(null, value, keyType, valueType);
    }

    /**
     * Update value of the given object or create a new instance if {@code ref} is
     * null.
     *
     * @param ref       object to update, could be null
     * @param value     value
     * @param keyType   non-null class of key
     * @param valueType non-null class of value
     * @return same object as {@code ref} or a new instance if it's null
     */
    public static ProtonMapValue of(ProtonValue ref, Map<?, ?> value, Class<?> keyType, Class<?> valueType) {
        return ref instanceof ProtonMapValue ? ((ProtonMapValue) ref).set(value)
                : new ProtonMapValue(value, keyType, valueType);
    }

    private final Class<?> keyType;
    private final Class<?> valueType;

    protected ProtonMapValue(Map<?, ?> value, Class<?> keyType, Class<?> valueType) {
        super(value);

        this.keyType = ProtonChecker.nonNull(keyType, "keyType");
        this.valueType = ProtonChecker.nonNull(valueType, "valueType");
    }

    protected Object getDefaultKey() {
        Object key;

        if (keyType == String.class) {
            key = DEFAULT_STRING_KEY;
        } else if (keyType == UUID.class) {
            key = DEFAULT_UUID_KEY;
        } else if (keyType == Byte.class) {
            key = Byte.valueOf((byte) 1);
        } else if (keyType == Short.class) {
            key = Short.valueOf((short) 1);
        } else if (keyType == Integer.class) {
            key = Integer.valueOf(1);
        } else if (keyType == Long.class) {
            key = Long.valueOf(1L);
        } else if (keyType == Float.class) {
            key = Float.valueOf(1F);
        } else if (keyType == Double.class) {
            key = Double.valueOf(1D);
        } else if (keyType == BigInteger.class) {
            key = BigInteger.ONE;
        } else if (keyType == BigDecimal.class) {
            key = BigDecimal.ONE;
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + keyType);
        }

        return key;
    }

    @Override
    protected ProtonMapValue set(Map<?, ?> value) {
        super.set(ProtonChecker.nonNull(value, "value"));
        return this;
    }

    @Override
    public ProtonMapValue copy(boolean deep) {
        if (!deep) {
            return new ProtonMapValue(getValue(), keyType, valueType);
        }

        Map<?, ?> value = getValue();
        Map<Object, Object> newValue = new LinkedHashMap<>();
        newValue.putAll(value);
        return new ProtonMapValue(newValue, keyType, valueType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> asMap() {
        return (Map<Object, Object>) getValue();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> asMap(Class<K> keyClass, Class<V> valueClass) {
        if (!keyType.isAssignableFrom(keyClass) || !valueType.isAssignableFrom(valueClass)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Incompatible types, expected (%s:%s) but got (%s:%s)",
                            keyType.getName(), valueType.getName(), keyClass, valueClass));
        }

        return (Map<K, V>) getValue();
    }

    @Override
    public String asString(int length, Charset charset) {
        Map<?, ?> value = getValue();
        if (value == null || value.isEmpty()) {
            return "{}";
        }
        StringBuilder builder = new StringBuilder().append('{');
        for (Entry<?, ?> e : value.entrySet()) {
            builder.append(String.valueOf(e.getKey())).append(':').append(String.valueOf(e.getValue())).append(',');
        }
        builder.setLength(builder.length() - 1);

        String str = builder.append('}').toString();
        return length > 0 ? ProtonChecker.notWithDifferentLength(str, length) : str;
    }

    @Override
    public boolean isNullOrEmpty() {
        return getValue().isEmpty();
    }

    @Override
    public ProtonMapValue resetToNullOrEmpty() {
        set(Collections.emptyMap());
        return this;
    }

    @Override
    public String toSqlExpression() {
        Map<?, ?> value = getValue();
        if (value == null || value.isEmpty()) {
            return "{}";
        }

        StringBuilder builder = new StringBuilder().append('{');
        // non-null number, string or uuid
        Function<Object, String> keySerializer = String.class == keyType || UUID.class == keyType
                ? ProtonValues::convertToQuotedString
                : ProtonValues::convertToString;
        // any value which may or may not be null
        Function<Object, String> valueSerializer = ProtonValues::convertToSqlExpression;

        for (Entry<?, ?> e : value.entrySet()) {
            builder.append(keySerializer.apply(e.getKey())).append(" : ").append(valueSerializer.apply(e.getValue()))
                    .append(',');
        }
        builder.setLength(builder.length() - 1);

        return builder.append('}').toString();
    }

    @Override
    public ProtonMapValue update(byte value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(short value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(int value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(long value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(float value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(double value) {
        set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
        return this;
    }

    @Override
    public ProtonMapValue update(BigInteger value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(BigDecimal value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(String value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }

        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(Enum<?> value) {
        Object v;
        if (value == null) {
            v = null;
        } else if (valueType == value.getClass()) {
            v = value;
        } else if (valueType == String.class) {
            v = value.name();
        } else if (keyType == Byte.class) {
            v = Byte.valueOf((byte) value.ordinal());
        } else if (keyType == Short.class) {
            v = Short.valueOf((short) value.ordinal());
        } else if (keyType == Integer.class) {
            v = Integer.valueOf(value.ordinal());
        } else if (keyType == Long.class) {
            v = Long.valueOf(value.ordinal());
        } else if (keyType == Float.class) {
            v = Float.valueOf(value.ordinal());
        } else if (keyType == Double.class) {
            v = Double.valueOf(value.ordinal());
        } else if (keyType == BigInteger.class) {
            v = BigInteger.valueOf(value.ordinal());
        } else if (keyType == BigDecimal.class) {
            v = BigDecimal.valueOf(value.ordinal());
        } else {
            throw newUnsupportedException(value.getClass().getName(), valueType.getName());
        }
        return set(Collections.singletonMap(getDefaultKey(), v));
    }

    @Override
    public ProtonMapValue update(Inet4Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(Inet6Address value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(LocalDate value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(LocalTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(LocalDateTime value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonMapValue update(Map<?, ?> value) {
        return set(value == null ? Collections.emptyMap() : value);
    }

    @Override
    public ProtonMapValue update(UUID value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        return set(Collections.singletonMap(getDefaultKey(), valueType.cast(value)));
    }

    @Override
    public ProtonValue updateUnknown(Object value) {
        if (value == null) {
            return resetToNullOrEmpty();
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }

    @Override
    public ProtonMapValue update(ProtonValue value) {
        if (value == null) {
            resetToNullOrEmpty();
        } else if (value instanceof ProtonMapValue) {
            set(((ProtonMapValue) value).getValue());
        } else {
            set(value.asMap());
        }

        return this;
    }

    @Override
    public ProtonValue update(Object value) {
        if (value instanceof Map) {
            set((Map<?, ?>) value);
        } else {
            super.update(value);
        }
        return this;
    }
}
