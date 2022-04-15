package com.clickhouse.client;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Basic ClickHouse data types.
 *
 * <p>
 * This list is based on the list of data type families returned by
 * {@code SELECT * FROM system.data_type_families}
 *
 * <p>
 * {@code low_cardinality} and {@code nullable} are technically data types in
 * ClickHouse, but for the sake of this driver, we treat these data types as
 * modifiers for the underlying base data types.
 */
@SuppressWarnings("squid:S115")
public enum ClickHouseDataType {
    interval_year(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_quarter(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_month(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_week(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_day(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_hour(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_minute(Long.class, false, true, true, 8, 19, 0, 0, 0),
    interval_second(Long.class, false, true, true, 8, 19, 0, 0, 0),
    uint8(Short.class, false, true, false, 1, 3, 0, 0, 0),
    uint16(Integer.class, false, true, false, 2, 5, 0, 0, 0),
    uint32(Long.class, false, true, false, 4, 10, 0, 0, 0),
    uint64(Long.class, false, true, false, 8, 20, 0, 0, 0),
    uint128(BigInteger.class, false, true, false, 16, 39, 0, 0, 0),
    uint256(BigInteger.class, false, true, false, 32, 78, 0, 0, 0),
    int8(Byte.class, false, true, true, 1, 3, 0, 0, 0, "byte"),
    int16(Short.class, false, true, true, 2, 5, 0, 0, 0, "smallint"),
    int32(Integer.class, false, true, true, 4, 10, 0, 0, 0, "integer"),
    int64(Long.class, false, true, true, 8, 19, 0, 0, 0, "bigint"),
    int128(BigInteger.class, false, true, true, 16, 39, 0, 0, 0),
    int256(BigInteger.class, false, true, true, 32, 77, 0, 0, 0),
    bool(Boolean.class, false, false, true, 1, 1, 0, 0, 0, "boolean"),
    date(LocalDate.class, false, false, false, 2, 10, 0, 0, 0),
    date32(LocalDate.class, false, false, false, 4, 10, 0, 0, 0),
    datetime(LocalDateTime.class, true, false, false, 0, 29, 0, 0, 9),
    datetime32(LocalDateTime.class, true, false, false, 4, 19, 0, 0, 0),
    datetime64(LocalDateTime.class, true, false, false, 8, 29, 3, 0, 9),
    decimal(BigDecimal.class, true, false, true, 0, 76, 0, 0, 76),
    decimal32(BigDecimal.class, true, false, true, 4, 9, 9, 0, 9),
    decimal64(BigDecimal.class, true, false, true, 8, 18, 18, 0, 18),
    decimal128(BigDecimal.class, true, false, true, 16, 38, 38, 0, 38),
    decimal256(BigDecimal.class, true, false, true, 32, 76, 20, 0, 76),
    uuid(UUID.class, false, true, false, 16, 69, 0, 0, 0),
    enum8(String.class, true, true, false, 1, 0, 0, 0, 0, "enum"),
    enum16(String.class, true, true, false, 2, 0, 0, 0, 0),
    float32(Float.class, false, true, true, 4, 12, 0, 0, 38, "float"),
    float64(Double.class, false, true, true, 16, 22, 0, 0, 308, "double"),
    ipv4(Inet4Address.class, false, true, false, 4, 10, 0, 0, 0),
    ipv6(Inet6Address.class, false, true, false, 16, 39, 0, 0, 0),
    fixed_string(String.class, true, true, false, 0, 0, 0, 0, 0),
    string(String.class, false, true, false, 0, 0, 0, 0, 0),
    aggregate_function(String.class, true, true, false, 0, 0, 0, 0, 0), // implementation-defined intermediate state
    simple_aggregate_function(String.class, true, true, false, 0, 0, 0, 0, 0),
    array(Object.class, true, true, false, 0, 0, 0, 0, 0),
    map(Map.class, true, true, false, 0, 0, 0, 0, 0),
    nested(Object.class, true, true, false, 0, 0, 0, 0, 0),
    tuple(List.class, true, true, false, 0, 0, 0, 0, 0),
    point(Object.class, false, true, true, 33, 0, 0, 0, 0), // same as Tuple(Float64, Float64)
    polygon(Object.class, false, true, true, 0, 0, 0, 0, 0), // same as Array(Ring)
    multi_polygon(Object.class, false, true, true, 0, 0, 0, 0, 0), // same as Array(Polygon)
    ring(Object.class, false, true, true, 0, 0, 0, 0, 0), // same as Array(Point)
    nothing(Object.class, false, true, false, 0, 0, 0, 0, 0);

    /**
     * Immutable set(sorted) for all aliases.
     */
    public static final Set<String> allAliases;

    /**
     * Immutable mapping between name and type.
     */
    public static final Map<String, ClickHouseDataType> name2type;

    static {
        Set<String> set = new TreeSet<>();
        Map<String, ClickHouseDataType> map = new HashMap<>();
        String errorMsg = "[%s] is used by type [%s]";
        ClickHouseDataType used = null;
        for (ClickHouseDataType t : ClickHouseDataType.values()) {
            String name = t.name();
            // if (!t.isCaseSensitive()) {
            //    name = name.toUpperCase();
            // }

            used = map.put(name, t);
            if (used != null) {
                throw new IllegalStateException(java.lang.String.format(Locale.ROOT, errorMsg, name, used.name()));
            }

            // aliases are all case-insensitive
            for (String alias : t.aliases) {
                String aliasInLowerCase = alias.toLowerCase();
                set.add(aliasInLowerCase);
                used = map.put(aliasInLowerCase, t);
                if (used != null) {
                    throw new IllegalStateException(java.lang.String.format(Locale.ROOT, errorMsg, alias, used.name()));
                }
            }
        }

        allAliases = Collections.unmodifiableSet(set);
        name2type = Collections.unmodifiableMap(map);
    }

    /**
     * Checks if the given type name is an alias or not.
     *
     * @param typeName type name
     * @return true if the type name is an alias; false otherwise
     */
    public static boolean isAlias(String typeName) {
        return typeName != null && !typeName.isEmpty() && allAliases.contains(typeName.trim().toLowerCase());
    }

    public static List<String> match(String part) {
        List<String> types = new LinkedList<>();

        for (ClickHouseDataType t : values()) {
            if (t.isCaseSensitive()) {
                if (t.name().equals(part)) {
                    types.add(t.name());
                    break;
                }
            } else {
                if (t.name().equalsIgnoreCase(part)) {
                    types.add(part);
                    break;
                }
            }
        }

        if (types.isEmpty()) {
            part = part.toLowerCase();
            String prefix = part + ' ';
            for (String alias : allAliases) {
                if (alias.length() == part.length() && alias.equals(part)) {
                    types.add(alias);
                } else if (alias.length() > part.length() && alias.startsWith(prefix)) {
                    types.add(alias);
                }
            }
        }

        return types;
    }

    /**
     * Converts given type name to corresponding data type.
     *
     * @param typeName non-empty type name
     * @return data type
     */
    public static ClickHouseDataType of(String typeName) {
        if (typeName == null || (typeName = typeName.trim()).isEmpty()) {
            throw new IllegalArgumentException("Non-empty typeName is required");
        }

        ClickHouseDataType type = name2type.get(typeName);
        if (type == null) {
            type = name2type.get(typeName.toLowerCase()); // case-insensitive or just an alias
        }

        if (type == null) {
            throw new IllegalArgumentException("Unknown data type: " + typeName);
        }
        return type;
    }

    /**
     * Converts given Java class to wrapper object(e.g. {@code int.class} to
     * {@code Integer.class}) if applicable.
     *
     * @param javaClass Java class
     * @return wrapper object
     */
    public static Class<?> toObjectType(Class<?> javaClass) {
        if (byte.class == javaClass || boolean.class == javaClass || Boolean.class == javaClass) {
            javaClass = Byte.class;
        } else if (short.class == javaClass) {
            javaClass = Short.class;
        } else if (int.class == javaClass || char.class == javaClass || Character.class == javaClass) {
            javaClass = Integer.class;
        } else if (long.class == javaClass) {
            javaClass = Long.class;
        } else if (float.class == javaClass) {
            javaClass = Float.class;
        } else if (double.class == javaClass) {
            javaClass = Double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    /**
     * Converts given Java class to primitive types(e.g. {@code Integer.class} to
     * {@code int.class}) if applicable.
     *
     * @param javaClass Java class
     * @return primitive type
     */
    public static Class<?> toPrimitiveType(Class<?> javaClass) {
        if (Byte.class == javaClass || Boolean.class == javaClass || boolean.class == javaClass) {
            javaClass = byte.class;
        } else if (Short.class == javaClass) {
            javaClass = short.class;
        } else if (Integer.class == javaClass || Character.class == javaClass || char.class == javaClass) {
            javaClass = int.class;
        } else if (Long.class == javaClass) {
            javaClass = long.class;
        } else if (Float.class == javaClass) {
            javaClass = float.class;
        } else if (Double.class == javaClass) {
            javaClass = double.class;
        } else if (javaClass == null) {
            javaClass = Object.class;
        }

        return javaClass;
    }

    private final Class<?> objectType;
    private final Class<?> primitiveType;
    private final boolean parameter;
    private final boolean caseSensitive;
    private final boolean signed;
    private final List<String> aliases;
    private final int byteLength;
    private final int maxPrecision;
    private final int defaultScale;
    private final int minScale;
    private final int maxScale;

    ClickHouseDataType(Class<?> javaClass, boolean parameter, boolean caseSensitive, boolean signed, int byteLength,
            int maxPrecision, int defaultScale, int minScale, int maxScale, String... aliases) {
        this.objectType = toObjectType(javaClass);
        this.primitiveType = toPrimitiveType(javaClass);
        this.parameter = parameter;
        this.caseSensitive = caseSensitive;
        this.signed = signed;
        this.byteLength = byteLength;
        this.maxPrecision = maxPrecision;
        this.defaultScale = defaultScale;
        this.minScale = minScale;
        this.maxScale = maxScale;
        if (aliases == null || aliases.length == 0) {
            this.aliases = Collections.emptyList();
        } else {
            this.aliases = Collections.unmodifiableList(Arrays.asList(aliases));
        }
    }

    /**
     * Gets Java class for this data type. Prefer wrapper objects to primitives(e.g.
     * {@code Integer.class} instead of {@code int.class}).
     *
     * @return Java class
     */
    public Class<?> getObjectClass() {
        return objectType;
    }

    /**
     * Gets Java class for this data type. Prefer primitives to wrapper objects(e.g.
     * {@code int.class} instead of {@code Integer.class}).
     *
     * @return Java class
     */
    public Class<?> getPrimitiveClass() {
        return primitiveType;
    }

    /**
     * Checks if this data type may have parameter(s).
     *
     * @return true if this data type may have parameter; false otherwise
     */
    public boolean hasParameter() {
        return parameter;
    }

    /**
     * Checks if name of this data type is case sensistive or not.
     *
     * @return true if it's case sensitive; false otherwise
     */
    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    /**
     * Checks if this data type could be a nested structure.
     *
     * @return true if it could be a nested structure; false otherwise
     */
    public boolean isNested() {
        return this == aggregate_function || this == array || this == map || this == nested || this == tuple;
    }

    /**
     * Checks if this data type represents signed number.
     *
     * @return true if it's signed; false otherwise
     */
    public boolean isSigned() {
        return signed;
    }

    /**
     * Gets immutable list of aliases for this data type.
     *
     * @return immutable list of aliases
     */
    public List<String> getAliases() {
        return aliases;
    }

    /**
     * Gets byte length of this data type. Zero means unlimited.
     *
     * @return byte length of this data type
     */
    public int getByteLength() {
        return byteLength;
    }

    /**
     * Gets maximum precision of this data type. Zero means unknown or not
     * supported.
     *
     * @return maximum precision of this data type.
     */
    public int getMaxPrecision() {
        return maxPrecision;
    }

    /**
     * Gets default scale of this data type. Zero means unknown or not supported.
     *
     * @return default scale of this data type.
     */
    public int getDefaultScale() {
        return defaultScale;
    }

    /**
     * Gets minimum scale of this data type. Zero means unknown or not supported.
     *
     * @return minimum scale of this data type.
     */
    public int getMinScale() {
        return minScale;
    }

    /**
     * Gets maximum scale of this data type. Zero means unknown or not supported.
     *
     * @return maximum scale of this data type.
     */
    public int getMaxScale() {
        return maxScale;
    }
}
