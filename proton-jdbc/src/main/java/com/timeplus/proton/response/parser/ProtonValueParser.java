package com.timeplus.proton.response.parser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.Function;

import com.timeplus.proton.domain.ProtonDataType;
import com.timeplus.proton.except.ProtonUnknownException;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;
import com.timeplus.proton.util.ProtonBitmap;

public abstract class ProtonValueParser<T> {

    static Map<Class<?>, ProtonValueParser<?>> parsers;

    static {
        parsers = new HashMap<>();
        register(Array.class, ProtonArrayParser.getInstance());
        register(BigDecimal.class, BigDecimal::new);
        register(BigInteger.class, BigInteger::new);
        register(Boolean.class,
            s -> Boolean.valueOf("1".equals(s) || Boolean.parseBoolean(s)),
            Boolean.FALSE);
        register(Date.class, ProtonSQLDateParser.getInstance());
        register(Double.class, ProtonDoubleParser.getInstance());
        register(Float.class, ProtonFloatParser.getInstance());
        register(Instant.class, ProtonInstantParser.getInstance());
        register(Integer.class, Integer::decode, Integer.valueOf(0));
        register(LocalDate.class, ProtonLocalDateParser.getInstance());
        register(LocalDateTime.class, ProtonLocalDateTimeParser.getInstance());
        register(LocalTime.class, ProtonLocalTimeParser.getInstance());
        register(Long.class, Long::decode, Long.valueOf(0L));
        register(ProtonBitmap.class, ProtonBitmapParser.getInstance());
        register(Map.class, ProtonMapParser.getInstance());
        register(Object.class, s -> s);
        register(OffsetDateTime.class, ProtonOffsetDateTimeParser.getInstance());
        register(OffsetTime.class, ProtonOffsetTimeParser.getInstance());
        register(Short.class, Short::decode, Short.valueOf((short) 0));
        register(String.class, ProtonStringParser.getInstance());
        register(Time.class, ProtonSQLTimeParser.getInstance());
        register(Timestamp.class, ProtonSQLTimestampParser.getInstance());
        register(UUID.class, UUID::fromString);
        register(ZonedDateTime.class, ProtonZonedDateTimeParser.getInstance());
    }

    private static final long MILLISECONDS_A_DAY = 24 * 3600 * 1000;

    public static long normalizeTime(ProtonColumnInfo info, long time) {
        if (info == null ||
            (info.getProtonDataType() != ProtonDataType.DateTime64 && info.getScale() == 0)) {
            time -= time % 1000; // FIXME fix this after switching to RowBinary format
        }
        return (time + MILLISECONDS_A_DAY) % MILLISECONDS_A_DAY;
    }

    private static <T> void register(Class<T> clazz, Function<String, T> parseFunction) {
        parsers.put(
            clazz,
            new ProtonValueParserFunctionWrapper<>(parseFunction, null, null, clazz));
    }

    private static <T> void register(Class<T> clazz, Function<String, T> parseFunction,
        T defaultValue)
    {
        parsers.put(clazz, new ProtonValueParserFunctionWrapper<>(
            parseFunction, defaultValue, null, clazz));
    }

    private static <T> void register(Class<T> clazz, Function<String, T> parseFunction,
        T defaultValue, T nanValue)
    {
        parsers.put(clazz, new ProtonValueParserFunctionWrapper<>(
            parseFunction, defaultValue, nanValue, clazz));
    }

    private static <T> void register(Class<T> clazz, ProtonValueParser<T> parser) {
        if (parsers.containsKey(clazz)) {
            throw new IllegalStateException(
                "duplicate parsers for class " + clazz.getName());
        }
        parsers.put(clazz, Objects.requireNonNull(parser));
    }

    @SuppressWarnings("unchecked")
    public static <T> ProtonValueParser<T> getParser(Class<T> clazz)
        throws SQLException
    {
        ProtonValueParser<T> p = (ProtonValueParser<T>) parsers.get(clazz);
        if (p == null) {
            throw new ProtonUnknownException(
                "No value parser for class '" + clazz.getName() + "'", null);
        }
        return p;
    }

    public static final int parseInt(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Integer i = getParser(Integer.class).parse(value, columnInfo, null);
        return i != null ? i.intValue() : 0;
    }

    public static final long parseLong(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Long l = getParser(Long.class).parse(value, columnInfo, null);
        return l != null ? l.longValue() : 0L;
    }

    public static final boolean parseBoolean(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Boolean b = getParser(Boolean.class).parse(value, columnInfo, null);
        return b != null ? b.booleanValue() : false;
    }

    public static final short parseShort(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Short s = getParser(Short.class).parse(value, columnInfo, null);
        return s != null ? s.shortValue() : 0;
    }

    public static final double parseDouble(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Double d = getParser(Double.class).parse(value, columnInfo, null);
        return d != null ? d.doubleValue() : 0.0;
    }

    public static final float parseFloat(ByteFragment value, ProtonColumnInfo columnInfo)
        throws SQLException
    {
        Float f = getParser(Float.class).parse(value, columnInfo, null);
        return f != null ? f.floatValue() : 0.0f;
    }

    /**
     * Parses the supplied byte fragment {@code value} using meta data contained
     * in {@code columnInfo}. Date / time parsing uses {@code resultTimeZone},
     * unless only local values are involved.
     *
     * @param value
     *            value as returned from the server
     * @param columnInfo
     *            meta data of the column
     * @param timeZone
     *            time zone to be used when parsing dates or times
     * @return the result of parsing {@code value} as an object of type
     *         {@code T}
     * @throws SQLException
     *             if the value cannot be parsed under the given circumstances
     */
    public abstract T parse(ByteFragment value, ProtonColumnInfo columnInfo,
        TimeZone timeZone) throws SQLException;

    /**
     * Parses the supplied byte fragment {@code value} using meta data contained
     * in {@code columnInfo}. Date / time parsing uses {@code resultTimeZone},
     * unless only local values are involved.
     * <p>
     * If the result would be null, this method will check if there is a default
     * value in place which should be returned instead. The default value
     * depends on the class. This method is intended to be used when parsing
     * numeric values which later need to be converted to primitive, e.g. int or
     * float.
     *
     * @param value
     *            value as returned from the server or a default value
     * @param columnInfo
     *            meta data of the column
     * @param resultTimeZone
     *            time zone to be used when parsing dates or times
     * @return the result of parsing {@code value} as an object of type
     *         {@code T}
     * @throws SQLException
     *             if the value cannot be parsed under the given circumstances
     */
    public T parseWithDefault(ByteFragment value, ProtonColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        T t = parse(value, columnInfo, resultTimeZone);
        return t == null ? getDefaultValue() : t;
    }

    protected T getDefaultValue() {
        return null;
    }

    private static final class ProtonValueParserFunctionWrapper<T>
        extends ProtonValueParser<T>
    {

        private final Function<String, T> f;
        private final T nanValue;
        private final T defaultValue;
        private final Class<T> clazz;

        private ProtonValueParserFunctionWrapper(Function<String, T> f,
            T defaultValue, T nanValue, Class<T> clazz)
        {
            this.f = Objects.requireNonNull(f);
            this.nanValue = nanValue;
            this.defaultValue = defaultValue;
            this.clazz = Objects.requireNonNull(clazz);
        }

        @Override
        public T parse(ByteFragment value, ProtonColumnInfo columnInfo,
            TimeZone resultTimeZone) throws SQLException
        {
            if (value.isNull() || value.isEmpty()) {
                return null;
            }
            if (nanValue != null && value.isNaN()) {
                return nanValue;
            }
            try {
                return f.apply(value.asString());
            } catch (Exception e) {
                throw new ProtonUnknownException(
                    "Error parsing '" + value.asString() + "' as " + clazz.getName(),
                    e);
            }
        }

        @Override
        protected T getDefaultValue() {
            return defaultValue;
        }

    }

}
