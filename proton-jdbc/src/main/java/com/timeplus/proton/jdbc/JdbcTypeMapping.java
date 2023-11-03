package com.timeplus.proton.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonDataType;

public final class JdbcTypeMapping {
    static Class<?> getCustomJavaClass(Map<String, Class<?>> typeMap, ProtonColumn column) {
        if (typeMap != null && !typeMap.isEmpty()) {
            Class<?> javaClass = typeMap.get(column.getOriginalTypeName());
            if (javaClass == null) {
                javaClass = typeMap.get(column.getDataType().name());
            }

            return javaClass;
        }

        return null;
    }

    /**
     * Gets corresponding JDBC type for the given Java class.
     *
     * @param javaClass non-null Java class
     * @return JDBC type
     */
    public static int toJdbcType(Class<?> javaClass) {
        int sqlType = Types.OTHER;
        if (javaClass == boolean.class || javaClass == Boolean.class) {
            sqlType = Types.BOOLEAN;
        } else if (javaClass == byte.class || javaClass == Byte.class) {
            sqlType = Types.TINYINT;
        } else if (javaClass == short.class || javaClass == Short.class) {
            sqlType = Types.SMALLINT;
        } else if (javaClass == int.class || javaClass == Integer.class) {
            sqlType = Types.INTEGER;
        } else if (javaClass == long.class || javaClass == Long.class) {
            sqlType = Types.BIGINT;
        } else if (javaClass == float.class || javaClass == Float.class) {
            sqlType = Types.FLOAT;
        } else if (javaClass == double.class || javaClass == Double.class) {
            sqlType = Types.DOUBLE;
        } else if (javaClass == BigInteger.class) {
            sqlType = Types.NUMERIC;
        } else if (javaClass == BigDecimal.class) {
            sqlType = Types.DECIMAL;
        } else if (javaClass == Date.class || javaClass == LocalDate.class) {
            sqlType = Types.DATE;
        } else if (javaClass == Time.class || javaClass == LocalTime.class) {
            sqlType = Types.TIME;
        } else if (javaClass == Timestamp.class || javaClass == LocalDateTime.class) {
            sqlType = Types.TIMESTAMP;
        } else if (javaClass == OffsetDateTime.class || javaClass == ZonedDateTime.class) {
            sqlType = Types.TIMESTAMP_WITH_TIMEZONE;
        } else if (javaClass == String.class || javaClass == byte[].class || Enum.class.isAssignableFrom(javaClass)) {
            sqlType = Types.VARCHAR;
        } else if (javaClass.isArray()) {
            sqlType = Types.ARRAY;
        }
        return sqlType;
    }

    /**
     * Gets corresponding JDBC type for the given column.
     *
     * @param typeMap type mappings, could be null
     * @param column  non-null column definition
     * @return JDBC type
     */
    public static int toJdbcType(Map<String, Class<?>> typeMap, ProtonColumn column) {
        Class<?> javaClass = getCustomJavaClass(typeMap, column);
        if (javaClass != null) {
            return toJdbcType(javaClass);
        }

        int sqlType = Types.OTHER;

        switch (column.getDataType()) {
            case bool:
                sqlType = Types.BOOLEAN;
                break;
            case int8:
                sqlType = Types.TINYINT;
                break;
            case uint8:
            case int16:
                sqlType = Types.SMALLINT;
                break;
            case uint16:
            case int32:
                sqlType = Types.INTEGER;
                break;
            case uint32:
            case interval_year:
            case interval_quarter:
            case interval_month:
            case interval_week:
            case interval_day:
            case interval_hour:
            case interval_minute:
            case interval_second:
            case int64:
                sqlType = Types.BIGINT;
                break;
            case uint64:
            case int128:
            case uint128:
            case int256:
            case uint256:
                sqlType = Types.NUMERIC;
                break;
            case float32:
                sqlType = Types.FLOAT;
                break;
            case float64:
                sqlType = Types.DOUBLE;
                break;
            case decimal:
            case decimal32:
            case decimal64:
            case decimal128:
            case decimal256:
                sqlType = Types.DECIMAL;
                break;
            case date:
            case date32:
                sqlType = Types.DATE;
                break;
            case datetime:
            case datetime32:
            case datetime64:
                sqlType = column.getTimeZone() != null ? Types.TIMESTAMP_WITH_TIMEZONE : Types.TIMESTAMP;
                break;
            case enum8:
            case enum16:
            case ipv4:
            case ipv6:
            case fixed_string:
            case string:
            case uuid:
                sqlType = Types.VARCHAR;
                break;
            case point:
            case ring:
            case polygon:
            case multi_polygon:
            case array:
                sqlType = Types.ARRAY;
                break;
            case tuple:
            case nested:
                sqlType = Types.STRUCT;
                break;
            case nothing:
                sqlType = Types.NULL;
                break;
            case map:
            default:
                break;
        }

        return sqlType;
    }

    /**
     * Gets Java class for the given column.
     *
     * @param typeMap type mappings, could be null
     * @param column  non-null column definition
     * @return Java class for the column
     */
    public static Class<?> toJavaClass(Map<String, Class<?>> typeMap, ProtonColumn column) {
        Class<?> clazz = getCustomJavaClass(typeMap, column);
        if (clazz != null) {
            return clazz;
        }

        ProtonDataType type = column.getDataType();
        switch (type) {
            case datetime:
            case datetime32:
            case datetime64:
                clazz = column.getTimeZone() != null ? OffsetDateTime.class : LocalDateTime.class;
                break;
            default:
                clazz = type.getObjectClass();
                break;
        }
        return clazz;
    }

    public static ProtonColumn fromJdbcType(int jdbcType, int scaleOrLength) {
        ProtonDataType dataType = fromJdbcType(jdbcType);
        ProtonColumn column = null;
        if (scaleOrLength > 0) {
            if (jdbcType == Types.NUMERIC || jdbcType == Types.DECIMAL) {
                for (ProtonDataType t : new ProtonDataType[] {}) {
                    if (scaleOrLength <= t.getMaxScale() / 2) {
                        column = ProtonColumn.of("", t, false, t.getMaxPrecision() - t.getMaxScale(),
                                scaleOrLength);
                        break;
                    }
                }
            } else if (dataType == ProtonDataType.date) {
                if (scaleOrLength > 2) {
                    dataType = ProtonDataType.date32;
                }
            } else if (dataType == ProtonDataType.datetime) {
                column = ProtonColumn.of("", ProtonDataType.datetime64, false, 0, scaleOrLength);
            } else if (dataType == ProtonDataType.string) {
                column = ProtonColumn.of("", ProtonDataType.fixed_string, false, scaleOrLength, 0);
            }
        }

        return column == null ? ProtonColumn.of("", dataType, false, false) : column;
    }

    public static ProtonDataType fromJdbcType(int jdbcType) {
        ProtonDataType dataType;

        switch (jdbcType) {
            case Types.BIT:
            case Types.BOOLEAN:
                dataType = ProtonDataType.uint8;
                break;
            case Types.TINYINT:
                dataType = ProtonDataType.int8;
                break;
            case Types.SMALLINT:
                dataType = ProtonDataType.int16;
                break;
            case Types.INTEGER:
                dataType = ProtonDataType.int32;
                break;
            case Types.BIGINT:
                dataType = ProtonDataType.int64;
                break;
            case Types.NUMERIC:
                dataType = ProtonDataType.int256;
                break;
            case Types.FLOAT:
            case Types.REAL:
                dataType = ProtonDataType.float32;
                break;
            case Types.DOUBLE:
                dataType = ProtonDataType.float64;
                break;
            case Types.DECIMAL:
                dataType = ProtonDataType.decimal;
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.CHAR:
            case Types.CLOB:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.SQLXML:
            case Types.VARBINARY:
            case Types.VARCHAR:
                dataType = ProtonDataType.string;
                break;
            case Types.DATE:
                dataType = ProtonDataType.date;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                dataType = ProtonDataType.datetime;
                break;
            case Types.ARRAY:
                dataType = ProtonDataType.array;
                break;
            case Types.STRUCT:
                dataType = ProtonDataType.nested;
                break;
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.ROWID:
            case Types.NULL:
            default:
                dataType = ProtonDataType.nothing;
                break;
        }
        return dataType;
    }

    private JdbcTypeMapping() {
    }
}
