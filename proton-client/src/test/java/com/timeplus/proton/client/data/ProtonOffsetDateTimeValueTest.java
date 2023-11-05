package com.timeplus.proton.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.UUID;

import com.timeplus.proton.client.BaseProtonValueTest;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.timeplus.proton.client.ProtonDataType;
import com.timeplus.proton.client.ProtonValues;

public class ProtonOffsetDateTimeValueTest extends BaseProtonValueTest {
    @Test(groups = { "unit" })
    public void testUpdate() {
        Assert.assertEquals(ProtonOffsetDateTimeValue.ofNull(0, null).update(-1L).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));
        Assert.assertEquals(ProtonOffsetDateTimeValue.ofNull(0, null).update(-1.1F).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));
        Assert.assertEquals(ProtonOffsetDateTimeValue.ofNull(3, null).update(-1L).getValue(),
                LocalDateTime.ofEpochSecond(-1L, 999000000, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));
        Assert.assertEquals(ProtonOffsetDateTimeValue.ofNull(3, null).update(-1.1F).getValue(),
                LocalDateTime.ofEpochSecond(-2L, 900000000, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));

        Assert.assertEquals(
                ProtonOffsetDateTimeValue.ofNull(9, null).update(new BigDecimal(BigInteger.ONE, 9)).getValue(),
                LocalDateTime.ofEpochSecond(0L, 1, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));
        Assert.assertEquals(
                ProtonOffsetDateTimeValue.ofNull(9, null).update(new BigDecimal(BigInteger.valueOf(-1L), 9))
                        .getValue(),
                LocalDateTime.ofEpochSecond(-1L, 999999999, ZoneOffset.UTC).atOffset(ZoneOffset.UTC));
    }

    @Test(groups = { "unit" })
    public void testValueWithoutScale() throws Exception {
        // null value
        checkNull(ProtonOffsetDateTimeValue.ofNull(0, null));
        checkNull(ProtonOffsetDateTimeValue.of(LocalDateTime.now(), 0, null).resetToNullOrEmpty());

        // non-null
        checkValue(ProtonOffsetDateTimeValue.of(LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), 0, null), false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0F, // float
                0D, // double
                BigDecimal.valueOf(0L), // BigDecimal
                new BigDecimal(BigInteger.ZERO, 3), // BigDecimal
                BigInteger.ZERO, // BigInteger
                ProtonDataType.values()[0].name(), // Enum<ProtonDataType>
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "1970-01-01 00:00:00", // String
                "'1970-01-01 00:00:00'", // SQL Expression
                LocalTime.ofSecondOfDay(0L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                OffsetDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // Array
                new OffsetDateTime[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // typed
                // Array
                buildMap(new Object[] { 1 },
                        new Object[] { LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new OffsetDateTime[] {
                                LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // typed
                // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(0L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)) // Tuple
        );
        checkValue(ProtonOffsetDateTimeValue.of(LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), 0, null), false, // isInfinity
                false, // isNan
                false, // isNull
                true, // boolean
                (byte) 1, // byte
                (short) 1, // short
                1, // int
                1L, // long
                1F, // float
                1D, // double
                BigDecimal.valueOf(1L), // BigDecimal
                new BigDecimal(BigInteger.ONE, 3), // BigDecimal
                BigInteger.ONE, // BigInteger
                ProtonDataType.values()[1].name(), // Enum<ProtonDataType>
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.1")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:1")[0], // Inet6Address
                "1970-01-01 00:00:01", // String
                "'1970-01-01 00:00:01'", // SQL Expression
                LocalTime.ofSecondOfDay(1L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000001"), // UUID
                Object.class, // Key class
                OffsetDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // Array
                new OffsetDateTime[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // typed
                // Array
                buildMap(new Object[] { 1 },
                        new Object[] { LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new OffsetDateTime[] {
                                LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // typed
                // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(1L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)) // Tuple
        );
        checkValue(ProtonOffsetDateTimeValue.of(LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), 0, null), false, // isInfinity
                false, // isNan
                false, // isNull
                IllegalArgumentException.class, // boolean
                (byte) 2, // byte
                (short) 2, // short
                2, // int
                2L, // long
                2F, // float
                2D, // double
                BigDecimal.valueOf(2L), // BigDecimal
                new BigDecimal(BigInteger.valueOf(2L), 3), // BigDecimal
                BigInteger.valueOf(2L), // BigInteger
                ProtonDataType.values()[2].name(), // Enum<ProtonDataType>
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC), // Object
                LocalDate.ofEpochDay(0L), // Date
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime
                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.2")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:2")[0], // Inet6Address
                "1970-01-01 00:00:02", // String
                "'1970-01-01 00:00:02'", // SQL Expression
                LocalTime.ofSecondOfDay(2L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000002"), // UUID
                Object.class, // Key class
                OffsetDateTime.class, // Value class
                new Object[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // Array
                new OffsetDateTime[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }, // typed
                // Array
                buildMap(new Object[] { 1 },
                        new Object[] { LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // Map
                buildMap(new Object[] { 1 },
                        new OffsetDateTime[] {
                                LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC) }), // typed
                // Map
                Arrays.asList(LocalDateTime.ofEpochSecond(2L, 0, ZoneOffset.UTC).atOffset(ZoneOffset.UTC)) // Tuple
        );
    }

    @Test(groups = { "unit" })
    public void testValueWithScale() throws Exception {
        // null value
        checkNull(ProtonOffsetDateTimeValue.ofNull(3, null));
        checkNull(ProtonOffsetDateTimeValue.of(LocalDateTime.now(), 9, null).resetToNullOrEmpty());

        // non-null
        OffsetDateTime dateTime = LocalDateTime.ofEpochSecond(0L, 123456789, ZoneOffset.UTC).atOffset(ZoneOffset.UTC);
        checkValue(ProtonOffsetDateTimeValue.of(dateTime.toLocalDateTime(), 3, null), false, // isInfinity
                false, // isNan
                false, // isNull
                false, // boolean
                (byte) 0, // byte
                (short) 0, // short
                0, // int
                0L, // long
                0.123456789F, // float
                0.123456789D, // double
                BigDecimal.valueOf(0L), // BigDecimal
                BigDecimal.valueOf(0.123D), // BigDecimal
                BigInteger.ZERO, // BigInteger
                ProtonDataType.values()[0].name(), // Enum<ProtonDataType>
                dateTime, // Object
                LocalDate.ofEpochDay(0L), // Date
                dateTime.toLocalDateTime(), // DateTime
                dateTime.toLocalDateTime(), // DateTime(9)
                Inet4Address.getAllByName("0.0.0.0")[0], // Inet4Address
                Inet6Address.getAllByName("0:0:0:0:0:0:0:0")[0], // Inet6Address
                "1970-01-01 00:00:00.123456789", // String
                "'1970-01-01 00:00:00.123456789'", // SQL Expression
                LocalTime.ofNanoOfDay(123456789L), // Time
                UUID.fromString("00000000-0000-0000-0000-000000000000"), // UUID
                Object.class, // Key class
                OffsetDateTime.class, // Value class
                new Object[] { dateTime }, // Array
                new OffsetDateTime[] { dateTime }, // typed Array
                buildMap(new Object[] { 1 }, new Object[] { dateTime }), // Map
                buildMap(new Object[] { 1 }, new OffsetDateTime[] { dateTime }), // typed
                                                                                 // Map
                Arrays.asList(dateTime) // Tuple
        );
        Assert.assertEquals(ProtonOffsetDateTimeValue.of(dateTime.toLocalDateTime(), 3, null).asBigDecimal(4),
                BigDecimal.valueOf(0.1235D));
    }

    @Test(groups = { "unit" })
    public void testValueWithTimeZone() throws Exception {
        LocalDateTime dateTime = LocalDateTime.of(2020, 2, 11, 0, 23, 33);
        TimeZone tz = null;
        ProtonOffsetDateTimeValue v = ProtonOffsetDateTimeValue.of(dateTime, 0, tz);
        Assert.assertEquals(v.asDateTime(0), dateTime);
        Assert.assertEquals(v.asOffsetDateTime(0), OffsetDateTime.of(dateTime, ZoneOffset.UTC));
        Assert.assertEquals(ProtonValues.convertToSqlExpression(v.asOffsetDateTime()), v.toSqlExpression());
        v = ProtonOffsetDateTimeValue.of(dateTime, 0, tz = TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(v.asDateTime(0), dateTime);
        Assert.assertEquals(v.asOffsetDateTime(0), ZonedDateTime.of(dateTime, tz.toZoneId()).toOffsetDateTime());
        Assert.assertEquals(ProtonValues.convertToSqlExpression(v.asOffsetDateTime()), v.toSqlExpression());
        v = ProtonOffsetDateTimeValue.of(dateTime, 0, tz = TimeZone.getTimeZone("Asia/Shanghai"));
        Assert.assertEquals(v.asDateTime(0), dateTime);
        Assert.assertEquals(v.asOffsetDateTime(0), ZonedDateTime.of(dateTime, tz.toZoneId()).toOffsetDateTime());
        Assert.assertEquals(ProtonValues.convertToSqlExpression(v.asOffsetDateTime()), v.toSqlExpression());
        v = ProtonOffsetDateTimeValue.of(dateTime, 0, tz = TimeZone.getTimeZone("America/Los_Angeles"));
        Assert.assertEquals(v.asDateTime(0), dateTime);
        Assert.assertEquals(v.asOffsetDateTime(0), ZonedDateTime.of(dateTime, tz.toZoneId()).toOffsetDateTime());
        Assert.assertEquals(ProtonValues.convertToSqlExpression(v.asOffsetDateTime()), v.toSqlExpression());
    }
}
