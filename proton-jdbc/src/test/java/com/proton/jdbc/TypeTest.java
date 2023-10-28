package com.proton.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class TypeTest {
    static class Pair<U, V> {
        public U first;
        public V second;

        public Pair(U first, V second) {
            this.first = first;
            this.second = second;
        }

        public static <U, V> Pair<U, V> of(U first, V second) {
            return new Pair<>(first, second);
        }
    }

    void wait(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Connection getConnection() throws SQLException {
        String url = String.format("jdbc:ch://%s:%d/default",
                System.getProperty("chHost", "localhost"),
                Integer.parseInt(System.getProperty("chPort", "3218")));
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        return DriverManager.getConnection(url, user, password);
    }

    void execute(String... sqls) throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            for (String sql : sqls) {
                stmt.execute(sql);
            }
        }
    }

    String insertPreparedSql(String stream, List<Pair<String, String>> struct) {
        String sql = "insert into " + stream + "(" +
                String.join(", ", struct.stream().map(st -> st.first).collect(Collectors.toList()))
                + ")";
        System.out.println(sql);
        return sql;
    }

    String querySql(String stream, List<Pair<String, String>> struct) {
        String sql = "select " +
                String.join(", ", struct.stream().map(st -> st.first).collect(Collectors.toList()))
                + " from table(" + stream + ")";
        System.out.println(sql);
        return sql;
    }

    void dropAndCreateStream(String stream, List<Pair<String, String>> struct) throws SQLException {
        List<String> tableStruct = new ArrayList<>();
        struct.forEach(pair -> tableStruct.add(String.format("%s %s", pair.first, pair.second)));
        execute(
                String.format("drop stream if exists %s", stream),
                String.format("create stream if not exists %s (%s)", stream, String.join(",", tableStruct))
        );
    }

    static final String STREAMNAME = "test";

    @Test(groups = {"unit"})
    void IntegerTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("int8_col", "int8"),
                Pair.of("int16_col", "int16"),
                Pair.of("int32_col", "int32"),
                Pair.of("int64_col", "int64"),
                Pair.of("int128_col", "int128"),
                Pair.of("int256_col", "int256"),
                Pair.of("byte_col", "byte"),
                Pair.of("smallint_col", "smallint"),
                Pair.of("int_col", "int"),
                Pair.of("bigint_col", "bigint")
        );
        dropAndCreateStream(STREAMNAME, struct);
        BigInteger INT128_MAX = BigInteger.TWO.pow(127).subtract(BigInteger.ONE);
        BigInteger INT256_MAX = BigInteger.TWO.pow(255).subtract(BigInteger.ONE);
        BigInteger INT128_MIN = BigInteger.TWO.pow(127).negate();
        BigInteger INT256_MIN = BigInteger.TWO.pow(255).negate();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setByte(1, Byte.MAX_VALUE);
            ps.setShort(2, Short.MAX_VALUE);
            ps.setInt(3, Integer.MAX_VALUE);
            ps.setLong(4, Long.MAX_VALUE);
            ps.setObject(5, INT128_MAX);
            ps.setObject(6, INT256_MAX);
            ps.setByte(7, Byte.MAX_VALUE);
            ps.setShort(8, Short.MAX_VALUE);
            ps.setInt(9, Integer.MAX_VALUE);
            ps.setLong(10, Long.MAX_VALUE);
            ps.addBatch();
            ps.setByte(1, Byte.MIN_VALUE);
            ps.setShort(2, Short.MIN_VALUE);
            ps.setInt(3, Integer.MIN_VALUE);
            ps.setLong(4, Long.MIN_VALUE);
            ps.setObject(5, INT128_MIN);
            ps.setObject(6, INT256_MIN);
            ps.setByte(7, Byte.MIN_VALUE);
            ps.setShort(8, Short.MIN_VALUE);
            ps.setInt(9, Integer.MIN_VALUE);
            ps.setLong(10, Long.MIN_VALUE);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getByte(1), Byte.MAX_VALUE);
            Assert.assertEquals(rs.getShort(2), Short.MAX_VALUE);
            Assert.assertEquals(rs.getInt(3), Integer.MAX_VALUE);
            Assert.assertEquals(rs.getLong(4), Long.MAX_VALUE);
            Assert.assertEquals(rs.getObject(5, BigInteger.class), INT128_MAX);
            Assert.assertEquals(rs.getObject(6, BigInteger.class), INT256_MAX);
            Assert.assertEquals(rs.getByte(7), Byte.MAX_VALUE);
            Assert.assertEquals(rs.getShort(8), Short.MAX_VALUE);
            Assert.assertEquals(rs.getInt(9), Integer.MAX_VALUE);
            Assert.assertEquals(rs.getLong(10), Long.MAX_VALUE);
            rs.next();
            Assert.assertEquals(rs.getByte(1), Byte.MIN_VALUE);
            Assert.assertEquals(rs.getShort(2), Short.MIN_VALUE);
            Assert.assertEquals(rs.getInt(3), Integer.MIN_VALUE);
            Assert.assertEquals(rs.getLong(4), Long.MIN_VALUE);
            Assert.assertEquals(rs.getObject(5, BigInteger.class), INT128_MIN);
            Assert.assertEquals(rs.getObject(6, BigInteger.class), INT256_MIN);
            Assert.assertEquals(rs.getByte(7), Byte.MIN_VALUE);
            Assert.assertEquals(rs.getShort(8), Short.MIN_VALUE);
            Assert.assertEquals(rs.getInt(9), Integer.MIN_VALUE);
            Assert.assertEquals(rs.getLong(10), Long.MIN_VALUE);
        }
    }

    @Test(groups = {"unit"})
    void DecimalTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("decimal_1_0", "decimal(1,0)"),
                Pair.of("decimal_1_1", "decimal(1,1)"),
                Pair.of("decimal_2_1", "decimal(2,1)"),
                Pair.of("decimal_5_2", "decimal(5,2)"),
                Pair.of("decimal_30_2", "decimal(30,15)")
        );
        dropAndCreateStream(STREAMNAME, struct);
        BigDecimal[] insertList1 = new BigDecimal[]{
                BigDecimal.valueOf(1, 0),
                BigDecimal.valueOf(1, 1),
                BigDecimal.valueOf(1, 1),
                BigDecimal.valueOf(1, 2),
                BigDecimal.valueOf(1, 15)
        };
        BigDecimal[] insertList2 = new BigDecimal[]{
                new BigDecimal("9"),
                new BigDecimal("0.9"),
                new BigDecimal("9.9"),
                new BigDecimal("999.99"),
                new BigDecimal("999999999999999.999999999999999")
        };
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            for (int i = 0; i < 5; i++) {
                ps.setBigDecimal(i + 1, insertList1[i]);
            }
            ps.addBatch();
            for (int i = 0; i < 5; i++) {
                ps.setBigDecimal(i + 1, insertList2[i]);
            }
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals(rs.getBigDecimal(i + 1), insertList1[i]);
            }
            rs.next();
            for (int i = 0; i < 5; i++) {
                Assert.assertEquals(rs.getBigDecimal(i + 1), insertList2[i]);
            }
        }
    }

    @Test(groups = {"unit"})
    void FloatTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("float_col", "float"),
                Pair.of("float32_col", "float32"),
                Pair.of("float64_col", "float64"),
                Pair.of("double_col", "double")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setFloat(1, Float.MIN_VALUE);
            ps.setFloat(2, Float.MIN_VALUE);
            ps.setDouble(3, Double.MIN_VALUE);
            ps.setDouble(4, Double.MIN_VALUE);
            ps.addBatch();
            ps.setFloat(1, Float.MAX_VALUE);
            ps.setFloat(2, Float.MAX_VALUE);
            ps.setDouble(3, Double.MAX_VALUE);
            ps.setDouble(4, Double.MAX_VALUE);
            ps.addBatch();
            ps.setFloat(1, Float.MIN_NORMAL);
            ps.setFloat(2, Float.MIN_NORMAL);
            ps.setDouble(3, Double.MIN_NORMAL);
            ps.setDouble(4, Double.MIN_NORMAL);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getFloat(1), Float.MIN_VALUE);
            Assert.assertEquals(rs.getFloat(2), Float.MIN_VALUE);
            Assert.assertEquals(rs.getDouble(3), Double.MIN_VALUE);
            Assert.assertEquals(rs.getDouble(4), Double.MIN_VALUE);
            rs.next();
            Assert.assertEquals(rs.getFloat(1), Float.MAX_VALUE);
            Assert.assertEquals(rs.getFloat(2), Float.MAX_VALUE);
            Assert.assertEquals(rs.getDouble(3), Double.MAX_VALUE);
            Assert.assertEquals(rs.getDouble(4), Double.MAX_VALUE);
            rs.next();
            Assert.assertEquals(rs.getFloat(1), Float.MIN_NORMAL);
            Assert.assertEquals(rs.getFloat(2), Float.MIN_NORMAL);
            Assert.assertEquals(rs.getDouble(3), Double.MIN_NORMAL);
            Assert.assertEquals(rs.getDouble(4), Double.MIN_NORMAL);
        }
    }

    @Test(groups = {"unit"})
    void BooleanTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("bool_col", "bool")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setBoolean(1, true);
            ps.addBatch();
            ps.setBoolean(1, false);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertTrue(rs.getBoolean(1));
            rs.next();
            Assert.assertFalse(rs.getBoolean(1));
        }
    }

    @Test(groups = {"unit"})
    void StringTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("string_col", "string")
        );
        dropAndCreateStream(STREAMNAME, struct);
        List<String> testList = List.of("",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890` ~-_=+[]{}\\|;:'\",./<>?",
                "你好，世界。");
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            for (String str : testList) {
                ps.setString(1, str);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            for (String str : testList) {
                rs.next();
                Assert.assertEquals(rs.getString(1), str);
            }
        }
    }

    @Test(groups = {"unit"})
    void FixedStringTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("fixed_string_col", "fixed_string(10)")
        );
        dropAndCreateStream(STREAMNAME, struct);
        List<String> testList = List.of(
                "abcdefghij",
                "ABCDEFGHIJ",
                "ABCDE",
                ""
        );
        List<String> testListFail = List.of(
                "abcdefghijkaaa"
        );
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            for (String str : testList) {
                ps.setString(1, str);
                ps.addBatch();
            }
            ps.executeBatch();
        }
        for (String str : testListFail) {
            Assert.assertThrows(() -> {
                try (Connection conn = getConnection();
                     PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                    ps.setString(1, str);
                    ps.addBatch();
                    ps.executeBatch();
                }
            });
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            for (String str : testList) {
                rs.next();
                Assert.assertEquals(rs.getString(1), str);
            }
        }
    }

    @Test(groups = {"unit"})
    void UUIDTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("uuid_col", "uuid")
        );
        dropAndCreateStream(STREAMNAME, struct);
        UUID uuid = UUID.randomUUID();
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, uuid);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), uuid);
        }
    }

    @Test(groups = {"unit"})
    void Ipv4TypeTest() throws SQLException, UnknownHostException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("ipv4_col", "ipv4")
        );
        dropAndCreateStream(STREAMNAME, struct);
        Inet4Address ipv4 = (Inet4Address) Inet4Address.getByName("116.253.40.133");
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, ipv4);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), ipv4);
        }
    }

    @Test(groups = {"unit"})
    void Ipv6TypeTest() throws SQLException, UnknownHostException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("ipv6_col", "ipv6")
        );
        dropAndCreateStream(STREAMNAME, struct);
        Inet6Address ipv6 = (Inet6Address) Inet6Address.getByName("2a02:aa08:e000:3100::2");
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, ipv6);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), ipv6);
        }
    }

    @Test(groups = {"unit"})
    void DateTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("date_col", "date"),
                Pair.of("datetime_col", "datetime"),
                Pair.of("datetime64_col", "datetime64")
        );
        {
            dropAndCreateStream(STREAMNAME, struct);
            Date date = Date.valueOf("2023-10-17");
            Timestamp datetime = Timestamp.valueOf("2023-10-17 11:09:01");
            Timestamp datetime64 = Timestamp.valueOf("2023-10-17 11:09:01.123");
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setDate(1, date);
                ps.setTimestamp(2, datetime);
                ps.setTimestamp(3, datetime64);
                ps.addBatch();
                ps.executeBatch();
            }
            wait(2);
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
                rs.next();
                Assert.assertEquals(rs.getDate(1), date);
                Assert.assertEquals(rs.getTimestamp(2), datetime);
                Assert.assertEquals(rs.getTimestamp(3), datetime64);
            }
        }
        {
            dropAndCreateStream(STREAMNAME, struct);
            LocalDate date = LocalDate.of(2023, 10, 17);
            LocalDateTime datetime = LocalDateTime.of(2023, 10, 17, 11, 11, 11);
            LocalDateTime datetime64 = LocalDateTime.of(2023, 10, 17, 11, 11, 11, 123 * 1000 * 1000);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setObject(1, date);
                ps.setObject(2, datetime);
                ps.setObject(3, datetime64);
                ps.addBatch();
                ps.executeBatch();
            }
            wait(2);
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
                rs.next();
                Assert.assertEquals(rs.getObject(1), date);
                Assert.assertEquals(rs.getObject(2), datetime);
                Assert.assertEquals(rs.getObject(3), datetime64);
            }
        }
    }

    @Test(groups = {"unit"})
    void ArrayTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("array_int_col", "array(int)"),
                Pair.of("array_string_col", "array(string)"),
                Pair.of("array_fixed_string_col", "array(fixed_string(3))"),
                Pair.of("array_array_col", "array(array(int))")
        );
        dropAndCreateStream(STREAMNAME, struct);
        Assert.assertThrows(() -> {
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setObject(1, new int[]{});
                ps.setObject(2, new String[]{});
                ps.setObject(3, new String[]{"1000", "2000", "3000"});
                ps.setObject(4, new int[][]{});
                ps.addBatch();
                ps.executeBatch();
            }
        });
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, new int[]{1, 2, 3});
            ps.setObject(2, new String[]{"1", "2", "3"});
            ps.setObject(3, new String[]{"1", "2", "3"});
            ps.setObject(4, new int[][]{new int[]{1}, new int[]{}, new int[]{2, 3}});
            ps.addBatch();
            ps.setObject(1, List.of(4, 5, 6));
            ps.setObject(2, List.of("4", "5", "6"));
            ps.setObject(3, List.of("4", "5", "6"));
            ps.setObject(4, List.of(List.of(4), List.of(), List.of(5, 6)));
            ps.addBatch();
            ps.setObject(1, new int[]{});
            ps.setObject(2, new String[]{});
            ps.setObject(3, new String[]{});
            ps.setObject(4, new int[][]{});
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), new int[]{1, 2, 3});
            Assert.assertEquals(rs.getObject(2), new String[]{"1", "2", "3"});
            Assert.assertEquals(rs.getObject(3), new String[]{"1", "2", "3"});
            Assert.assertEquals(rs.getObject(4), new int[][]{new int[]{1}, new int[]{}, new int[]{2, 3}});
            rs.next();
            Assert.assertEquals(rs.getObject(1), new int[]{4, 5, 6});
            Assert.assertEquals(rs.getObject(2), new String[]{"4", "5", "6"});
            Assert.assertEquals(rs.getObject(3), new String[]{"4", "5", "6"});
            Assert.assertEquals(rs.getObject(4), new int[][]{new int[]{4}, new int[]{}, new int[]{5, 6}});
            rs.next();
            Assert.assertEquals(rs.getObject(1), new int[]{});
            Assert.assertEquals(rs.getObject(2), new String[]{});
            Assert.assertEquals(rs.getObject(3), new String[]{});
            Assert.assertEquals(rs.getObject(4), new int[][]{});
        }
    }

    @Test(groups = {"unit"})
    void MapTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("map_int_string_col", "map(int,string)"),
                Pair.of("map_string_int_col", "map(string,int)"),
                Pair.of("map_string_map_int_int_col", "map(string,map(int,int))")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, Map.of(1, "a", 0, "b"));
            ps.setObject(2, Map.of("a", 1, "b", 0));
            ps.setObject(3, Map.of("a", Map.of(1, 2, 3, 4), "b", Map.of(5, 6, 7, 8)));
            ps.addBatch();
            ps.setObject(1, Map.of());
            ps.setObject(2, Map.of());
            ps.setObject(3, Map.of());
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), Map.of(1, "a", 0, "b"));
            Assert.assertEquals(rs.getObject(2), Map.of("a", 1, "b", 0));
            Assert.assertEquals(rs.getObject(3), Map.of("a", Map.of(1, 2, 3, 4), "b", Map.of(5, 6, 7, 8)));
            rs.next();
            Assert.assertEquals(rs.getObject(1), Map.of());
            Assert.assertEquals(rs.getObject(2), Map.of());
            Assert.assertEquals(rs.getObject(3), Map.of());
        }
    }

    @Test(groups = {"unit"})
    void TupleTypeTest() throws SQLException {
        List<Pair<String, String>> struct = List.of(
                Pair.of("tuple_col", "tuple(int,string,date,bool)"),
                Pair.of("tuple_tuple_col", "tuple(int,tuple(string,date,bool))")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setObject(1, List.of(1, "2", LocalDate.of(2023, 10, 17), true));
            ps.setObject(2, List.of(1, List.of("2", LocalDate.of(2023, 10, 17), true)));
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getObject(1), List.of(1, "2", LocalDate.of(2023, 10, 17), true));
            Assert.assertEquals(rs.getObject(2), List.of(1, List.of("2", LocalDate.of(2023, 10, 17), true)));
        }
    }
}
