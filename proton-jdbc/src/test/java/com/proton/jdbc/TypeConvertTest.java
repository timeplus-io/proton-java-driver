package com.proton.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TypeConvertTest {
    static class Pair<U, V> {
        public U first;
        public V second;

        public Pair(U first, V second) {
            this.first = first;
            this.second = second;
        }

        public static <U, V> TypeTest.Pair<U, V> of(U first, V second) {
            return new TypeTest.Pair<>(first, second);
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

    String insertPreparedSql(String stream, List<TypeTest.Pair<String, String>> struct) {
        String sql = "insert into " + stream + "(" +
                String.join(", ", struct.stream().map(st -> st.first).collect(Collectors.toList()))
                + ")";
        System.out.println(sql);
        return sql;
    }

    String querySql(String stream, List<TypeTest.Pair<String, String>> struct) {
        String sql = "select " +
                String.join(", ", struct.stream().map(st -> st.first).collect(Collectors.toList()))
                + " from table(" + stream + ")";
        System.out.println(sql);
        return sql;
    }

    void dropAndCreateStream(String stream, List<TypeTest.Pair<String, String>> struct) throws SQLException {
        List<String> tableStruct = new ArrayList<>();
        struct.forEach(pair -> tableStruct.add(String.format("%s %s", pair.first, pair.second)));
        execute(
                String.format("drop stream if exists %s", stream),
                String.format("create stream if not exists %s (%s)", stream, String.join(",", tableStruct))
        );
    }

    static final String STREAMNAME = "convert_test";

    @Test(groups = {"unit"})
    void IntegerConvert() throws SQLException {
        List<TypeTest.Pair<String, String>> struct = List.of(
                Pair.of("int16_col", "int16"),
                Pair.of("int128_col", "int128")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setInt(1, 32768);
            ps.setObject(2, new BigInteger("170141183460469231731687303715884105728"));
            ps.addBatch();
            ps.setInt(1, 65535);
            ps.setObject(2, new BigInteger("340282366920938463463374607431768211455"));
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getInt(1), -32768);
            Assert.assertEquals(rs.getObject(2), new BigInteger("-170141183460469231731687303715884105728"));
            rs.next();
            Assert.assertEquals(rs.getInt(1), -1);
            Assert.assertEquals(rs.getObject(2), new BigInteger("-1"));
        }
    }

    @Test(groups = {"unit"})
    void FloatConvert() throws SQLException {
        List<TypeTest.Pair<String, String>> struct = List.of(
                Pair.of("float_col", "float")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setDouble(1, 123456789012345E-17);
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertNotEquals(rs.getDouble(1), 123456789012345E-17);
            Assert.assertEquals(rs.getFloat(1), (float) 123456789012345E-17);
        }
    }

    @Test(groups = {"unit"})
    void DecimalConvert() throws SQLException {
        List<TypeTest.Pair<String, String>> struct = List.of(
                Pair.of("decimal_col", "decimal(19,15)")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setBigDecimal(1, new BigDecimal("1234567890.12345678901234567890"));
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getBigDecimal(1), new BigDecimal("1234567890.123456789012345"));
        }
    }

    @Test(groups = {"unit"})
    void UUIDConvert() throws SQLException {
        Assert.assertThrows(() -> {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("uuid_col", "uuid")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setString(1, "123456");
                ps.addBatch();
                ps.executeBatch();
            }
        });
    }

    @Test(groups = {"unit"})
    void StringConvert() throws SQLException {
        List<TypeTest.Pair<String, String>> struct = List.of(
                Pair.of("string_col", "string")
        );
        dropAndCreateStream(STREAMNAME, struct);
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setFloat(1, 3.14f);
            ps.addBatch();
            ps.setDate(1, Date.valueOf(LocalDate.of(2023, 1, 1)));
            ps.addBatch();
            ps.executeBatch();
        }
        wait(2);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
            rs.next();
            Assert.assertEquals(rs.getString(1), "1");
            rs.next();
            Assert.assertEquals(rs.getString(1), "3.14");
            rs.next();
            Assert.assertEquals(rs.getString(1), LocalDate.of(2023, 1, 1).toString());
        }
    }

    @Test(groups = {"unit"})
    void DateConvert() throws SQLException {
        Assert.assertThrows(() -> {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("date_col", "date")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setDate(1, Date.valueOf(LocalDate.of(2149, 6, 7)));
                ps.addBatch();
                ps.executeBatch();
            }
        });
        Assert.assertThrows(() -> {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("datetime_col", "datetime")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setTimestamp(2, Timestamp.valueOf(LocalDateTime.of(2106, 2, 7, 6, 28, 16)));
                ps.addBatch();
                ps.executeBatch();
            }
        });
        {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("datetime_col", "datetime")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.of(2023, 10, 18, 9, 59, 10, 123 * 1000 * 1000)));
                ps.addBatch();
                ps.executeBatch();
            }
            wait(2);
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
                rs.next();
                Assert.assertEquals(rs.getTimestamp(1), Timestamp.valueOf(LocalDateTime.of(2023, 10, 18, 9, 59, 10)));
            }
        }
    }

    @Test(groups = {"unit"})
    void ArrayConvert() throws SQLException {
        Assert.assertThrows(() -> {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("array_col", "array(int)")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setObject(1, List.of("1", 2, '3'));
                ps.addBatch();
                ps.executeBatch();
            }
        });
        {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("array_col", "array(string)")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setObject(1, new Object[]{"1", 2, "3"});
                ps.addBatch();
                ps.executeBatch();
            }
            wait(2);
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
                rs.next();
                Assert.assertEquals(rs.getObject(1), new Object[]{"1", "2", "3"});
            }
        }
    }

    @Test(groups = {"unit"})
    void TupleConvert() throws SQLException {
        {
            List<TypeTest.Pair<String, String>> struct = List.of(
                    Pair.of("tuple_col", "tuple(int,string)")
            );
            dropAndCreateStream(STREAMNAME, struct);
            try (Connection conn = getConnection();
                 PreparedStatement ps = conn.prepareStatement(insertPreparedSql(STREAMNAME, struct))) {
                ps.setObject(1, List.of(1, "2", 3));
                ps.addBatch();
                ps.executeBatch();
            }
            wait(2);
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(querySql(STREAMNAME, struct))) {
                rs.next();
                Assert.assertEquals(rs.getObject(1), List.of(1, "2"));
            }
        }
    }
}
