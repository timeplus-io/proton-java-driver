package com.timeplus.proton.examples.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Basic {
    static void dropAndCreateTable(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            String sql = String.format(
                    "drop stream if exists %1$s; create stream if not exists %1$s(a string, b nullable(string));",
                    table);
            stmt.execute(sql);
        }
    }

    static void batchInsert(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            // not that fast as it's based on string substitution and large sql statement
            String sql = String.format("insert into %1$s (* except _tp_time) values(?, ?)", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }

            // faster when inserting massive data
            sql = String.format("insert into %1$s (* except _tp_time)", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }

            // fastest approach as it does not need to issue additional query for metadata
            sql = String.format("insert into %1$s (a, b) select a, b from input('a string, b nullable(string)')", table);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, "a");
                ps.setString(2, "b");
                ps.addBatch();
                ps.setString(1, "c");
                ps.setString(2, null);
                ps.addBatch();
                ps.executeBatch();
            }
        }
    }

    static int query(String url, String user, String password, String table) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select * from table(" + table + ")")) {
            int count = 0;
            while (rs.next()) {
                System.out.println(rs.getString(1) + "," + rs.getString(2) + "|" + rs.getString(3));
                count++;
            }
            return count;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static void main(String[] args) {
        String url = String.format("jdbc:ch://%s:%d/default", System.getProperty("chHost", "localhost"),
                Integer.parseInt(System.getProperty("chPort", "3218")));
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");
        String table = "jdbc_example_basic";

        try {
            dropAndCreateTable(url, user, password, table);

            batchInsert(url, user, password, table);

            Thread.sleep(2000);
            System.out.println(query(url, user, password, table));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
