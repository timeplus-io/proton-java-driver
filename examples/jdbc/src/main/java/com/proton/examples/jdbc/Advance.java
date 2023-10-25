package com.proton.examples.jdbc;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Properties;

import com.proton.client.ProtonFormat;
import com.proton.client.data.ProtonExternalTable;

public class Advance {
    static String exteralTables(String url, String user, String password) throws SQLException {
        String sql = "select a.name as n1, b.name as n2 from {tt 'table1'} as a inner join {tt 'table2'} as b on a.id=b.id";
        try (Connection conn = DriverManager.getConnection(url, user, password);
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1,
                    ProtonExternalTable.builder().name("table1").columns("id int32, name nullable(string)")
                            .format(ProtonFormat.CSV)
                            .content(new ByteArrayInputStream("1,a\n2,b".getBytes(StandardCharsets.US_ASCII))).build());
            ps.setObject(2,
                    ProtonExternalTable.builder().name("table2").columns("id int32, name string")
                            .format(ProtonFormat.JSONEachRow)
                            .content(new ByteArrayInputStream("{\"id\":3,\"name\":\"c\"}\n{\"id\":1,\"name\":\"d\"}"
                                    .getBytes(StandardCharsets.US_ASCII)))
                            .build());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Should have at least one record");
                }

                // n1=a, n2=d
                return String.format("n1=%s, n2=%s", rs.getString(1), rs.getString(2));
            }
        }
    }

    static String namedParameter(String url, String user, String password) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        props.setProperty("namedParameter", "true");
        // two parameters:
        // * a - String
        // * b - DateTime64(3)
        String sql = "select :a as a1, :a(string) as a2, :b(datetime64(3)) as b";
        try (Connection conn = DriverManager.getConnection(url, props);
                PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, "a");
            ps.setObject(2, LocalDateTime.of(2022, 1, 7, 22, 48, 17, 123000000));

            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new IllegalStateException("Should have at least one record");
                }
                // a1=a, a2=a, b=2022-01-07 22:48:17.123
                return String.format("a1=%s, a2=%s, b=%s", rs.getString(1), rs.getString(2), rs.getString("B"));
            }
        }
    }

    public static void main(String[] args) {
        String url = String.format("jdbc:ch://%s:%d/default", System.getProperty("chHost", "localhost"),
                Integer.parseInt(System.getProperty("chPort", "3218")));
        String user = System.getProperty("chUser", "default");
        String password = System.getProperty("chPassword", "");

        try {
            System.out.println(exteralTables(url, user, password));
            System.out.println(namedParameter(url, user, password));
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
