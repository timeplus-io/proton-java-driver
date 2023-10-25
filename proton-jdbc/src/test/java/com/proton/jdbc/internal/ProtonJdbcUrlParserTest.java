package com.proton.jdbc.internal;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Properties;

import com.proton.client.ProtonCredentials;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonProtocol;
import com.proton.client.config.ProtonDefaults;
import com.proton.jdbc.internal.ProtonJdbcUrlParser.ConnectionInfo;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonJdbcUrlParserTest {
    @Test(groups = "unit")
    public void testRemoveCredentialsFromQuery() {
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery(null), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery(""), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery(" "), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("&"), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery(" & "), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("a=1&b=2"), "a=1&b=2");
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("user=a"), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("password=a%20b"), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("user=default&password=a%20b"), null);
        Assert.assertEquals(ProtonJdbcUrlParser.removeCredentialsFromQuery("user=default&a=1&password=a%20b"),
                "a=1");
    }

    @Test(groups = "unit")
    public void testParseInvalidUri() {
        Assert.assertThrows(SQLException.class, () -> ProtonJdbcUrlParser.parse(null, null));
        Assert.assertThrows(SQLException.class, () -> ProtonJdbcUrlParser.parse("", null));
        Assert.assertThrows(SQLException.class, () -> ProtonJdbcUrlParser.parse("some_invalid_uri", null));
        Assert.assertThrows(SQLException.class, () -> ProtonJdbcUrlParser.parse("jdbc:proton:.", null));
        Assert.assertThrows(SQLException.class, () -> ProtonJdbcUrlParser.parse("jdbc:proton://", null));
        Assert.assertThrows(SQLException.class,
                () -> ProtonJdbcUrlParser.parse("jdbc:proton:///db", null));
        Assert.assertThrows(SQLException.class,
                () -> ProtonJdbcUrlParser.parse("jdbc:proton://server/ ", null));
        Assert.assertThrows(SQLException.class,
                () -> ProtonJdbcUrlParser.parse("proton://a:b:c@aaa", null));
        Assert.assertThrows(SQLException.class,
                () -> ProtonJdbcUrlParser.parse("proton://::1:1234/a", null));
    }

    @Test(groups = "unit")
    public void testParseIpv6() throws SQLException, URISyntaxException {
        ConnectionInfo info = ProtonJdbcUrlParser.parse("jdbc:proton://[::1]:1234", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://[::1]:1234/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("[::1]").port(ProtonProtocol.HTTP, 1234)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());
    }

    @Test(groups = "unit")
    public void testParseAbbrevation() throws SQLException, URISyntaxException {
        ConnectionInfo info = ProtonJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.HTTP)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ProtonJdbcUrlParser.parse("jdbc:ch:grpc://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:grpc://localhost:9100/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.GRPC)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ProtonJdbcUrlParser.parse("jdbc:ch:https://:letmein@[::1]:3218/db1?user=aaa", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://[::1]:3218/db1"));
        Assert.assertEquals(info.getServer(), ProtonNode.builder().host("[::1]").port(ProtonProtocol.HTTP, 3218)
                .database("db1").credentials(ProtonCredentials.fromUserAndPassword("aaa", "letmein")).build());
        Assert.assertEquals(info.getProperties().getProperty("user"), "aaa");
    }

    @Test(groups = "unit")
    public void testParse() throws SQLException, URISyntaxException {
        ConnectionInfo info = ProtonJdbcUrlParser.parse("jdbc:ch://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.HTTP)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ProtonJdbcUrlParser.parse("jdbc:ch:grpc://localhost", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:grpc://localhost:9100/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.GRPC)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ProtonJdbcUrlParser.parse("jdbc:ch:https://:letmein@127.0.0.1:3218/db1", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://127.0.0.1:3218/db1"));
        Assert.assertEquals(info.getServer(), ProtonNode.builder().host("127.0.0.1")
                .port(ProtonProtocol.HTTP, 3218).database("db1")
                .credentials(ProtonCredentials
                        .fromUserAndPassword((String) ProtonDefaults.USER.getEffectiveDefaultValue(), "letmein"))
                .build());
    }

    @Test(groups = "unit")
    public void testParseWithProperties() throws SQLException, URISyntaxException {
        ConnectionInfo info = ProtonJdbcUrlParser.parse("jdbc:proton://localhost/", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://localhost:8123/default"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.HTTP)
                        .database((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue())
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        info = ProtonJdbcUrlParser.parse("jdbc:proton://localhost:4321/ndb", null);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://localhost:4321/ndb"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.HTTP, 4321).database("ndb")
                        .credentials(ProtonCredentials.fromUserAndPassword(
                                (String) ProtonDefaults.USER.getEffectiveDefaultValue(),
                                (String) ProtonDefaults.PASSWORD.getEffectiveDefaultValue()))
                        .build());

        Properties props = new Properties();
        props.setProperty("database", "db1");
        info = ProtonJdbcUrlParser.parse("jdbc:proton://me@localhost:1234/mydb?password=123", props);
        Assert.assertEquals(info.getUri(), new URI("jdbc:proton:http://localhost:1234/db1"));
        Assert.assertEquals(info.getServer(),
                ProtonNode.builder().host("localhost").port(ProtonProtocol.HTTP, 1234).database("db1")
                        .credentials(ProtonCredentials.fromUserAndPassword("me", "123")).build());
        Assert.assertEquals(info.getProperties().getProperty("database"), "db1");
    }

    @Test(groups = "unit")
    public void testParseCredentials() throws Exception {
        Properties props = new Properties();
        props.setProperty("user", "default1");
        props.setProperty("password", "password1");
        ProtonNode server = ProtonJdbcUrlParser.parse("jdbc:proton://user:a:passwd@foo.ch/test", props)
                .getServer();
        Assert.assertEquals(server.getCredentials().get().getUserName(), "default1");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "password1");

        server = ProtonJdbcUrlParser.parse("jdbc:proton://let%40me%3Ain:let%40me%3Ain@foo.ch", null)
                .getServer();
        Assert.assertEquals(server.getCredentials().get().getUserName(), "let@me:in");
        Assert.assertEquals(server.getCredentials().get().getPassword(), "let@me:in");
    }
}
