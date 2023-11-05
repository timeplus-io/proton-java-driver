package com.timeplus.proton.client;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import com.timeplus.proton.client.config.ProtonClientOption;
import com.timeplus.proton.client.config.ProtonDefaults;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.timeplus.proton.client.data.ProtonBigIntegerValue;
import com.timeplus.proton.client.data.ProtonByteValue;
import com.timeplus.proton.client.data.ProtonDateTimeValue;
import com.timeplus.proton.client.data.ProtonExternalTable;
import com.timeplus.proton.client.data.ProtonFloatValue;
import com.timeplus.proton.client.data.ProtonIntegerValue;
import com.timeplus.proton.client.data.ProtonStringValue;

public class ProtonRequestTest {
    @Test(groups = { "unit" })
    public void testBuild() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        Assert.assertNotNull(request);

        ProtonConfig config = request.getConfig();
        List<String> stmts = request.getStatements();
        Assert.assertEquals(config, request.getConfig());
        Assert.assertEquals(stmts, request.getStatements());
        Assert.assertEquals(stmts.size(), 0);

        String db = "db";
        String table = "test";
        String sql = "select 1";

        request.table(table);
        Assert.assertEquals(config, request.getConfig());
        Assert.assertNotEquals(stmts, request.getStatements());
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), "SELECT * FROM " + table);

        request.query(sql);
        Assert.assertEquals(config, request.getConfig());
        Assert.assertEquals(request.getStatements().get(0), sql);

        request.use(db);
        Assert.assertNotEquals(config, request.getConfig()); // because new option being added
        Assert.assertEquals(request.getConfig().getDatabase(), db);
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), sql);

        ProtonRequest.Mutation m = request.write();
        Assert.assertEquals(m.getConfig().getDatabase(), db);
        Assert.assertEquals(m.getStatements().size(), 0);

        m.removeOption(ProtonClientOption.DATABASE).table(table);
        Assert.assertEquals(m.getStatements().size(), 1);
        Assert.assertEquals(m.getStatements().get(0), "INSERT INTO " + table);

        m.query(sql = "delete from test where id = 1");
        Assert.assertEquals(m.getStatements().size(), 1);
        Assert.assertEquals(m.getStatements().get(0), sql);
    }

    @Test(groups = { "unit" })
    public void testCopy() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        request.compressServerResponse(true, ProtonCompression.BROTLI, 2);
        request.decompressClientRequest(true, ProtonCompression.ZSTD, 5);
        request.external(ProtonExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
        request.format(ProtonFormat.Avro);
        request.table("table1", "query_id1");
        request.query("select :a", UUID.randomUUID().toString());
        request.params("a");
        request.session(UUID.randomUUID().toString(), true, 120);
        request.set("key", "value");
        request.use("db1");

        ProtonRequest<?> copy = request.copy();
        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertEquals(copy.namedParameters, request.namedParameters);
        Assert.assertEquals(copy.options, request.options);
        Assert.assertEquals(copy.queryId, request.queryId);
        Assert.assertEquals(copy.sessionId, request.sessionId);
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());

        copy = copy.write();
        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertTrue(copy.namedParameters.isEmpty(), "Named parameters should be empty");
        Assert.assertEquals(copy.options, request.options);
        Assert.assertNull(copy.queryId, "Query ID should be null");
        Assert.assertEquals(copy.sessionId, request.sessionId);
        Assert.assertNull(copy.sql, "SQL should be null");

        ProtonRequest<?> newCopy = copy;
        Assert.assertThrows(IllegalArgumentException.class, () -> newCopy.getPreparedQuery());

        copy.external(ProtonExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
        copy.table("table1", "query_id1");
        copy.query("select :a", request.queryId);
        copy.params("a");

        Assert.assertFalse(copy.isSealed(), "Should NOT be sealed");
        Assert.assertFalse(copy == request, "Should be two different instances");
        Assert.assertEquals(copy.namedParameters, request.namedParameters);
        Assert.assertEquals(copy.options, request.options);
        Assert.assertEquals(copy.queryId, request.queryId);
        Assert.assertEquals(copy.sessionId, request.sessionId);
        Assert.assertEquals(copy.sql, request.sql);
        Assert.assertEquals(copy.getPreparedQuery(), request.getPreparedQuery());
    }

    @Test(groups = { "unit" })
    public void testFormat() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        Assert.assertEquals(request.getFormat(),
                (ProtonFormat) ProtonDefaults.FORMAT.getEffectiveDefaultValue());
        request.format(ProtonFormat.ArrowStream);
        Assert.assertEquals(request.getFormat(), ProtonFormat.ArrowStream);
        request.format(null);
        Assert.assertEquals(request.getFormat(),
                (ProtonFormat) ProtonDefaults.FORMAT.getEffectiveDefaultValue());
        request.format(ProtonFormat.Arrow);
        Assert.assertEquals(request.getFormat(), ProtonFormat.Arrow);
    }

    @Test(groups = { "unit" })
    public void testOptions() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());

        Assert.assertEquals(request.options, Collections.emptyMap());
        Properties props = new Properties();
        props.setProperty(ProtonClientOption.ASYNC.getKey(), "false");
        props.setProperty(ProtonClientOption.DATABASE.getKey(), "mydb");
        props.setProperty(ProtonClientOption.CLIENT_NAME.getKey(), "new");
        props.setProperty(ProtonClientOption.FORMAT.getKey(), "CapnProto");
        request.options(props);

        Assert.assertEquals(request.options.size(), 4);
        Assert.assertEquals(request.options.get(ProtonClientOption.ASYNC), false);
        Assert.assertEquals(request.options.get(ProtonClientOption.DATABASE), "mydb");
        Assert.assertEquals(request.options.get(ProtonClientOption.CLIENT_NAME), "new");
        Assert.assertEquals(request.options.get(ProtonClientOption.FORMAT), ProtonFormat.CapnProto);
    }

    @Test(groups = { "unit" })
    public void testParams() {
        String sql = "select :one as one, :two as two, * from my_table where key=:key and arr[:idx] in numbers(:range)";
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build())
                .query(sql);
        Assert.assertEquals(request.getQuery(), sql);
        request.params(ProtonByteValue.of(Byte.MIN_VALUE));
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select -128 as one, null as two, * from my_table where key=null and arr[null] in numbers(null)");

        request.params(ProtonStringValue.of(""),
                ProtonDateTimeValue.of("2012-12-12 12:23:34.56789", 2, ProtonValues.UTC_TIMEZONE),
                ProtonStringValue.of("key"), ProtonIntegerValue.of(1),
                ProtonBigIntegerValue.of(BigInteger.TEN));
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select '' as one, '2012-12-12 12:23:34.56789' as two, * from my_table where key='key' and arr[1] in numbers(10)");

        Map<String, String> params = new HashMap<>();
        params.put("one", ProtonFloatValue.of(1.0F).toSqlExpression());
        request.params(params);
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select 1.0 as one, null as two, * from my_table where key=null and arr[null] in numbers(null)");

        params.put("one", ProtonStringValue.of("").toSqlExpression());
        params.put("two", ProtonDateTimeValue.of("2012-12-12 12:23:34.56789", 2, ProtonValues.UTC_TIMEZONE)
                .toSqlExpression());
        params.put("key", ProtonStringValue.of("key").toSqlExpression());
        params.put("some", ProtonBigIntegerValue.of(BigInteger.ONE).toSqlExpression());
        params.put("idx", ProtonIntegerValue.of(1).toSqlExpression());
        params.put("range", ProtonBigIntegerValue.of(BigInteger.TEN).toSqlExpression());
        request.params(params);
        Assert.assertEquals(request.getQuery(), sql);
        Assert.assertEquals(request.getStatements(false).size(), 1);
        Assert.assertEquals(request.getStatements(false).get(0),
                "select '' as one, '2012-12-12 12:23:34.56789' as two, * from my_table where key='key' and arr[1] in numbers(10)");
    }

    @Test(groups = { "unit" })
    public void testSeal() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        request.compressServerResponse(true, ProtonCompression.BROTLI, 2);
        request.decompressClientRequest(true, ProtonCompression.ZSTD, 5);
        request.external(ProtonExternalTable.builder().content(new ByteArrayInputStream(new byte[0])).build());
        request.format(ProtonFormat.Avro);
        request.table("table1", "query_id1");
        request.query("select :a", UUID.randomUUID().toString());
        request.params("a");
        request.session(UUID.randomUUID().toString(), true, 120);
        request.set("key", "value");
        request.use("db1");

        ProtonRequest<?> sealed = request.seal();
        Assert.assertTrue(sealed.isSealed(), "Should be sealed");
        Assert.assertFalse(sealed == request, "Should be two different instances");
        Assert.assertEquals(sealed.namedParameters, request.namedParameters);
        Assert.assertEquals(sealed.options, request.options);
        Assert.assertEquals(sealed.queryId, request.queryId);
        Assert.assertEquals(sealed.sessionId, request.sessionId);
        Assert.assertEquals(sealed.sql, request.sql);
        Assert.assertEquals(sealed.getPreparedQuery(), request.getPreparedQuery());

        Assert.assertThrows(IllegalStateException.class, () -> sealed.write());
    }

    @Test(groups = { "unit" })
    public void testSession() {
        String sessionId = UUID.randomUUID().toString();
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        Assert.assertEquals(request.getSessionId().isPresent(), false);
        Assert.assertEquals(request.sessionId, null);
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);

        request.session(sessionId, true, 10);
        Assert.assertEquals(request.getSessionId().get(), sessionId);
        Assert.assertEquals(request.getConfig().isSessionCheck(), true);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 10);

        ProtonRequest<?> sealedRequest = request.query("select 1").seal();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);

        sealedRequest = request.query("select 2").seal();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);

        request.query("select 3").clearSession();
        Assert.assertEquals(sealedRequest.getSessionId().get(), sessionId);
        Assert.assertEquals(sealedRequest.getConfig().isSessionCheck(), true);
        Assert.assertEquals(sealedRequest.getConfig().getSessionTimeout(), 10);
        Assert.assertEquals(request.getSessionId().isPresent(), false);
        Assert.assertEquals(request.sessionId, null);
        Assert.assertEquals(request.getConfig().isSessionCheck(), false);
        Assert.assertEquals(request.getConfig().getSessionTimeout(), 0);
    }

    @Test(groups = { "unit" })
    public void testSettings() {
        ProtonRequest<?> request = ProtonClient.newInstance().connect(ProtonNode.builder().build());
        Assert.assertEquals(request.getStatements().size(), 0);
        request.set("enable_optimize_predicate_expression", 1);
        Assert.assertEquals(request.getStatements().size(), 1);
        Assert.assertEquals(request.getStatements().get(0), "SET enable_optimize_predicate_expression=1");
        request.set("log_queries_min_type", "EXCEPTION_WHILE_PROCESSING");
        Assert.assertEquals(request.getStatements().size(), 2);
        Assert.assertEquals(request.getStatements().get(1), "SET log_queries_min_type='EXCEPTION_WHILE_PROCESSING'");
    }

    @Test(groups = { "unit" })
    public void testMutation() {
        ProtonRequest.Mutation request = ProtonClient.newInstance().connect(ProtonNode.builder().build()).write();
        request.table("test_table").format(ProtonFormat.Arrow).data(new ByteArrayInputStream(new byte[0]));

        String expectedSql = "INSERT INTO test_table FORMAT Arrow";
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);

        request = request.seal();
        Assert.assertEquals(request.getQuery(), expectedSql);
        Assert.assertEquals(request.getStatements().get(0), expectedSql);
    }
}
