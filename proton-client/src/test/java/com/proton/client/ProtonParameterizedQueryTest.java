package com.proton.client;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.proton.client.data.ProtonDateTimeValue;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ProtonParameterizedQueryTest {
    private final ProtonConfig config = new ProtonConfig();

    private String apply(ProtonParameterizedQuery q, Collection<String> p) {
        StringBuilder builder = new StringBuilder();
        q.apply(builder, p);
        return builder.toString();
    }

    private String apply(ProtonParameterizedQuery q, Map<String, String> p) {
        StringBuilder builder = new StringBuilder();
        q.apply(builder, p);
        return builder.toString();
    }

    private String apply(ProtonParameterizedQuery q, Object p, Object... more) {
        StringBuilder builder = new StringBuilder();
        q.apply(builder, p, more);
        return builder.toString();
    }

    private String apply(ProtonParameterizedQuery q, String p, String... more) {
        StringBuilder builder = new StringBuilder();
        q.apply(builder, p, more);
        return builder.toString();
    }

    @DataProvider(name = "queryWithoutParameterProvider")
    private Object[][] getQueriesWithoutAnyParameter() {
        return new Object[][] { { "1" }, { "select 1" }, { "select 1::float32" } };
    }

    @Test(groups = { "unit" })
    public void testApplyCollection() {
        String query = "select :param1::string";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Collection<String>) null), "select null::string");
        Assert.assertEquals(apply(q, Collections.emptyList()), "select null::string");
        Assert.assertEquals(apply(q, Collections.emptySet()), "select null::string");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first", "last" })), "select first::string");

        query = "select :param1::string,:param2 + 1 as result";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Collection<String>) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, Collections.emptyList()), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, Collections.emptySet()), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first" })),
                "select first::string,null + 1 as result");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first", "last" })),
                "select first::string,last + 1 as result");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first", "last", "more" })),
                "select first::string,last + 1 as result");

        query = "select :p1 p1, :p2 p2, :p1 p3";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Collection<String>) null), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, Collections.emptyList()), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, Collections.emptySet()), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first" })),
                "select first p1, null p2, first p3");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first", "last" })),
                "select first p1, last p2, first p3");
        Assert.assertEquals(apply(q, Arrays.asList(new String[] { "first", "last", "more" })),
                "select first p1, last p2, first p3");
    }

    @Test(groups = { "unit" })
    public void testApplyObjects() {
        String query = "select :param1::string";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Object) null), "select null::string");
        Assert.assertEquals(apply(q, (Object) null, (Object) null), "select null::string");
        Assert.assertEquals(apply(q, 'a'), "select 97::string");
        Assert.assertEquals(apply(q, 1, (Object) null), "select 1::string");
        Assert.assertEquals(apply(q, Collections.singletonList('a')), "select (97)::string");
        Assert.assertEquals(apply(q, Arrays.asList(1, null)), "select (1,null)::string");

        query = "select :param1::string,:param2 + 1 as result";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Object) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, (Object) null, (Object) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, 'a'), "select 97::string,null + 1 as result");
        Assert.assertEquals(apply(q, 1, (Object) null), "select 1::string,null + 1 as result");
        Assert.assertEquals(
                apply(q, ProtonDateTimeValue.ofNull(3, ProtonValues.UTC_TIMEZONE).update(1), (Object) null),
                "select '1970-01-01 00:00:00.001'::string,null + 1 as result");
        Assert.assertEquals(apply(q, Collections.singletonList('a')), "select (97)::string,null + 1 as result");
        Assert.assertEquals(apply(q, Arrays.asList(1, null)), "select (1,null)::string,null + 1 as result");
        Assert.assertEquals(
                apply(q, Arrays.asList(ProtonDateTimeValue.ofNull(3, ProtonValues.UTC_TIMEZONE).update(1),
                        null)),
                "select ('1970-01-01 00:00:00.001',null)::string,null + 1 as result");

        query = "select :p1 p1, :p2 p2, :p1 p3";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Object) null), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, (Object) null, (Object) null), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, 'a'), "select 97 p1, null p2, 97 p3");
        Assert.assertEquals(apply(q, 1, (Object) null), "select 1 p1, null p2, 1 p3");
        Assert.assertEquals(
                apply(q, ProtonDateTimeValue.ofNull(3, ProtonValues.UTC_TIMEZONE).update(1), (Object) null),
                "select '1970-01-01 00:00:00.001' p1, null p2, '1970-01-01 00:00:00.001' p3");
        Assert.assertEquals(apply(q, Collections.singletonList('a')), "select (97) p1, null p2, (97) p3");
        Assert.assertEquals(apply(q, Arrays.asList(1, null)), "select (1,null) p1, null p2, (1,null) p3");
        Assert.assertEquals(
                apply(q, Arrays.asList(ProtonDateTimeValue.ofNull(3, ProtonValues.UTC_TIMEZONE).update(1),
                        null)),
                "select ('1970-01-01 00:00:00.001',null) p1, null p2, ('1970-01-01 00:00:00.001',null) p3");
        Assert.assertEquals(
                apply(q, new StringBuilder("321"), new StringBuilder("123"), new StringBuilder("456")),
                "select 321 p1, 123 p2, 321 p3");
    }

    @Test(groups = { "unit" })
    public void testApplyMap() {
        String query = "select :param1::string";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Map<String, String>) null), "select null::string");
        Assert.assertEquals(apply(q, Collections.emptyMap()), "select null::string");
        Assert.assertEquals(apply(q, Collections.singletonMap("key", "value")), "select null::string");
        Assert.assertEquals(apply(q, Collections.singletonMap("param1", "value")), "select value::string");

        query = "select :param1::string,:param2 + 1 as result";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (Map<String, String>) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, Collections.emptyMap()), "select null::string,null + 1 as result");
        Map<String, String> map = new HashMap<>();
        map.put("param2", "v2");
        map.put("param1", "v1");
        map.put("param3", "v3");
        Assert.assertEquals(apply(q, Collections.singletonMap("key", "value")),
                "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, Collections.singletonMap("param2", "value")),
                "select null::string,value + 1 as result");
        Assert.assertEquals(apply(q, map), "select v1::string,v2 + 1 as result");
    }

    @Test(groups = { "unit" })
    public void testApplyStrings() {
        String query = "select :param1::string";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (String) null), "select null::string");
        Assert.assertEquals(apply(q, (String) null, (String) null), "select null::string");
        Assert.assertEquals(apply(q, "'a'"), "select 'a'::string");
        Assert.assertEquals(apply(q, "1", (String) null), "select 1::string");

        query = "select :param1::string,:param2 + 1 as result";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (String) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, (String) null, (String) null), "select null::string,null + 1 as result");
        Assert.assertEquals(apply(q, "'a'"), "select 'a'::string,null + 1 as result");
        Assert.assertEquals(apply(q, "1", (String) null), "select 1::string,null + 1 as result");

        query = "select :p1 p1, :p2 p2, :p1 p3";
        q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertEquals(apply(q, (String) null), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, (String) null, (String) null), "select null p1, null p2, null p3");
        Assert.assertEquals(apply(q, "'a'"), "select 'a' p1, null p2, 'a' p3");
        Assert.assertEquals(apply(q, "1", (String) null), "select 1 p1, null p2, 1 p3");
        Assert.assertEquals(apply(q, "1", "2", "3"), "select 1 p1, 2 p2, 1 p3");
    }

    @Test(groups = { "unit" })
    public void testInvalidQuery() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonParameterizedQuery.of(config, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonParameterizedQuery.of(config, ""));
    }

    @Test(dataProvider = "queryWithoutParameterProvider", groups = { "unit" })
    public void testQueryWithoutAnyParameter(String query) {
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertEquals(q.getOriginalQuery(), query);
        Assert.assertEquals(apply(q, (String) null), query);
        Assert.assertEquals(apply(q, (Object) null), query);
        Assert.assertEquals(apply(q, (Collection<String>) null), query);
        Assert.assertEquals(apply(q, Collections.emptyList()), query);
        Assert.assertEquals(apply(q, Collections.emptySet()), query);
        Assert.assertEquals(apply(q, (Enumeration<String>) null), query);
        Assert.assertEquals(apply(q, new Enumeration<String>() {
            @Override
            public boolean hasMoreElements() {
                return false;
            }

            @Override
            public String nextElement() {
                throw new NoSuchElementException();
            }
        }), query);
        Assert.assertEquals(apply(q, (Map<String, String>) null), query);
        Assert.assertEquals(apply(q, Collections.emptyMap()), query);
        Assert.assertEquals(apply(q, "test"), query);
        Assert.assertEquals(apply(q, "test1", "test2"), query);
        Assert.assertFalse(q.hasParameter());
        Assert.assertTrue((Object) q.getParameters() == Collections.emptyList());
        Assert.assertEquals(q.getQueryParts().toArray(new String[0][]),
                new String[][] { new String[] { query, null } });
    }

    @Test(groups = { "unit" })
    public void testQueryWithParameters() {
        String query = "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (:no ) and value = :v(string)";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, query);
        Assert.assertTrue(q.getOriginalQuery() == query);
        Assert.assertTrue(q.hasParameter());
        Assert.assertEquals(q.getQueryParts().toArray(new String[0][]), new String[][] { new String[] {
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (",
                "no" }, new String[] { " ) and value = ", "v" } });
        Assert.assertEquals(apply(q, (String) null),
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (null ) and value = null");
        Assert.assertEquals(apply(q, (String) null, (String) null, (String) null),
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (null ) and value = null");
        Assert.assertEquals(apply(q, "1", "2", "3"),
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (1 ) and value = 2");
        Assert.assertEquals(apply(q, "''", "'\\''", "233"),
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in ('' ) and value = '\\''");
    }

    @Test(groups = { "unit" })
    public void testApplyNamedParameters() {
        String sql = "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (:no ) and value = :v(string)";
        Map<String, String> params = new HashMap<>();
        params.put("no", "1,2,3");
        params.put("v", "'s t r'");

        Assert.assertEquals(ProtonParameterizedQuery.apply(sql, params),
                "select 2>1?3:2, name, value, value::decimal64(3) from my_table where value != ':ccc' and num in (1,2,3 ) and value = 's t r'");
    }

    @Test(groups = { "unit" })
    public void testApplyTypedParameters() {
        LocalDateTime ts = LocalDateTime.ofEpochSecond(10000, 123456789, ZoneOffset.UTC);
        String sql = "select :ts1 ts1, :ts2(datetime32) ts2, :ts2 ts3";
        ProtonParameterizedQuery q = ProtonParameterizedQuery.of(config, sql);
        ProtonValue[] templates = q.getParameterTemplates();
        Assert.assertEquals(templates.length, q.getParameters().size());
        Assert.assertNull(templates[0]);
        Assert.assertTrue(templates[1] instanceof ProtonDateTimeValue);
        Assert.assertEquals(((ProtonDateTimeValue) templates[1]).getScale(), 0);
        Assert.assertEquals(apply(q, ts, ts),
                "select '1970-01-01 02:46:40.123456789' ts1, '1970-01-01 02:46:40' ts2, '1970-01-01 02:46:40' ts3");
    }
}
