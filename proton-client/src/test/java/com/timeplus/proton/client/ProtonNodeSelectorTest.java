package com.timeplus.proton.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonNodeSelectorTest {
    @SuppressWarnings({ "unchecked", "varargs" })
    private <T> List<T> listOf(T... values) {
        List<T> list = new ArrayList<>(values.length);
        for (T value : values) {
            list.add(value);
        }
        return Collections.unmodifiableList(list);
    }

    @SuppressWarnings({ "unchecked", "varargs" })
    private <T> Set<T> setOf(T... values) {
        Set<T> set = new HashSet<>();
        for (T value : values) {
            set.add(value);
        }
        return Collections.unmodifiableSet(set);
    }

    @Test(groups = { "unit" })
    public void testBuilder() {
        Assert.assertTrue(ProtonNodeSelector.of((Collection<ProtonProtocol>) null,
                (Collection<String>) null) == ProtonNodeSelector.EMPTY);
        Assert.assertTrue(
                ProtonNodeSelector.of(new ArrayList<ProtonProtocol>(), null) == ProtonNodeSelector.EMPTY);
        Assert.assertTrue(ProtonNodeSelector.of(null, new ArrayList<String>()) == ProtonNodeSelector.EMPTY);
        Assert.assertTrue(ProtonNodeSelector.of(new ArrayList<ProtonProtocol>(),
                new ArrayList<String>()) == ProtonNodeSelector.EMPTY);
    }

    @Test(groups = { "unit" })
    public void testGetPreferredProtocols() {
        Assert.assertEquals(ProtonNodeSelector.of(listOf((ProtonProtocol) null), null).getPreferredProtocols(),
                Collections.emptyList());
        Assert.assertEquals(ProtonNodeSelector.of(listOf(ProtonProtocol.ANY), null).getPreferredProtocols(),
                Collections.emptyList());
        Assert.assertEquals(ProtonNodeSelector.of(listOf(ProtonProtocol.HTTP, ProtonProtocol.ANY), null)
                .getPreferredProtocols(), Collections.emptyList());

        Assert.assertEquals(ProtonNodeSelector
                .of(listOf(ProtonProtocol.HTTP, ProtonProtocol.GRPC, ProtonProtocol.HTTP), null)
                .getPreferredProtocols(), listOf(ProtonProtocol.HTTP, ProtonProtocol.GRPC));
    }

    @Test(groups = { "unit" })
    public void testGetPreferredTags() {
        Assert.assertEquals(ProtonNodeSelector.of(null, listOf((String) null)).getPreferredTags(),
                Collections.emptySet());
        Assert.assertEquals(ProtonNodeSelector.of(null, listOf("")).getPreferredTags(), Collections.emptySet());

        Assert.assertEquals(ProtonNodeSelector.of(null, listOf("A", "C", "D", "C", "B")).getPreferredTags(),
                setOf("A", "C", "D", "B"));
        Assert.assertEquals(ProtonNodeSelector.of(null, setOf("A", "C", "D", "C", "B")).getPreferredTags(),
                setOf("A", "C", "D", "B"));
    }

    @Test(groups = { "unit" })
    public void testMatchAnyOfPreferredProtocols() {
        ProtonNodeSelector selector = ProtonNodeSelector.of(listOf((ProtonProtocol) null), null);
        for (ProtonProtocol p : ProtonProtocol.values()) {
            Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
        }

        selector = ProtonNodeSelector.of(listOf(ProtonProtocol.ANY), null);
        for (ProtonProtocol p : ProtonProtocol.values()) {
            Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
        }

        for (ProtonProtocol protocol : ProtonProtocol.values()) {
            if (protocol == ProtonProtocol.ANY) {
                continue;
            }

            selector = ProtonNodeSelector.of(listOf(protocol), null);
            for (ProtonProtocol p : ProtonProtocol.values()) {
                if (p == ProtonProtocol.ANY || p == protocol) {
                    Assert.assertTrue(selector.matchAnyOfPreferredProtocols(p));
                } else {
                    Assert.assertFalse(selector.matchAnyOfPreferredProtocols(p));
                }
            }
        }
    }

    @Test(groups = { "unit" })
    public void testMatchAllPreferredTags() {
        List<String> tags = listOf((String) null);
        ProtonNodeSelector selector = ProtonNodeSelector.of(null, tags);
        Assert.assertTrue(selector.matchAllPreferredTags(Collections.emptyList()));

        selector = ProtonNodeSelector.of(null, tags = listOf((String) null, ""));
        Assert.assertTrue(selector.matchAllPreferredTags(Collections.emptyList()));

        selector = ProtonNodeSelector.of(null, tags = listOf("1", "3", "2"));
        Assert.assertTrue(selector.matchAllPreferredTags(tags));
        Assert.assertTrue(selector.matchAllPreferredTags(listOf("2", "2", "1", "3", "3")));
    }

    @Test(groups = { "unit" })
    public void testMatchAnyOfPreferredTags() {
        List<String> tags = listOf((String) null);
        ProtonNodeSelector selector = ProtonNodeSelector.of(null, tags);
        Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.emptyList()));

        selector = ProtonNodeSelector.of(null, tags = listOf((String) null, ""));
        Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.emptyList()));

        selector = ProtonNodeSelector.of(null, tags = listOf("1", "3", "2"));
        for (String tag : tags) {
            Assert.assertTrue(selector.matchAnyOfPreferredTags(Collections.singleton(tag)));
        }
        Assert.assertTrue(selector.matchAnyOfPreferredTags(listOf("v", "3", "5")));
    }
}
