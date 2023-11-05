package com.timeplus.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;

import com.timeplus.proton.client.config.ProtonClientOption;
import com.timeplus.proton.client.config.ProtonDefaults;

public class ProtonNodeTest {
    private void checkDefaultValues(ProtonNode node) {
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getCluster(), ProtonDefaults.CLUSTER.getEffectiveDefaultValue());
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getProtocol(), ProtonDefaults.PROTOCOL.getEffectiveDefaultValue());
        Assert.assertFalse(node.getCredentials().isPresent());
        Assert.assertTrue(node.getTags().isEmpty());
        Assert.assertNotNull(node.getAddress());
        Assert.assertEquals(node.getHost(), ProtonDefaults.HOST.getEffectiveDefaultValue());
        Assert.assertEquals(node.getPort(), ProtonDefaults.PORT.getEffectiveDefaultValue());
        Assert.assertEquals(node.getWeight(), ProtonDefaults.WEIGHT.getEffectiveDefaultValue());
    }

    private void checkCustomValues(ProtonNode node, String cluster, String host, int port, int weight,
            ProtonProtocol protocol, String database, ProtonCredentials credentials, String[] tags) {
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getCluster(), cluster);
        Assert.assertNotNull(node.getAddress());
        Assert.assertEquals(node.getHost(), host);
        Assert.assertEquals(node.getPort(), port);
        Assert.assertEquals(node.getWeight(), weight);
        Assert.assertEquals(node.getProtocol(), protocol);
        Assert.assertEquals(node.getDatabase().orElse(null), database);
        Assert.assertEquals(node.getCredentials().orElse(null), credentials);
        Assert.assertEquals(node.getTags().size(), tags.length);
        for (String t : tags) {
            Assert.assertTrue(node.getTags().contains(t));
        }
    }

    @Test(groups = { "unit" })
    public void testDefaultNode() {
        checkDefaultValues(ProtonNode.builder().build());
    }

    @Test(groups = { "unit" })
    public void testCustomNode() {
        String cluster = "my_cluster";
        String database = "my_db";
        String host = "non-existing.host";
        int port = 38123;
        int weight = -100;
        ProtonProtocol protocol = ProtonProtocol.HTTP;
        ProtonCredentials credentials = ProtonCredentials.fromUserAndPassword("user", "passwd");
        String[] tags = new String[] { "dc1", "rack1", "server1", "id1" };

        ProtonNode node = ProtonNode.builder().cluster(cluster).host(host).port(protocol, port).weight(weight)
                .database(database).credentials(credentials).tags(Arrays.asList(tags)).build();
        checkCustomValues(node, cluster, host, port, weight, protocol, database, credentials, tags);
    }

    @Test(groups = { "unit" })
    public void testBuildWithNode() {
        String cluster = "my_cluster";
        String database = "my_db";
        String host = "non-existing.host";
        int port = 38123;
        int weight = -100;
        ProtonProtocol protocol = ProtonProtocol.HTTP;
        ProtonCredentials credentials = ProtonCredentials.fromUserAndPassword("user", "passwd");
        String[] tags = new String[] { "dc1", "rack1", "server1", "id1" };

        ProtonNode base = ProtonNode.builder().cluster(cluster).host(host).port(protocol, port).weight(weight)
                .database(database).credentials(credentials).tags(null, tags).build();
        ProtonNode node = ProtonNode.builder(base).build();
        checkCustomValues(node, cluster, host, port, weight, protocol, database, credentials, tags);

        node = ProtonNode.builder(base).cluster(null).host(null).port(null, null).weight(null).database(null)
                .credentials(null).tags(null, (String[]) null).build();
        checkDefaultValues(node);
    }

    @Test(groups = { "unit" })
    public void testBuildInOneGo() {
        String host = "non-existing.host";
        String database = "my_db";
        ProtonProtocol protocol = ProtonProtocol.TCP;
        int port = 19000;
        ProtonNode node = ProtonNode.of(host, protocol, port, database);
        checkCustomValues(node, (String) ProtonDefaults.CLUSTER.getEffectiveDefaultValue(), host, port,
                (int) ProtonDefaults.WEIGHT.getEffectiveDefaultValue(), protocol, database, null, new String[0]);

        protocol = ProtonProtocol.GRPC;
        node = ProtonNode.of(host, protocol, port, database, "read-only", "primary");
        checkCustomValues(node, (String) ProtonDefaults.CLUSTER.getEffectiveDefaultValue(), host, port,
                (int) ProtonDefaults.WEIGHT.getEffectiveDefaultValue(), protocol, database, null,
                new String[] { "read-only", "primary" });
    }

    @Test(groups = { "unit" })
    public void testDatabase() {
        ProtonConfig config = new ProtonConfig(Collections.singletonMap(ProtonClientOption.DATABASE, "ttt"),
                null, null, null);
        ProtonNode node = ProtonNode.builder().build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        Assert.assertEquals(node.getDatabase().orElse(null), null);
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ProtonNode.builder().database("").build();
        Assert.assertEquals(node.hasPreferredDatabase(), false);
        Assert.assertEquals(node.getDatabase().orElse(null), "");
        Assert.assertEquals(node.getDatabase(config), config.getDatabase());

        node = ProtonNode.builder().database("123").build();
        Assert.assertEquals(node.hasPreferredDatabase(), true);
        Assert.assertEquals(node.getDatabase().orElse(null), "123");
        Assert.assertEquals(node.getDatabase(config), "ttt");
        Assert.assertEquals(node.getDatabase(new ProtonConfig()), "123");
    }
}
