package com.timeplus.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonClientTest {
    @Test(groups = { "unit" })
    public void testQuery() throws Exception {
        ProtonClient client = ProtonClient.builder().build();
        Assert.assertNotNull(client);
        ProtonRequest<?> req = client.connect(ProtonNode.builder().build());
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.query("select 1").execute().get());
    }

    @Test(groups = { "unit" })
    public void testMutation() throws Exception {
        ProtonClient client = ProtonClient.builder().build();
        Assert.assertNotNull(client);
        ProtonRequest.Mutation req = client.connect(ProtonNode.builder().build()).write();
        Assert.assertNotNull(req);
        Assert.assertNull(req.config);
        Assert.assertNotNull(req.getConfig());
        Assert.assertNotNull(req.config);
        Assert.assertEquals(req.getClient(), client);
        Assert.assertEquals(req.getFormat(), client.getConfig().getFormat());
        Assert.assertNull(req.sql);
        Assert.assertNull(req.table("my_table").format(ProtonFormat.RowBinary).execute().get());
    }
}
