package com.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.proton.client.config.ProtonClientOption;

public class ProtonClientBuilderTest {
    @Test(groups = { "unit" })
    public void testBuildClient() {
        ProtonClientBuilder builder = new ProtonClientBuilder();
        ProtonClient client = builder.build();
        Assert.assertTrue(client instanceof ProtonTestClient);
        Assert.assertNotEquals(builder.build(), client);

        ProtonTestClient testClient = (ProtonTestClient) client;
        Assert.assertTrue(testClient.getConfig() == builder.getConfig());
    }

    @Test(groups = { "unit" })
    public void testBuildConfig() {
        ProtonClientBuilder builder = new ProtonClientBuilder();
        ProtonConfig config = builder.getConfig();
        Assert.assertNotNull(config);
        Assert.assertEquals(builder.getConfig(), config);

        String clientName = "test client";
        builder.option(ProtonClientOption.CLIENT_NAME, clientName);
        Assert.assertNotEquals(builder.getConfig(), config);
        config = builder.getConfig();
        Assert.assertEquals(config.getClientName(), clientName);
        Assert.assertEquals(config.getOption(ProtonClientOption.CLIENT_NAME), clientName);
    }
}
