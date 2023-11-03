package com.timeplus.proton.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.timeplus.proton.client.config.ProtonClientOption;
import com.timeplus.proton.client.config.ProtonOption;
import com.timeplus.proton.client.config.ProtonDefaults;

public class ProtonConfigTest {
    @Test(groups = { "unit" })
    public void testDefaultValues() {
        ProtonConfig config = new ProtonConfig(null, null, null, null, null);
        Assert.assertEquals(config.getClientName(), ProtonClientOption.CLIENT_NAME.getEffectiveDefaultValue());
        Assert.assertEquals(config.getDatabase(), ProtonDefaults.DATABASE.getEffectiveDefaultValue());

        Assert.assertEquals(config.getOption(ProtonDefaults.CLUSTER),
                ProtonDefaults.CLUSTER.getEffectiveDefaultValue());
        Assert.assertEquals(config.getOption(ProtonDefaults.HOST),
                ProtonDefaults.HOST.getEffectiveDefaultValue());
        Assert.assertEquals(config.getOption(ProtonDefaults.PORT),
                ProtonDefaults.PORT.getEffectiveDefaultValue());
        Assert.assertEquals(config.getOption(ProtonDefaults.WEIGHT),
                ProtonDefaults.WEIGHT.getEffectiveDefaultValue());
        ProtonCredentials credentials = config.getDefaultCredentials();
        Assert.assertEquals(credentials.useAccessToken(), false);
        Assert.assertEquals(credentials.getUserName(), ProtonDefaults.USER.getEffectiveDefaultValue());
        Assert.assertEquals(credentials.getPassword(), ProtonDefaults.PASSWORD.getEffectiveDefaultValue());
        Assert.assertEquals(config.getFormat(), ProtonDefaults.FORMAT.getEffectiveDefaultValue());
        Assert.assertFalse(config.getMetricRegistry().isPresent());
    }

    @Test(groups = { "unit" })
    public void testCustomValues() throws Exception {
        String clientName = "test client";
        String cluster = "test cluster";
        String database = "test_database";
        String host = "test.host";
        Integer port = 12345;
        Integer weight = -99;
        String user = "sa";
        String password = "welcome";

        Map<ProtonOption, Serializable> options = new HashMap<>();
        options.put(ProtonClientOption.CLIENT_NAME, clientName);
        options.put(ProtonDefaults.CLUSTER, cluster);
        options.put(ProtonClientOption.DATABASE, database);
        options.put(ProtonDefaults.HOST, host);
        options.put(ProtonDefaults.PORT, port);
        options.put(ProtonDefaults.WEIGHT, weight);
        options.put(ProtonDefaults.USER, "useless");
        options.put(ProtonDefaults.PASSWORD, "useless");

        Object metricRegistry = new Object();

        ProtonConfig config = new ProtonConfig(options,
                ProtonCredentials.fromUserAndPassword(user, password), null, metricRegistry);
        Assert.assertEquals(config.getClientName(), clientName);
        Assert.assertEquals(config.getDatabase(), database);
        Assert.assertEquals(config.getOption(ProtonDefaults.CLUSTER), cluster);
        Assert.assertEquals(config.getOption(ProtonDefaults.HOST), host);
        Assert.assertEquals(config.getOption(ProtonDefaults.PORT), port);
        Assert.assertEquals(config.getOption(ProtonDefaults.WEIGHT), weight);

        ProtonCredentials credentials = config.getDefaultCredentials();
        Assert.assertEquals(credentials.useAccessToken(), false);
        Assert.assertEquals(credentials.getUserName(), user);
        Assert.assertEquals(credentials.getPassword(), password);
        Assert.assertEquals(config.getPreferredProtocols().size(), 0);
        Assert.assertEquals(config.getPreferredTags().size(), 0);
        Assert.assertEquals(config.getMetricRegistry().get(), metricRegistry);
    }
}
