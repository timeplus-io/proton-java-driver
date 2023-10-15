package com.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonDataStreamFactoryTest {
    @Test(groups = { "unit" })
    public void testGetInstance() throws Exception {
        Assert.assertNotNull(ProtonDataStreamFactory.getInstance());
    }
}
