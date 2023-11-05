package com.timeplus.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonProtocolTest {
    @Test(groups = { "unit" })
    public void testUriScheme() {
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> ProtonProtocol.GRPC.getUriSchemes().add("a"));
        Assert.assertThrows(UnsupportedOperationException.class,
                () -> ProtonProtocol.HTTP.getUriSchemes().remove(0));

        for (ProtonProtocol p : ProtonProtocol.values()) {
            for (String s : p.getUriSchemes()) {
                Assert.assertEquals(ProtonProtocol.fromUriScheme(s), p);
                Assert.assertEquals(ProtonProtocol.fromUriScheme(s.toUpperCase()), p);
                Assert.assertEquals(ProtonProtocol.fromUriScheme(s + " "), ProtonProtocol.ANY);
            }
        }

        Assert.assertEquals(ProtonProtocol.fromUriScheme("gRPC"), ProtonProtocol.GRPC);
    }
}
