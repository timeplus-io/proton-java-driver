package com.timeplus.proton.client.naming;

import java.net.InetSocketAddress;

import com.timeplus.proton.client.ProtonProtocol;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SrvResolverTest {
    @Test(groups = { "integration" })
    public void testLookup() {
        String dns = "_sip._udp.sip.voice.google.com";
        Assert.assertNotNull(new SrvResolver().lookup(dns, true));
        Assert.assertNotNull(new SrvResolver().lookup(dns, false));
    }

    @Test(groups = { "integration" })
    public void testResolv() throws Exception {
        String host = "_sip._udp.sip.voice.google.com";
        int port = 5060;

        String dns = "_sip._udp.sip.voice.google.com";
        Assert.assertEquals(new SrvResolver().resolve(ProtonProtocol.ANY, host, 0),
                new InetSocketAddress(host, port));
    }
}