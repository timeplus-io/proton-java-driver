package com.timeplus.proton.client;

import java.net.InetSocketAddress;

import com.timeplus.proton.client.config.ProtonDefaults;
import com.timeplus.proton.client.logging.Logger;
import com.timeplus.proton.client.logging.LoggerFactory;
import com.timeplus.proton.client.naming.SrvResolver;

/**
 * Default DNS resolver. It tries to look up service record (SRV record) when
 * {@link ProtonDefaults#SRV_RESOLVE} is set to
 * {@code true}.
 */
public class ProtonDnsResolver {
    private static final Logger log = LoggerFactory.getLogger(ProtonDnsResolver.class);

    private static final ProtonDnsResolver instance = ProtonUtils.getService(ProtonDnsResolver.class,
            new ProtonDnsResolver());

    protected static ProtonDnsResolver newInstance() {
        ProtonDnsResolver resolver = null;

        if ((boolean) ProtonDefaults.SRV_RESOLVE.getEffectiveDefaultValue()) {
            try {
                resolver = new SrvResolver();
            } catch (Throwable e) {
                log.warn("Failed to enable SRV resolver due to:", e);
            }
        }

        return resolver == null ? new ProtonDnsResolver() : resolver;
    }

    public static ProtonDnsResolver getInstance() {
        return instance;
    }

    public InetSocketAddress resolve(ProtonProtocol protocol, String host, int port) {
        return new InetSocketAddress(host, port);
    }
}
