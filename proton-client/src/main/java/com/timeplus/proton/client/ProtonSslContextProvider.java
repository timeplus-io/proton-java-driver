package com.timeplus.proton.client;

import java.util.Optional;
import javax.net.ssl.SSLException;

/**
 * This interface defines how to build Netty SSL context based on given
 * configuration and target server.
 */
public interface ProtonSslContextProvider {
    /**
     * Get non-null SSL context provider.
     * 
     * @return non-null SSL context provider
     */
    static ProtonSslContextProvider getProvider() {
        return ProtonUtils.getService(ProtonSslContextProvider.class);
    }

    /**
     * Get SSL context.
     * 
     * @param <T>             type of SSL context
     * @param sslContextClass SSL context class
     * @param config          client config
     * @return SSL context
     * @throws SSLException when error occured getting SSL context
     */
    <T> Optional<T> getSslContext(Class<? extends T> sslContextClass, ProtonConfig config) throws SSLException;
}
