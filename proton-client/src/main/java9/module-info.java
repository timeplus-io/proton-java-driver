/**
 * Declares com.proton.client module.
 */
module com.proton.client {
    exports com.proton.client;
    exports com.proton.client.config;
    exports com.proton.client.data;
    exports com.proton.client.data.array;
    exports com.proton.client.logging;

    requires static java.logging;
    requires static com.google.gson;
    requires static com.github.benmanes.caffeine;
    requires static org.dnsjava;
    requires static org.lz4.java;
    requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.proton.client.ProtonClient;
    uses com.proton.client.ProtonDataStreamFactory;
    uses com.proton.client.ProtonDnsResolver;
    uses com.proton.client.ProtonSslContextProvider;
    uses com.proton.client.logging.LoggerFactory;
}
