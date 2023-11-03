/**
 * Declares com.timeplus.proton.client module.
 */
module com.timeplus.proton.client {
    exports com.timeplus.proton.client;
    exports com.timeplus.proton.client.config;
    exports com.timeplus.proton.client.data;
    exports com.timeplus.proton.client.data.array;
    exports com.timeplus.proton.client.logging;

    requires static java.logging;
    requires static com.google.gson;
    requires static com.github.benmanes.caffeine;
    requires static org.dnsjava;
    requires static org.lz4.java;
    requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.timeplus.proton.client.ProtonClient;
    uses com.timeplus.proton.client.ProtonDataStreamFactory;
    uses com.timeplus.proton.client.ProtonDnsResolver;
    uses com.timeplus.proton.client.ProtonSslContextProvider;
    uses com.timeplus.proton.client.logging.LoggerFactory;
}
