/**
 * Declares com.timeplus.proton module.
 */
module com.timeplus.proton.jdbc {
    exports com.timeplus.proton.jdbc;

    requires transitive com.timeplus.proton.client;
    requires transitive com.google.gson;
    requires transitive org.apache.httpcomponents.httpclient;
    requires transitive org.apache.httpcomponents.httpmime;
    requires transitive org.lz4.java;

    requires static java.logging;
    // requires static com.github.benmanes.caffeine;
    // requires static org.dnsjava;
    // requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.timeplus.proton.client.ProtonClient;
    uses com.timeplus.proton.client.ProtonDataStreamFactory;
    uses com.timeplus.proton.client.ProtonDnsResolver;
    uses com.timeplus.proton.client.ProtonSslContextProvider;
    uses com.timeplus.proton.client.logging.LoggerFactory;
}
