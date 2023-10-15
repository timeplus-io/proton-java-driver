/**
 * Declares ru.yandex.proton module.
 */
module com.proton.jdbc {
    exports com.proton.jdbc;
    
    exports ru.yandex.proton;
    exports ru.yandex.proton.domain;
    exports ru.yandex.proton.except;
    exports ru.yandex.proton.response;
    exports ru.yandex.proton.settings;
    exports ru.yandex.proton.util;

    requires transitive com.proton.client;
    requires transitive com.google.gson;
    requires transitive org.apache.httpcomponents.httpclient;
    requires transitive org.apache.httpcomponents.httpmime;
    requires transitive org.lz4.java;

    requires static java.logging;
    // requires static com.github.benmanes.caffeine;
    // requires static org.dnsjava;
    // requires static org.slf4j;
    requires static org.roaringbitmap;

    uses com.proton.client.ProtonClient;
    uses com.proton.client.ProtonDataStreamFactory;
    uses com.proton.client.ProtonDnsResolver;
    uses com.proton.client.ProtonSslContextProvider;
    uses com.proton.client.logging.LoggerFactory;
}
