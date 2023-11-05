module com.timeplus.proton.client.http {
    exports com.timeplus.proton.client.http;
    exports com.timeplus.proton.client.http.config;

    provides com.timeplus.proton.client.ProtonClient with com.timeplus.proton.client.http.ProtonHttpClient;

    requires static com.google.gson;

    requires transitive com.timeplus.proton.client;
}
