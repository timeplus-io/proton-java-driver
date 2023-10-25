module com.proton.client.http {
    exports com.proton.client.http;
    exports com.proton.client.http.config;

    provides com.proton.client.ProtonClient with com.proton.client.http.ProtonHttpClient;

    requires java.net.http;

    requires static com.google.gson;

    requires transitive com.proton.client;
}
