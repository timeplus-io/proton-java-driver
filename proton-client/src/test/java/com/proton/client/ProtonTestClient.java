package com.proton.client;

import java.util.concurrent.CompletableFuture;

public class ProtonTestClient implements ProtonClient {
    private ProtonConfig clientConfig;

    @Override
    public boolean accept(ProtonProtocol protocol) {
        return true;
    }

    @Override
    public CompletableFuture<ProtonResponse> execute(ProtonRequest<?> request) {
        return CompletableFuture.supplyAsync(() -> null);
    }

    @Override
    public ProtonConfig getConfig() {
        return this.clientConfig;
    }

    @Override
    public void init(ProtonConfig config) {
        ProtonClient.super.init(config);

        this.clientConfig = config;
    }

    @Override
    public void close() {
        this.clientConfig = null;
    }
}
