package com.timeplus.proton.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonRequest;

public abstract class ProtonHttpConnectionFactory {
    public static ProtonHttpConnection createConnection(ProtonNode server, ProtonRequest<?> request,
            ExecutorService executor) throws IOException {
        return new HttpUrlConnectionImpl(server, request, executor);
    }
}
