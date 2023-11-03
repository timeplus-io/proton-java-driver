package com.timeplus.proton.client.http;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import com.timeplus.proton.client.ProtonNode;
import com.timeplus.proton.client.ProtonRequest;
import com.timeplus.proton.client.http.config.ProtonHttpOption;
import com.timeplus.proton.client.http.config.HttpConnectionProvider;

public abstract class ProtonHttpConnectionFactory {
    public static ProtonHttpConnection createConnection(ProtonNode server, ProtonRequest<?> request,
            ExecutorService executor) throws IOException {
        HttpConnectionProvider provider = (HttpConnectionProvider) request.getConfig()
                .getOption(ProtonHttpOption.CONNECTION_PROVIDER);

        try {
            return provider == null || provider == HttpConnectionProvider.HTTP_URL_CONNECTION
                    ? new HttpUrlConnectionImpl(server, request, executor)
                    : new HttpClientConnectionImpl(server, request, executor);
        } catch (IOException e) {
            throw e;
        } catch (Throwable t) {
            return new HttpUrlConnectionImpl(server, request, executor);
        }
    }
}
