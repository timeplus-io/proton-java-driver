package com.proton.client.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.proton.client.AbstractClient;
// import com.proton.client.ProtonCluster;
import com.proton.client.ProtonException;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonProtocol;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.config.ProtonOption;
import com.proton.client.data.ProtonStreamResponse;
import com.proton.client.http.config.ProtonHttpOption;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

public class ProtonHttpClient extends AbstractClient<ProtonHttpConnection> {
    private static final Logger log = LoggerFactory.getLogger(ProtonHttpClient.class);

    @Override
    protected boolean checkConnection(ProtonHttpConnection connection, ProtonNode requestServer,
            ProtonNode currentServer, ProtonRequest<?> request) {
        // return false to suggest creating a new connection
        return connection != null && connection.isReusable() && requestServer.equals(currentServer);
    }

    @Override
    protected ProtonHttpConnection newConnection(ProtonHttpConnection connection, ProtonNode server,
            ProtonRequest<?> request) {
        if (connection != null && connection.isReusable()) {
            closeConnection(connection, false);
        }

        try {
            return ProtonHttpConnectionFactory.createConnection(server, request, getExecutor());
        } catch (IOException e) {
            throw new CompletionException(e);
        }
    }

    @Override
    protected void closeConnection(ProtonHttpConnection connection, boolean force) {
        try {
            connection.close();
        } catch (Exception e) {
            log.warn("Failed to close http connection due to: %s", e.getMessage());
        }
    }

    protected String buildQueryParams(Map<String, String> params) {
        if (params == null || params.isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            builder.append(ProtonHttpConnection.urlEncode(entry.getKey(), StandardCharsets.UTF_8)).append('=')
                    .append(ProtonHttpConnection.urlEncode(entry.getValue(), StandardCharsets.UTF_8)).append('&');
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    protected ProtonResponse postRequest(ProtonRequest<?> sealedRequest) throws IOException {
        ProtonHttpConnection conn = getConnection(sealedRequest);

        List<String> stmts = sealedRequest.getStatements(false);
        int size = stmts.size();
        String sql;
        if (size == 0) {
            throw new IllegalArgumentException("At least one SQL statement is required for execution");
        } else if (size > 1) {
            throw new IllegalArgumentException("Expect one SQL statement to execute but we got " + size);
        } else {
            sql = stmts.get(0);
        }

        log.debug("Query: %s", sql);
        ProtonHttpResponse httpResponse = conn.post(sql, sealedRequest.getInputStream().orElse(null),
                sealedRequest.getExternalTables(), null);
        return ProtonStreamResponse.of(httpResponse.getConfig(sealedRequest), httpResponse.getInputStream(),
                sealedRequest.getSettings(), null, httpResponse.summary);
    }

    @Override
    public boolean accept(ProtonProtocol protocol) {
        return ProtonProtocol.HTTP == protocol || super.accept(protocol);
    }

    @Override
    public CompletableFuture<ProtonResponse> execute(ProtonRequest<?> request) {
        // sealedRequest is an immutable copy of the original request
        final ProtonRequest<?> sealedRequest = request.seal();

        if (sealedRequest.getConfig().isAsync()) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return postRequest(sealedRequest);
                } catch (IOException e) {
                    throw new CompletionException(ProtonException.of(e, sealedRequest.getServer()));
                }
            }, getExecutor());
        } else {
            try {
                return CompletableFuture.completedFuture(postRequest(sealedRequest));
            } catch (IOException e) {
                return failedResponse(ProtonException.of(e, sealedRequest.getServer()));
            }
        }
    }

    @Override
    public final Class<? extends ProtonOption> getOptionClass() {
        return ProtonHttpOption.class;
    }

    @Override
    public boolean ping(ProtonNode server, int timeout) {
        if (server != null) {
            // server = ProtonCluster.probe(server, timeout);
            return getConnection(connect(server)).ping(timeout);
        }

        return false;
    }
}
