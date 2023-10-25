package com.proton.client.grpc;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.proton.client.AbstractClient;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonColumn;
import com.proton.client.ProtonCompression;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonCredentials;
import com.proton.client.ProtonException;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonProtocol;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonUtils;
import com.proton.client.config.ProtonOption;
import com.proton.client.data.ProtonExternalTable;
import com.proton.client.grpc.config.ProtonGrpcOption;
import com.proton.client.grpc.impl.ProtonGrpc;
import com.proton.client.grpc.impl.Compression;
import com.proton.client.grpc.impl.CompressionAlgorithm;
import com.proton.client.grpc.impl.CompressionLevel;
import com.proton.client.grpc.impl.ExternalTable;
import com.proton.client.grpc.impl.NameAndType;
import com.proton.client.grpc.impl.QueryInfo;
import com.proton.client.grpc.impl.Result;
import com.proton.client.grpc.impl.QueryInfo.Builder;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

public class ProtonGrpcClient extends AbstractClient<ManagedChannel> {
    private static final Logger log = LoggerFactory.getLogger(ProtonGrpcClient.class);

    private static final Compression COMPRESSION_DISABLED = Compression.newBuilder()
            .setAlgorithm(CompressionAlgorithm.NO_COMPRESSION).setLevel(CompressionLevel.COMPRESSION_NONE).build();

    protected static String getRequestEncoding(ProtonConfig config) {
        if (config.isDecompressClientRequet()) {
            return ProtonCompression.NONE.encoding();
        }

        String encoding = ProtonCompression.GZIP.encoding();
        switch (config.getDecompressAlgorithmForClientRequest()) {
            case GZIP:
                break;
            default:
                log.debug("Unsupported encoding [%s], change to [%s]",
                        config.getDecompressAlgorithmForClientRequest().encoding(),
                        encoding);
                break;
        }

        return encoding;
    }

    protected static Compression getResultCompression(ProtonConfig config) {
        if (!config.isCompressServerResponse()) {
            return COMPRESSION_DISABLED;
        }

        Compression.Builder builder = Compression.newBuilder();
        CompressionAlgorithm algorithm = CompressionAlgorithm.GZIP;
        CompressionLevel level = CompressionLevel.COMPRESSION_MEDIUM;
        switch (config.getDecompressAlgorithmForClientRequest()) {
            case NONE:
                algorithm = CompressionAlgorithm.NO_COMPRESSION;
                break;
            case DEFLATE:
                algorithm = CompressionAlgorithm.DEFLATE;
                break;
            case GZIP:
                break;
            // case STREAM_GZIP:
            default:
                log.debug("Unsupported algorithm [%s], change to [%s]", config.getDecompressAlgorithmForClientRequest(),
                        algorithm);
                break;
        }

        int l = config.getDecompressLevelForClientRequest();
        if (l <= 0) {
            level = CompressionLevel.COMPRESSION_NONE;
        } else if (l < 3) {
            level = CompressionLevel.COMPRESSION_LOW;
        } else if (l < 7) {
            level = CompressionLevel.COMPRESSION_MEDIUM;
        } else {
            level = CompressionLevel.COMPRESSION_HIGH;
        }

        return builder.setAlgorithm(algorithm).setLevel(level).build();
    }

    protected static QueryInfo convert(ProtonNode server, ProtonRequest<?> request) {
        ProtonConfig config = request.getConfig();
        ProtonCredentials credentials = server.getCredentials(config);

        Builder builder = QueryInfo.newBuilder();
        String database = server.getDatabase(config);
        if (!ProtonChecker.isNullOrEmpty(database)) {
            builder.setDatabase(server.getDatabase(config));
        }
        builder.setUserName(credentials.getUserName())
                .setPassword(credentials.getPassword()).setOutputFormat(request.getFormat().name());

        Optional<String> optionalValue = request.getSessionId();
        if (optionalValue.isPresent()) {
            builder.setSessionId(optionalValue.get());
        }
        if (config.isSessionCheck()) {
            builder.setSessionCheck(true);
        }
        if (config.getSessionTimeout() > 0) {
            builder.setSessionTimeout(config.getSessionTimeout());
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            builder.setQueryId(optionalValue.get());
        }

        builder.setResultCompression(getResultCompression(config));

        // builder.setNextQueryInfo(true);
        for (Entry<String, Object> s : request.getSettings().entrySet()) {
            builder.putSettings(s.getKey(), String.valueOf(s.getValue()));
        }

        Optional<InputStream> input = request.getInputStream();
        if (input.isPresent()) {
            try {
                builder.setInputData(ByteString.readFrom(input.get()));
            } catch (IOException e) {
                throw new CompletionException(ProtonException.of(e, server));
            }
        }

        List<ProtonExternalTable> externalTables = request.getExternalTables();
        if (!externalTables.isEmpty()) {
            for (ProtonExternalTable external : externalTables) {
                ExternalTable.Builder b = ExternalTable.newBuilder().setName(external.getName());
                for (ProtonColumn c : ProtonColumn.parse(external.getStructure())) {
                    b.addColumns(NameAndType.newBuilder().setName(c.getColumnName()).setType(c.getOriginalTypeName())
                            .build());
                }
                if (external.getFormat() != null) {
                    b.setFormat(external.getFormat().name());
                }

                try {
                    builder.addExternalTables(b.setData(ByteString.readFrom(external.getContent())).build());
                } catch (IOException e) {
                    throw new CompletionException(ProtonException.of(e, server));
                }
            }
        }

        List<String> stmts = request.getStatements(false);
        int size = stmts.size();
        String sql;
        if (size == 0) {
            throw new IllegalArgumentException("At least one SQL statement is required for execution");
        } else if (size == 1) {
            sql = stmts.get(0);
        } else { // consolidate statements into one
            if (!builder.getSessionCheck()) {
                builder.setSessionCheck(true);
            }

            if (ProtonChecker.isNullOrEmpty(builder.getSessionId())) {
                builder.setSessionId(UUID.randomUUID().toString());
            }

            // builder.getSessionTimeout()
            StringBuilder sb = new StringBuilder();
            for (String s : stmts) {
                sb.append(s).append(';').append('\n');
            }
            sql = sb.toString();
        }

        log.debug("Query: %s", sql);

        return builder.setQuery(sql).build();
    }

    @Override
    protected void closeConnection(ManagedChannel connection, boolean force) {
        if (!force) {
            connection.shutdown();
        } else {
            connection.shutdownNow();
        }
    }

    @Override
    protected ManagedChannel newConnection(ManagedChannel connection, ProtonNode server,
            ProtonRequest<?> request) {
        if (connection != null) {
            closeConnection(connection, false);
        }

        return ProtonGrpcChannelFactory.getFactory(request.getConfig(), server).create();
    }

    protected void fill(ProtonRequest<?> request, StreamObserver<QueryInfo> observer) {
        try {
            observer.onNext(convert(getServer(), request));
        } finally {
            observer.onCompleted();
        }
    }

    @Override
    public boolean accept(ProtonProtocol protocol) {
        return ProtonProtocol.GRPC == protocol || super.accept(protocol);
    }

    protected CompletableFuture<ProtonResponse> executeAsync(ProtonRequest<?> sealedRequest,
            ManagedChannel channel, ProtonNode server) {
        // reuse stub?
        ProtonGrpc.ProtonStub stub = ProtonGrpc.newStub(channel);
        stub.withCompression(getRequestEncoding(sealedRequest.getConfig()));

        final ProtonStreamObserver responseObserver = new ProtonStreamObserver(sealedRequest.getConfig(),
                server);
        final StreamObserver<QueryInfo> requestObserver = stub.executeQueryWithStreamIO(responseObserver);

        if (sealedRequest.hasInputStream()) {
            getExecutor().execute(() -> fill(sealedRequest, requestObserver));
        } else {
            fill(sealedRequest, requestObserver);
        }

        // return new ProtonGrpcFuture(server, sealedRequest, requestObserver,
        // responseObserver);
        return CompletableFuture.supplyAsync(() -> {
            ProtonConfig config = sealedRequest.getConfig();
            int timeout = config.getConnectionTimeout() / 1000
                    + Math.max(config.getSocketTimeout() / 1000, config.getMaxExecutionTime());
            try {
                if (!responseObserver.await(timeout, TimeUnit.SECONDS)) {
                    if (!Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED))) {
                        requestObserver.onError(new StatusException(Status.CANCELLED));
                    }
                    throw new CompletionException(
                            ProtonUtils.format("Timed out after waiting for %d %s", timeout, TimeUnit.SECONDS),
                            null);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException(ProtonException.of(e, server));
            }

            try {
                ProtonResponse response = new ProtonGrpcResponse(sealedRequest.getConfig(),
                        sealedRequest.getSettings(), responseObserver);
                Throwable cause = responseObserver.getError();
                if (cause != null) {
                    throw new CompletionException(ProtonException.of(cause, server));
                }
                return response;
            } catch (IOException e) {
                throw new CompletionException(ProtonException.of(e, server));
            }
        }, getExecutor());
    }

    protected CompletableFuture<ProtonResponse> executeSync(ProtonRequest<?> sealedRequest,
            ManagedChannel channel, ProtonNode server) {
        ProtonGrpc.ProtonBlockingStub stub = ProtonGrpc.newBlockingStub(channel);
        stub.withCompression(getRequestEncoding(sealedRequest.getConfig()));

        // TODO not as elegant as ProtonImmediateFuture :<
        try {
            Result result = stub.executeQuery(convert(server, sealedRequest));

            ProtonResponse response = new ProtonGrpcResponse(sealedRequest.getConfig(),
                    sealedRequest.getSettings(), result);

            return result.hasException()
                    ? failedResponse(new ProtonException(result.getException().getCode(),
                            result.getException().getDisplayText(), server))
                    : CompletableFuture.completedFuture(response);
        } catch (IOException e) {
            throw new CompletionException(ProtonException.of(e, server));
        }
    }

    @Override
    public CompletableFuture<ProtonResponse> execute(ProtonRequest<?> request) {
        // sealedRequest is an immutable copy of the original request
        final ProtonRequest<?> sealedRequest = request.seal();
        final ManagedChannel c = getConnection(sealedRequest);
        final ProtonNode s = getServer();

        return sealedRequest.getConfig().isAsync() ? executeAsync(sealedRequest, c, s)
                : executeSync(sealedRequest, c, s);
    }

    @Override
    public Class<? extends ProtonOption> getOptionClass() {
        return ProtonGrpcOption.class;
    }
}
