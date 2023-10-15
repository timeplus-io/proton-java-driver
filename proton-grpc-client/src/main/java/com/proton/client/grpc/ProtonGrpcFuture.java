package com.proton.client.grpc;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonUtils;
import com.proton.client.grpc.impl.QueryInfo;

@Deprecated
public class ProtonGrpcFuture implements Future<ProtonResponse> {
    private final ProtonNode server;
    private final ProtonRequest<?> request;

    private final StreamObserver<QueryInfo> requestObserver;
    private final ProtonStreamObserver responseObserver;

    protected ProtonGrpcFuture(ProtonNode server, ProtonRequest<?> request,
            StreamObserver<QueryInfo> requestObserver, ProtonStreamObserver responseObserver) {
        this.server = ProtonChecker.nonNull(server, "server");
        this.request = ProtonChecker.nonNull(request, "request").seal();

        this.requestObserver = ProtonChecker.nonNull(requestObserver, "requestObserver");
        this.responseObserver = ProtonChecker.nonNull(responseObserver, "responseObserver");
    }

    public ProtonNode getServer() {
        return server;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = true;

        if (mayInterruptIfRunning) {
            cancelled = Context.current().withCancellation().cancel(new StatusException(Status.CANCELLED));
        } else {
            requestObserver.onError(new StatusException(Status.CANCELLED));
        }

        return cancelled;
    }

    @Override
    public boolean isCancelled() {
        return responseObserver.isCancelled();
    }

    @Override
    public boolean isDone() {
        return responseObserver.isCompleted();
    }

    @Override
    public ProtonResponse get() throws InterruptedException, ExecutionException {
        try {
            ProtonConfig config = request.getConfig();
            return get(
                    config.getConnectionTimeout() / 1000
                            + Math.max(config.getSocketTimeout() / 1000, config.getMaxExecutionTime()),
                    TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            cancel(true);
            throw new InterruptedException(e.getMessage());
        }
    }

    @Override
    public ProtonResponse get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (!responseObserver.await(timeout, unit)) {
            cancel(true);
            throw new TimeoutException(ProtonUtils.format("Timed out after waiting for %d %s", timeout, unit));
        }

        try {
            return new ProtonGrpcResponse(request.getConfig(), request.getSettings(), responseObserver);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }
}
