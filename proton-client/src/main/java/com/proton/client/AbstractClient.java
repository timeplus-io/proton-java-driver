package com.proton.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

/**
 * Base class for implementing a thread-safe Proton client. It uses
 * {@link ReadWriteLock} to manage access to underlying connection.
 */
public abstract class AbstractClient<T> implements ProtonClient {
    private static final Logger log = LoggerFactory.getLogger(AbstractClient.class);

    private boolean initialized = false;

    private ExecutorService executor = null;
    private ProtonConfig config = null;
    private ProtonNode server = null;
    private T connection = null;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    private void ensureInitialized() {
        if (!initialized) {
            throw new IllegalStateException("Please initialize the client first");
        }
    }

    // just for testing purpose
    final boolean isInitialized() {
        return initialized;
    }

    protected CompletableFuture<ProtonResponse> failedResponse(Throwable ex) {
        CompletableFuture<ProtonResponse> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }

    /**
     * Gets executor service for this client.
     *
     * @return executor service
     * @throws IllegalStateException when the client is either closed or not
     *                               initialized
     */
    protected final ExecutorService getExecutor() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return executor;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets current server.
     *
     * @return current server
     * @throws IllegalStateException when the client is either closed or not
     *                               initialized
     */
    protected final ProtonNode getServer() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return server;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if the underlying connection can be reused. In general, new connection
     * will be created when {@code connection} is null or {@code requestServer} is
     * different from {@code currentServer} - the existing connection will be closed
     * in the later case.
     *
     * @param connection    existing connection which may or may not be null
     * @param requestServer non-null requested server, returned from previous call
     *                      of {@code request.getServer()}
     * @param currentServer current server, same as {@code getServer()}
     * @param request       non-null request
     * @return true if the connection should NOT be changed(e.g. requestServer is
     *         same as currentServer); false otherwise
     */
    protected boolean checkConnection(T connection, ProtonNode requestServer, ProtonNode currentServer,
            ProtonRequest<?> request) {
        return connection != null && requestServer.equals(currentServer);
    }

    /**
     * Creates a new connection and optionally close existing connection. This
     * method will be called from {@link #getConnection(ProtonRequest)} as
     * needed.
     *
     * @param connection existing connection which may or may not be null
     * @param server     non-null requested server, returned from previous call of
     *                   {@code request.getServer()}
     * @param request    non-null request
     * @return new connection
     * @throws CompletionException when error occured
     */
    protected abstract T newConnection(T connection, ProtonNode server, ProtonRequest<?> request);

    /**
     * Closes a connection. This method will be called from {@link #close()}.
     *
     * @param connection connection to close
     * @param force      whether force to close the connection or not
     */
    protected abstract void closeConnection(T connection, boolean force);

    /**
     * Gets a connection according to the given request.
     *
     * @param request non-null request
     * @return non-null connection
     * @throws CompletionException when error occured
     */
    protected final T getConnection(ProtonRequest<?> request) {
        ProtonNode newNode = ProtonChecker.nonNull(request, "request").getServer();
        lock.readLock().lock();
        try {
            ensureInitialized();
            if (checkConnection(connection, newNode, server, request)) {
                return connection;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            server = newNode;
            log.debug("Connecting to: %s", newNode);
            connection = newConnection(connection, server, request);
            log.debug("Connection established: %s", connection);

            return connection;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public final ProtonConfig getConfig() {
        lock.readLock().lock();
        try {
            ensureInitialized();
            return config;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void init(ProtonConfig config) {
        ProtonChecker.nonNull(config, "config");

        lock.writeLock().lock();
        try {
            this.config = config;
            if (this.executor == null) { // only initialize once
                int threads = config.getMaxThreadsPerClient();
                this.executor = threads < 1 ? ProtonClient.getExecutorService()
                        : ProtonUtils.newThreadPool(this, threads, config.getMaxQueuedRequests());
            }

            initialized = true;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public final void close() {
        lock.readLock().lock();
        try {
            if (!initialized) {
                return;
            }
        } finally {
            lock.readLock().unlock();
        }

        lock.writeLock().lock();
        try {
            server = null;

            if (connection != null) {
                closeConnection(connection, false);
                connection = null;
            }

            // avoid shutting down shared thread pool
            if (executor != null && config.getMaxThreadsPerClient() > 0 && !executor.isTerminated()) {
                executor.shutdown();
            }
            executor = null;
        } catch (Exception e) {
            log.warn("Exception occurred when closing client", e);
        } finally {
            initialized = false;
            try {
                if (connection != null) {
                    closeConnection(connection, true);
                }

                if (executor != null && config.getMaxThreadsPerClient() > 0) {
                    executor.shutdownNow();
                }
            } finally {
                executor = null;
                connection = null;
                lock.writeLock().unlock();
            }
        }
    }
}
