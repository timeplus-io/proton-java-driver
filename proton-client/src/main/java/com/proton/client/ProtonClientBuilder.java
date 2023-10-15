package com.proton.client;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ExecutorService;

import com.proton.client.config.ProtonOption;
import com.proton.client.config.ProtonDefaults;

/**
 * Builder class for creating {@link ProtonClient}. Please use
 * {@link ProtonClient#builder()} for instantiation, and avoid
 * multi-threading as it's NOT thread-safe.
 */
public class ProtonClientBuilder {
    // expose method to change default thread pool in runtime? JMX?
    static final ExecutorService defaultExecutor;

    static {
        int maxThreads = (int) ProtonDefaults.MAX_THREADS.getEffectiveDefaultValue();
        int maxRequests = (int) ProtonDefaults.MAX_REQUESTS.getEffectiveDefaultValue();
        long keepAliveTimeoutMs = (long) ProtonDefaults.THREAD_KEEPALIVE_TIMEOUT.getEffectiveDefaultValue();

        if (maxThreads <= 0) {
            maxThreads = Runtime.getRuntime().availableProcessors();
        }
        if (maxRequests <= 0) {
            maxRequests = 0;
        }

        defaultExecutor = ProtonUtils.newThreadPool(ProtonClient.class.getSimpleName(), maxThreads,
                maxThreads * 2, maxRequests, keepAliveTimeoutMs, false);
    }

    protected ProtonConfig config;

    protected ProtonCredentials credentials;
    protected Object metricRegistry;
    protected ProtonNodeSelector nodeSelector;

    protected final Map<ProtonOption, Serializable> options;

    /**
     * Default constructor.
     */
    protected ProtonClientBuilder() {
        options = new HashMap<>();
    }

    /**
     * Resets client configuration to null.
     */
    protected void resetConfig() {
        if (config != null) {
            config = null;
        }
    }

    /**
     * Gets client configuration.
     *
     * @return non-null client configuration
     */
    public ProtonConfig getConfig() {
        if (config == null) {
            config = new ProtonConfig(options, credentials, nodeSelector, metricRegistry);
        }

        return config;
    }

    /**
     * Builds an instance of {@link ProtonClient}. This method will use
     * {@link java.util.ServiceLoader} to load a suitable implementation based on
     * preferred protocol(s), or just the first one if no preference given.
     * {@link ProtonClient#accept(ProtonProtocol)} will be invoked during
     * the process to test if the implementation is compatible with the preferred
     * protocol(s) or not. At the end of process, if a suitable implementation is
     * found, {@link ProtonClient#init(ProtonConfig)} will be invoked for
     * initialization.
     *
     * @return suitable client to handle preferred protocols
     * @throws IllegalStateException when no suitable client found in classpath
     */
    public ProtonClient build() {
        ProtonClient client = null;

        boolean noSelector = nodeSelector == null || nodeSelector == ProtonNodeSelector.EMPTY;
        int counter = 0;
        for (ProtonClient c : ServiceLoader.load(ProtonClient.class, getClass().getClassLoader())) {
            counter++;
            if (noSelector || nodeSelector.match(c)) {
                client = c;
                break;
            }
        }

        if (client == null) {
            throw new IllegalStateException(
                    ProtonUtils.format("No suitable Proton client(out of %d) found in classpath.", counter));
        } else {
            client.init(getConfig());
        }

        return client;
    }

    /**
     * Sets configuration.
     *
     * @param config non-null configuration
     * @return this builder
     */
    public ProtonClientBuilder config(ProtonConfig config) {
        this.config = config;

        this.credentials = config.getDefaultCredentials();
        this.metricRegistry = config.getMetricRegistry().orElse(null);
        this.nodeSelector = config.getNodeSelector();

        this.options.putAll(config.getAllOptions());

        return this;
    }

    /**
     * Adds an option, which is usually an Enum type that implements
     * {@link com.proton.client.config.ProtonOption}.
     *
     * @param option non-null option
     * @param value  value
     * @return this builder
     */
    public ProtonClientBuilder option(ProtonOption option, Serializable value) {
        if (option == null || value == null) {
            throw new IllegalArgumentException("Non-null option and value are required");
        }
        Object oldValue = options.put(option, value);
        if (oldValue == null || !value.equals(oldValue)) {
            resetConfig();
        }

        return this;
    }

    /**
     * Removes an option.
     *
     * @param option non-null option
     * @return this builder
     */
    public ProtonClientBuilder removeOption(ProtonOption option) {
        Object value = options.remove(ProtonChecker.nonNull(option, "option"));
        if (value != null) {
            resetConfig();
        }

        return this;
    }

    /**
     * Sets options.
     *
     * @param options map containing all options
     * @return this builder
     */
    public ProtonClientBuilder options(Map<ProtonOption, Serializable> options) {
        if (options != null && !options.isEmpty()) {
            this.options.putAll(options);
            resetConfig();
        }

        return this;
    }

    /*
     * public ProtonClientBuilder addUserType(Object... userTypeMappers) {
     * resetConfig(); return this; }
     */

    /**
     * Sets default credentials, which will be used to connect to a
     * {@link ProtonNode} only when it has no credentials defined.
     *
     * @param credentials default credentials
     * @return this builder
     */
    public ProtonClientBuilder defaultCredentials(ProtonCredentials credentials) {
        if (!ProtonChecker.nonNull(credentials, "credentials").equals(this.credentials)) {
            this.credentials = credentials;
            resetConfig();
        }

        return this;
    }

    /*
     * public ProtonClientBuilder databaseChangeListener(@NonNull Object
     * listener) { resetConfig(); return this; }
     */

    /**
     * Sets node selector.
     *
     * @param nodeSelector non-null node selector
     * @return this builder
     */
    public ProtonClientBuilder nodeSelector(ProtonNodeSelector nodeSelector) {
        if (!ProtonChecker.nonNull(nodeSelector, "nodeSelector").equals(this.nodeSelector)) {
            this.nodeSelector = nodeSelector;
            resetConfig();
        }

        return this;
    }

    /**
     * Sets metric registry.
     *
     * @param metricRegistry metric registry, could be null
     * @return this builder
     */
    public ProtonClientBuilder metricRegistry(Object metricRegistry) {
        if (!Objects.equals(this.metricRegistry, metricRegistry)) {
            this.metricRegistry = metricRegistry;
            resetConfig();
        }

        return this;
    }
}
