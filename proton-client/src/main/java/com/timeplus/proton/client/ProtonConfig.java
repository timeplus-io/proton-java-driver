package com.timeplus.proton.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;

import com.timeplus.proton.client.config.ProtonClientOption;
import com.timeplus.proton.client.config.ProtonOption;
import com.timeplus.proton.client.config.ProtonDefaults;
import com.timeplus.proton.client.config.ProtonSslMode;

/**
 * An immutable class holding client-specific options like
 * {@link ProtonCredentials} and {@link ProtonNodeSelector} etc.
 */
public class ProtonConfig implements Serializable {
    protected static final Map<ProtonOption, Serializable> mergeOptions(List<ProtonConfig> list) {
        if (list == null || list.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ProtonOption, Serializable> options = new HashMap<>();
        List<ProtonConfig> cl = new ArrayList<>(list.size());
        for (ProtonConfig c : list) {
            if (c != null) {
                boolean duplicated = false;
                for (ProtonConfig conf : cl) {
                    if (conf == c) {
                        duplicated = true;
                        break;
                    }
                }

                if (duplicated) {
                    continue;
                }
                options.putAll(c.options);
                cl.add(c);
            }
        }

        return options;
    }

    protected static final ProtonCredentials mergeCredentials(List<ProtonConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        ProtonCredentials credentials = null;
        for (ProtonConfig c : list) {
            if (c != null && c.credentials != null) {
                credentials = c.credentials;
                break;
            }
        }

        return credentials;
    }

    protected static final ProtonNodeSelector mergeNodeSelector(List<ProtonConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        ProtonNodeSelector nodeSelector = null;
        for (ProtonConfig c : list) {
            if (c != null && c.nodeSelector != null) {
                nodeSelector = c.nodeSelector;
                break;
            }
        }

        return nodeSelector;
    }

    protected static final Object mergeMetricRegistry(List<ProtonConfig> list) {
        if (list == null || list.isEmpty()) {
            return null;
        }

        Object metricRegistry = null;
        for (ProtonConfig c : list) {
            if (c != null && c.metricRegistry.isPresent()) {
                metricRegistry = c.metricRegistry.get();
                break;
            }
        }

        return metricRegistry;
    }

    private static final long serialVersionUID = 7794222888859182491L;

    // common options optimized for read
    private final boolean async;
    private final String clientName;
    private final boolean compressServerResponse;
    private final ProtonCompression compressAlgorithm;
    private final int compressLevel;
    private final boolean decompressClientRequest;
    private final ProtonCompression decompressAlgorithm;
    private final int decompressLevel;
    private final int connectionTimeout;
    private final String database;
    private final ProtonFormat format;
    private final int maxBufferSize;
    private final int maxExecutionTime;
    private final int maxQueuedBuffers;
    private final int maxQueuedRequests;
    private final long maxResultRows;
    private final int maxThreads;
    private final boolean retry;
    private final boolean reuseValueWrapper;
    private final boolean serverInfo;
    private final TimeZone serverTimeZone;
    private final ProtonVersion serverVersion;
    private final int sessionTimeout;
    private final boolean sessionCheck;
    private final int socketTimeout;
    private final boolean ssl;
    private final ProtonSslMode sslMode;
    private final String sslRootCert;
    private final String sslCert;
    private final String sslKey;
    private final boolean useObjectsInArray;
    private final boolean useServerTimeZone;
    private final boolean useServerTimeZoneForDates;
    private final TimeZone timeZoneForDate;
    private final TimeZone useTimeZone;

    // client specific options
    private final Map<ProtonOption, Serializable> options;
    private final ProtonCredentials credentials;
    private final transient Optional<Object> metricRegistry;

    // node selector - pick only interested nodes from given list
    private final ProtonNodeSelector nodeSelector;

    /**
     * Construct a new configuration by consolidating given ones.
     *
     * @param configs list of configuration
     */
    public ProtonConfig(ProtonConfig... configs) {
        this(configs == null || configs.length == 0 ? Collections.emptyList() : Arrays.asList(configs));
    }

    /**
     * Construct a new configuration by consolidating given ones.
     *
     * @param configs list of configuration
     */
    public ProtonConfig(List<ProtonConfig> configs) {
        this(mergeOptions(configs), mergeCredentials(configs), mergeNodeSelector(configs),
                mergeMetricRegistry(configs));
    }

    /**
     * Default contructor.
     *
     * @param options        generic options
     * @param credentials    default credential
     * @param nodeSelector   node selector
     * @param metricRegistry metric registry
     */
    public ProtonConfig(Map<ProtonOption, Serializable> options, ProtonCredentials credentials,
            ProtonNodeSelector nodeSelector, Object metricRegistry) {
        this.options = new HashMap<>();
        if (options != null) {
            this.options.putAll(options);
        }

        this.async = (boolean) getOption(ProtonClientOption.ASYNC, ProtonDefaults.ASYNC);
        this.clientName = (String) getOption(ProtonClientOption.CLIENT_NAME);
        this.compressServerResponse = (boolean) getOption(ProtonClientOption.COMPRESS);
        this.compressAlgorithm = (ProtonCompression) getOption(ProtonClientOption.COMPRESS_ALGORITHM);
        this.compressLevel = (int) getOption(ProtonClientOption.COMPRESS_LEVEL);
        this.decompressClientRequest = (boolean) getOption(ProtonClientOption.DECOMPRESS);
        this.decompressAlgorithm = (ProtonCompression) getOption(ProtonClientOption.DECOMPRESS_ALGORITHM);
        this.decompressLevel = (int) getOption(ProtonClientOption.DECOMPRESS_LEVEL);
        this.connectionTimeout = (int) getOption(ProtonClientOption.CONNECTION_TIMEOUT);
        this.database = (String) getOption(ProtonClientOption.DATABASE, ProtonDefaults.DATABASE);
        this.format = (ProtonFormat) getOption(ProtonClientOption.FORMAT, ProtonDefaults.FORMAT);
        this.maxBufferSize = (int) getOption(ProtonClientOption.MAX_BUFFER_SIZE);
        this.maxExecutionTime = (int) getOption(ProtonClientOption.MAX_EXECUTION_TIME);
        this.maxQueuedBuffers = (int) getOption(ProtonClientOption.MAX_QUEUED_BUFFERS);
        this.maxQueuedRequests = (int) getOption(ProtonClientOption.MAX_QUEUED_REQUESTS);
        this.maxResultRows = (long) getOption(ProtonClientOption.MAX_RESULT_ROWS);
        this.maxThreads = (int) getOption(ProtonClientOption.MAX_THREADS_PER_CLIENT);
        this.retry = (boolean) getOption(ProtonClientOption.RETRY);
        this.reuseValueWrapper = (boolean) getOption(ProtonClientOption.REUSE_VALUE_WRAPPER);
        this.serverInfo = !ProtonChecker.isNullOrBlank((String) getOption(ProtonClientOption.SERVER_TIME_ZONE))
                && !ProtonChecker.isNullOrBlank((String) getOption(ProtonClientOption.SERVER_VERSION));
        this.serverTimeZone = TimeZone.getTimeZone(
                (String) getOption(ProtonClientOption.SERVER_TIME_ZONE, ProtonDefaults.SERVER_TIME_ZONE));
        this.serverVersion = ProtonVersion
                .of((String) getOption(ProtonClientOption.SERVER_VERSION, ProtonDefaults.SERVER_VERSION));
        this.sessionTimeout = (int) getOption(ProtonClientOption.SESSION_TIMEOUT);
        this.sessionCheck = (boolean) getOption(ProtonClientOption.SESSION_CHECK);
        this.socketTimeout = (int) getOption(ProtonClientOption.SOCKET_TIMEOUT);
        this.ssl = (boolean) getOption(ProtonClientOption.SSL);
        this.sslMode = (ProtonSslMode) getOption(ProtonClientOption.SSL_MODE);
        this.sslRootCert = (String) getOption(ProtonClientOption.SSL_ROOT_CERTIFICATE);
        this.sslCert = (String) getOption(ProtonClientOption.SSL_CERTIFICATE);
        this.sslKey = (String) getOption(ProtonClientOption.SSL_KEY);
        this.useObjectsInArray = (boolean) getOption(ProtonClientOption.USE_OBJECTS_IN_ARRAYS);
        this.useServerTimeZone = (boolean) getOption(ProtonClientOption.USE_SERVER_TIME_ZONE);
        this.useServerTimeZoneForDates = (boolean) getOption(ProtonClientOption.USE_SERVER_TIME_ZONE_FOR_DATES);

        String timeZone = (String) getOption(ProtonClientOption.USE_TIME_ZONE);
        TimeZone tz = ProtonChecker.isNullOrBlank(timeZone) ? TimeZone.getDefault()
                : TimeZone.getTimeZone(timeZone);
        this.useTimeZone = this.useServerTimeZone ? this.serverTimeZone : tz;
        this.timeZoneForDate = this.useServerTimeZoneForDates ? this.useTimeZone : null;

        if (credentials == null) {
            this.credentials = ProtonCredentials.fromUserAndPassword((String) getOption(ProtonDefaults.USER),
                    (String) getOption(ProtonDefaults.PASSWORD));
        } else {
            this.credentials = credentials;
        }
        this.metricRegistry = Optional.ofNullable(metricRegistry);
        this.nodeSelector = nodeSelector == null ? ProtonNodeSelector.EMPTY : nodeSelector;
    }

    public boolean isAsync() {
        return async;
    }

    public String getClientName() {
        return clientName;
    }

    public boolean isCompressServerResponse() {
        return compressServerResponse;
    }

    public ProtonCompression getCompressAlgorithmForServerResponse() {
        return compressAlgorithm;
    }

    public int getCompressLevelForServerResponse() {
        return compressLevel;
    }

    public boolean isDecompressClientRequet() {
        return decompressClientRequest;
    }

    public ProtonCompression getDecompressAlgorithmForClientRequest() {
        return decompressAlgorithm;
    }

    public int getDecompressLevelForClientRequest() {
        return decompressLevel;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public String getDatabase() {
        return database;
    }

    public ProtonFormat getFormat() {
        return format;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public int getMaxQueuedBuffers() {
        return maxQueuedBuffers;
    }

    public int getMaxQueuedRequests() {
        return maxQueuedRequests;
    }

    public long getMaxResultRows() {
        return maxResultRows;
    }

    public int getMaxThreadsPerClient() {
        return maxThreads;
    }

    public boolean isRetry() {
        return retry;
    }

    public boolean isReuseValueWrapper() {
        return reuseValueWrapper;
    }

    /**
     * Checks whether we got all server information(e.g. timezone and version).
     *
     * @return true if we got all server information; false otherwise
     */
    public boolean hasServerInfo() {
        return serverInfo;
    }

    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    public ProtonVersion getServerVersion() {
        return serverVersion;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public boolean isSessionCheck() {
        return sessionCheck;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public boolean isSsl() {
        return ssl;
    }

    public ProtonSslMode getSslMode() {
        return sslMode;
    }

    public String getSslRootCert() {
        return sslRootCert;
    }

    public String getSslCert() {
        return sslCert;
    }

    public String getSslKey() {
        return sslKey;
    }

    public boolean isUseObjectsInArray() {
        return useObjectsInArray;
    }

    public boolean isUseServerTimeZone() {
        return useServerTimeZone;
    }

    public boolean isUseServerTimeZoneForDates() {
        return useServerTimeZoneForDates;
    }

    /**
     * Gets time zone for date values.
     *
     * @return time zone, could be null when {@code use_server_time_zone_for_date}
     *         is set to {@code false}.
     */
    public TimeZone getTimeZoneForDate() {
        return timeZoneForDate;
    }

    /**
     * Gets preferred time zone. When {@link #isUseServerTimeZone()} is
     * {@code true}, this returns same time zone as {@link #getServerTimeZone()}.
     *
     * @return non-null preferred time zone
     */
    public TimeZone getUseTimeZone() {
        return useTimeZone;
    }

    public ProtonCredentials getDefaultCredentials() {
        return this.credentials;
    }

    public Optional<Object> getMetricRegistry() {
        return this.metricRegistry;
    }

    public ProtonNodeSelector getNodeSelector() {
        return this.nodeSelector;
    }

    public List<ProtonProtocol> getPreferredProtocols() {
        return this.nodeSelector.getPreferredProtocols();
    }

    public Set<String> getPreferredTags() {
        return this.nodeSelector.getPreferredTags();
    }

    public Map<ProtonOption, Serializable> getAllOptions() {
        return Collections.unmodifiableMap(this.options);
    }

    public Serializable getOption(ProtonOption option) {
        return getOption(option, null);
    }

    public Serializable getOption(ProtonOption option, ProtonDefaults defaultValue) {
        return this.options.getOrDefault(ProtonChecker.nonNull(option, "option"),
                defaultValue == null ? option.getEffectiveDefaultValue() : defaultValue.getEffectiveDefaultValue());
    }

    /**
     * Test whether a given option is configured or not.
     *
     * @param option option to test
     * @return true if the option is configured; false otherwise
     */
    public boolean hasOption(ProtonOption option) {
        return option != null && this.options.containsKey(option);
    }
}
