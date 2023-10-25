package com.timeplus.proton.settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonProtocol;

public class ProtonProperties {

    // connection settings
    private boolean async;
    private int bufferSize;
    private int apacheBufferSize;
    private int socketTimeout;
    private int connectionTimeout;
    private int dataTransferTimeout;
    private int timeToLiveMillis;
    private int validateAfterInactivityMillis;
    private int defaultMaxPerRoute;
    private int maxTotal;
    private int maxRetries;
    private String host;
    private int port;
    private ProtonProtocol protocol;
    private boolean usePathAsDb;
    private String path;
    private boolean ssl;
    private String sslRootCertificate;
    private String sslMode;

    /**
     * Maximum number of allowed redirects. Active only when {@link ProtonProperties#checkForRedirects}
     * is <code>true</code>
     */
    private int maxRedirects;

    /**
     * If set to <code>true</code>, driver will first try to connect to the server using GET request. If the response is 307,
     * it will use URI given in the response's Location header instead of the original one.
     * <p>
     * Those queries will be repeated until response is anything other than 307, or until
     * {@link ProtonProperties#maxRedirects maxRedirects} is hit.
     * <p>
     * This is a workaround to issues with properly following HTTP POST redirects.
     * Namely, Apache HTTP client's inability to process early responses, and difficulties with resending non-repeatable
     * {@link org.apache.http.entity.InputStreamEntity InputStreamEntity}
     */
    private boolean checkForRedirects;
    //additional
    private int maxCompressBufferSize;

    private boolean useServerTimeZone;
    private String useTimeZone;
    private boolean useServerTimeZoneForDates;
    private boolean useObjectsInArrays;
    // the shared cookie store is scoped to a database
    private boolean useSharedCookieStore;

    // queries settings
    private Integer maxParallelReplicas;
    private Integer maxPartitionsPerInsertBlock;
    private String  totalsMode;
    private String  quotaKey;
    private Integer priority;
    private String  database;
    private boolean compress;
    private boolean decompress;
    private boolean extremes;
    private Integer maxThreads;
    private Integer maxExecutionTime;
    private Integer maxBlockSize;
    private Integer maxRowsToGroupBy;
    private String  profile;
    private String  user;
    private String  password;
    private String  httpAuthorization;
    private boolean distributedAggregationMemoryEfficient;
    private Long    maxBytesBeforeExternalGroupBy;
    private Long    maxBytesBeforeExternalSort;
    private Long    maxMemoryUsage;
    private Long    maxMemoryUsageForUser;
    private Long    maxMemoryUsageForAllQueries;
    private Long    preferredBlockSizeBytes;
    private Long    maxQuerySize;
    private Long    maxAstElements;
    private boolean sessionCheck;
    private String  sessionId;
    private Long    sessionTimeout;
    private Long    insertQuorum;
    private Long    insertQuorumTimeout;
    private Long    selectSequentialConsistency;
    private Boolean enableOptimizePredicateExpression;
    private Long    maxInsertBlockSize;
    private Boolean insertDeduplicate;
    private Boolean insertDistributedSync;
    private Boolean anyJoinDistinctRightTableKeys;
    private Boolean sendProgressInHttpHeaders;
    private Boolean waitEndOfQuery;
    private String  clientName;

    public ProtonProperties() {
        this(new Properties());
    }

    public ProtonProperties(Properties info) {
        // need casts for java 6
        this.async = (Boolean)getSetting(info, ProtonConnectionSettings.ASYNC);
        this.bufferSize = (Integer)getSetting(info, ProtonConnectionSettings.BUFFER_SIZE);
        this.apacheBufferSize = (Integer)getSetting(info, ProtonConnectionSettings.APACHE_BUFFER_SIZE);
        this.socketTimeout = (Integer)getSetting(info, ProtonConnectionSettings.SOCKET_TIMEOUT);
        this.connectionTimeout = (Integer)getSetting(info, ProtonConnectionSettings.CONNECTION_TIMEOUT);
        this.dataTransferTimeout = (Integer)getSetting(info, ProtonConnectionSettings.DATA_TRANSFER_TIMEOUT);
        this.timeToLiveMillis = (Integer)getSetting(info, ProtonConnectionSettings.TIME_TO_LIVE_MILLIS);
        this.validateAfterInactivityMillis = (Integer)getSetting(info, ProtonConnectionSettings.VALIDATE_AFTER_INACTIVITY_MILLIS);
        this.defaultMaxPerRoute = (Integer)getSetting(info, ProtonConnectionSettings.DEFAULT_MAX_PER_ROUTE);
        this.maxTotal = (Integer)getSetting(info, ProtonConnectionSettings.MAX_TOTAL);
        this.maxRetries = (Integer)getSetting(info, ProtonConnectionSettings.MAX_RETRIES);
        this.maxCompressBufferSize = (Integer) getSetting(info, ProtonConnectionSettings.MAX_COMPRESS_BUFFER_SIZE);
        this.ssl = (Boolean) getSetting(info, ProtonConnectionSettings.SSL);
        this.sslRootCertificate = (String) getSetting(info, ProtonConnectionSettings.SSL_ROOT_CERTIFICATE);
        this.sslMode = (String) getSetting(info, ProtonConnectionSettings.SSL_MODE);
        this.usePathAsDb = (Boolean) getSetting(info, ProtonConnectionSettings.USE_PATH_AS_DB);
        this.path = (String) getSetting(info, ProtonConnectionSettings.PATH);
        this.protocol = ProtonProtocol.valueOf(((String) getSetting(info, ProtonConnectionSettings.PROTOCOL)).toUpperCase());
        this.maxRedirects = (Integer) getSetting(info, ProtonConnectionSettings.MAX_REDIRECTS);
        this.checkForRedirects = (Boolean) getSetting(info, ProtonConnectionSettings.CHECK_FOR_REDIRECTS);
        this.useServerTimeZone = (Boolean)getSetting(info, ProtonConnectionSettings.USE_SERVER_TIME_ZONE);
        this.useTimeZone = (String)getSetting(info, ProtonConnectionSettings.USE_TIME_ZONE);
        this.useServerTimeZoneForDates = (Boolean)getSetting(info, ProtonConnectionSettings.USE_SERVER_TIME_ZONE_FOR_DATES);
        this.useObjectsInArrays = (Boolean)getSetting(info, ProtonConnectionSettings.USE_OBJECTS_IN_ARRAYS);
        this.useSharedCookieStore = (Boolean)getSetting(info, ProtonConnectionSettings.USE_SHARED_COOKIE_STORE);
        this.clientName = (String)getSetting(info, ProtonConnectionSettings.CLIENT_NAME);

        this.maxParallelReplicas = getSetting(info, ProtonQueryParam.MAX_PARALLEL_REPLICAS);
        this.maxPartitionsPerInsertBlock = getSetting(info, ProtonQueryParam.MAX_PARTITIONS_PER_INSERT_BLOCK);
        this.totalsMode = getSetting(info, ProtonQueryParam.TOTALS_MODE);
        this.quotaKey = getSetting(info, ProtonQueryParam.QUOTA_KEY);
        this.priority = getSetting(info, ProtonQueryParam.PRIORITY);
        this.database = getSetting(info, ProtonQueryParam.DATABASE);
        this.protocol = ProtonProtocol.valueOf(((String)getSetting(info, ProtonConnectionSettings.PROTOCOL)).toUpperCase());
        this.compress = (Boolean)getSetting(info, ProtonQueryParam.COMPRESS);
        this.decompress = (Boolean)getSetting(info, ProtonQueryParam.DECOMPRESS);
        this.extremes = (Boolean)getSetting(info, ProtonQueryParam.EXTREMES);
        this.maxThreads = getSetting(info, ProtonQueryParam.MAX_THREADS);
        this.maxExecutionTime = getSetting(info, ProtonQueryParam.MAX_EXECUTION_TIME);
        this.maxBlockSize = getSetting(info, ProtonQueryParam.MAX_BLOCK_SIZE);
        this.maxRowsToGroupBy = getSetting(info, ProtonQueryParam.MAX_ROWS_TO_GROUP_BY);
        this.profile = getSetting(info, ProtonQueryParam.PROFILE);
        this.user = getSetting(info, ProtonQueryParam.USER);
        this.password = getSetting(info, ProtonQueryParam.PASSWORD);
        this.httpAuthorization = getSetting(info, ProtonQueryParam.AUTHORIZATION);
        this.distributedAggregationMemoryEfficient = (Boolean)getSetting(info, ProtonQueryParam.DISTRIBUTED_AGGREGATION_MEMORY_EFFICIENT);
        this.maxBytesBeforeExternalGroupBy = (Long)getSetting(info, ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY);
        this.maxBytesBeforeExternalSort = (Long)getSetting(info, ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_SORT);
        this.maxMemoryUsage = getSetting(info, ProtonQueryParam.MAX_MEMORY_USAGE);
        this.maxMemoryUsageForUser = getSetting(info, ProtonQueryParam.MAX_MEMORY_USAGE_FOR_USER);
        this.maxMemoryUsageForAllQueries = getSetting(info, ProtonQueryParam.MAX_MEMORY_USAGE_FOR_ALL_QUERIES);
        this.preferredBlockSizeBytes = getSetting(info, ProtonQueryParam.PREFERRED_BLOCK_SIZE_BYTES);
        this.maxQuerySize = getSetting(info, ProtonQueryParam.MAX_QUERY_SIZE);
        this.maxAstElements = getSetting(info, ProtonQueryParam.MAX_AST_ELEMENTS);
        this.sessionCheck = (Boolean) getSetting(info, ProtonQueryParam.SESSION_CHECK);
        this.sessionId = getSetting(info, ProtonQueryParam.SESSION_ID);
        this.sessionTimeout = getSetting(info, ProtonQueryParam.SESSION_TIMEOUT);
        this.insertQuorum = (Long)getSetting(info, ProtonQueryParam.INSERT_QUORUM);
        this.insertQuorumTimeout = (Long)getSetting(info, ProtonQueryParam.INSERT_QUORUM_TIMEOUT);
        this.selectSequentialConsistency = (Long)getSetting(info, ProtonQueryParam.SELECT_SEQUENTIAL_CONSISTENCY);
        this.enableOptimizePredicateExpression = getSetting(info, ProtonQueryParam.ENABLE_OPTIMIZE_PREDICATE_EXPRESSION);
        this.maxInsertBlockSize = getSetting(info, ProtonQueryParam.MAX_INSERT_BLOCK_SIZE);
        this.insertDeduplicate = getSetting(info, ProtonQueryParam.INSERT_DEDUPLICATE);
        this.insertDistributedSync = getSetting(info, ProtonQueryParam.INSERT_DISTRIBUTED_SYNC);
        this.anyJoinDistinctRightTableKeys = getSetting(info, ProtonQueryParam.ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS);
        this.sendProgressInHttpHeaders = (Boolean)getSetting(info, ProtonQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS);
        this.waitEndOfQuery = (Boolean)getSetting(info, ProtonQueryParam.WAIT_END_OF_QUERY);
    }

    public Properties asProperties() {
        PropertiesBuilder ret = new PropertiesBuilder();
        ret.put(ProtonConnectionSettings.ASYNC.getKey(), String.valueOf(async));
        ret.put(ProtonConnectionSettings.BUFFER_SIZE.getKey(), String.valueOf(bufferSize));
        ret.put(ProtonConnectionSettings.APACHE_BUFFER_SIZE.getKey(), String.valueOf(apacheBufferSize));
        ret.put(ProtonConnectionSettings.SOCKET_TIMEOUT.getKey(), String.valueOf(socketTimeout));
        ret.put(ProtonConnectionSettings.CONNECTION_TIMEOUT.getKey(), String.valueOf(connectionTimeout));
        ret.put(ProtonConnectionSettings.DATA_TRANSFER_TIMEOUT.getKey(), String.valueOf(dataTransferTimeout));
        ret.put(ProtonConnectionSettings.TIME_TO_LIVE_MILLIS.getKey(), String.valueOf(timeToLiveMillis));
        ret.put(ProtonConnectionSettings.VALIDATE_AFTER_INACTIVITY_MILLIS.getKey(), String.valueOf(validateAfterInactivityMillis));
        ret.put(ProtonConnectionSettings.DEFAULT_MAX_PER_ROUTE.getKey(), String.valueOf(defaultMaxPerRoute));
        ret.put(ProtonConnectionSettings.MAX_TOTAL.getKey(), String.valueOf(maxTotal));
        ret.put(ProtonConnectionSettings.MAX_RETRIES.getKey(), String.valueOf(maxRetries));
        ret.put(ProtonConnectionSettings.MAX_COMPRESS_BUFFER_SIZE.getKey(), String.valueOf(maxCompressBufferSize));
        ret.put(ProtonConnectionSettings.SSL.getKey(), String.valueOf(ssl));
        ret.put(ProtonConnectionSettings.SSL_ROOT_CERTIFICATE.getKey(), String.valueOf(sslRootCertificate));
        ret.put(ProtonConnectionSettings.SSL_MODE.getKey(), String.valueOf(sslMode));
        ret.put(ProtonConnectionSettings.USE_PATH_AS_DB.getKey(), String.valueOf(usePathAsDb));
        ret.put(ProtonConnectionSettings.PATH.getKey(), String.valueOf(path));
        ret.put(ProtonConnectionSettings.PROTOCOL.getKey(), String.valueOf(protocol.name().toLowerCase()));
        ret.put(ProtonConnectionSettings.MAX_REDIRECTS.getKey(), String.valueOf(maxRedirects));
        ret.put(ProtonConnectionSettings.CHECK_FOR_REDIRECTS.getKey(), String.valueOf(checkForRedirects));
        ret.put(ProtonConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), String.valueOf(useServerTimeZone));
        ret.put(ProtonConnectionSettings.USE_TIME_ZONE.getKey(), String.valueOf(useTimeZone));
        ret.put(ProtonConnectionSettings.USE_SERVER_TIME_ZONE_FOR_DATES.getKey(), String.valueOf(useServerTimeZoneForDates));
        ret.put(ProtonConnectionSettings.USE_OBJECTS_IN_ARRAYS.getKey(), String.valueOf(useObjectsInArrays));
        ret.put(ProtonConnectionSettings.USE_SHARED_COOKIE_STORE.getKey(), String.valueOf(useSharedCookieStore));
        ret.put(ProtonConnectionSettings.CLIENT_NAME.getKey(), String.valueOf(clientName));
        
        ret.put(ProtonQueryParam.MAX_PARALLEL_REPLICAS.getKey(), maxParallelReplicas);
        ret.put(ProtonQueryParam.MAX_PARTITIONS_PER_INSERT_BLOCK.getKey(), maxPartitionsPerInsertBlock);
        ret.put(ProtonQueryParam.TOTALS_MODE.getKey(), totalsMode);
        ret.put(ProtonQueryParam.QUOTA_KEY.getKey(), quotaKey);
        ret.put(ProtonQueryParam.PRIORITY.getKey(), priority);
        ret.put(ProtonQueryParam.DATABASE.getKey(), database);
        ret.put(ProtonQueryParam.COMPRESS.getKey(), String.valueOf(compress));
        ret.put(ProtonQueryParam.DECOMPRESS.getKey(), String.valueOf(decompress));
        ret.put(ProtonQueryParam.EXTREMES.getKey(), String.valueOf(extremes));
        ret.put(ProtonQueryParam.MAX_THREADS.getKey(), maxThreads);
        ret.put(ProtonQueryParam.MAX_EXECUTION_TIME.getKey(), maxExecutionTime);
        ret.put(ProtonQueryParam.MAX_BLOCK_SIZE.getKey(), maxBlockSize);
        ret.put(ProtonQueryParam.MAX_ROWS_TO_GROUP_BY.getKey(), maxRowsToGroupBy);
        ret.put(ProtonQueryParam.PROFILE.getKey(), profile);
        ret.put(ProtonQueryParam.USER.getKey(), user);
        ret.put(ProtonQueryParam.PASSWORD.getKey(), password);
        ret.put(ProtonQueryParam.AUTHORIZATION.getKey(), httpAuthorization);
        ret.put(ProtonQueryParam.DISTRIBUTED_AGGREGATION_MEMORY_EFFICIENT.getKey(), String.valueOf(distributedAggregationMemoryEfficient));
        ret.put(ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY.getKey(), maxBytesBeforeExternalGroupBy);
        ret.put(ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_SORT.getKey(), maxBytesBeforeExternalSort);
        ret.put(ProtonQueryParam.MAX_MEMORY_USAGE.getKey(), maxMemoryUsage);
        ret.put(ProtonQueryParam.MAX_MEMORY_USAGE_FOR_USER.getKey(), maxMemoryUsageForUser);
        ret.put(ProtonQueryParam.MAX_MEMORY_USAGE_FOR_ALL_QUERIES.getKey(), maxMemoryUsageForAllQueries);
        ret.put(ProtonQueryParam.PREFERRED_BLOCK_SIZE_BYTES.getKey(), preferredBlockSizeBytes);
        ret.put(ProtonQueryParam.MAX_QUERY_SIZE.getKey(), maxQuerySize);
        ret.put(ProtonQueryParam.MAX_AST_ELEMENTS.getKey(), maxAstElements);
        ret.put(ProtonQueryParam.SESSION_CHECK.getKey(), String.valueOf(sessionCheck));
        ret.put(ProtonQueryParam.SESSION_ID.getKey(), sessionId);
        ret.put(ProtonQueryParam.SESSION_TIMEOUT.getKey(), sessionTimeout);
        ret.put(ProtonQueryParam.INSERT_QUORUM.getKey(), insertQuorum);
        ret.put(ProtonQueryParam.INSERT_QUORUM_TIMEOUT.getKey(), insertQuorumTimeout);
        ret.put(ProtonQueryParam.SELECT_SEQUENTIAL_CONSISTENCY.getKey(), selectSequentialConsistency);
        ret.put(ProtonQueryParam.ENABLE_OPTIMIZE_PREDICATE_EXPRESSION.getKey(), enableOptimizePredicateExpression);
        ret.put(ProtonQueryParam.MAX_INSERT_BLOCK_SIZE.getKey(), maxInsertBlockSize);
        ret.put(ProtonQueryParam.INSERT_DEDUPLICATE.getKey(), insertDeduplicate);
        ret.put(ProtonQueryParam.INSERT_DISTRIBUTED_SYNC.getKey(), insertDistributedSync);
        ret.put(ProtonQueryParam.ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS.getKey(), anyJoinDistinctRightTableKeys);
        ret.put(ProtonQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS.getKey(), sendProgressInHttpHeaders);
        ret.put(ProtonQueryParam.WAIT_END_OF_QUERY.getKey(), waitEndOfQuery);

        return ret.getProperties();
    }

    public ProtonProperties(ProtonProperties properties) {
        setHost(properties.host);
        setPort(properties.port);
        setAsync(properties.async);
        setBufferSize(properties.bufferSize);
        setApacheBufferSize(properties.apacheBufferSize);
        setSocketTimeout(properties.socketTimeout);
        setConnectionTimeout(properties.connectionTimeout);
        setDataTransferTimeout(properties.dataTransferTimeout);
        setTimeToLiveMillis(properties.timeToLiveMillis);
        setValidateAfterInactivityMillis(properties.validateAfterInactivityMillis);
        setDefaultMaxPerRoute(properties.defaultMaxPerRoute);
        setMaxTotal(properties.maxTotal);
        setMaxRetries(properties.maxRetries);
        setMaxCompressBufferSize(properties.maxCompressBufferSize);
        setSsl(properties.ssl);
        setSslRootCertificate(properties.sslRootCertificate);
        setSslMode(properties.sslMode);
        setUsePathAsDb(properties.usePathAsDb);
        setPath(properties.path);
        setProtocol(properties.protocol);
        setMaxRedirects(properties.maxRedirects);
        setCheckForRedirects(properties.checkForRedirects);
        setUseServerTimeZone(properties.useServerTimeZone);
        setUseTimeZone(properties.useTimeZone);
        setUseServerTimeZoneForDates(properties.useServerTimeZoneForDates);
        setUseObjectsInArrays(properties.useObjectsInArrays);
        setUseSharedCookieStore(properties.useSharedCookieStore);
        setClientName(properties.clientName);
        setMaxParallelReplicas(properties.maxParallelReplicas);
        setMaxPartitionsPerInsertBlock(properties.maxPartitionsPerInsertBlock);
        setTotalsMode(properties.totalsMode);
        setQuotaKey(properties.quotaKey);
        setPriority(properties.priority);
        setDatabase(properties.database);
        setCompress(properties.compress);
        setDecompress(properties.decompress);
        setExtremes(properties.extremes);
        setMaxThreads(properties.maxThreads);
        setMaxExecutionTime(properties.maxExecutionTime);
        setMaxBlockSize(properties.maxBlockSize);
        setMaxRowsToGroupBy(properties.maxRowsToGroupBy);
        setProfile(properties.profile);
        setUser(properties.user);
        setPassword(properties.password);
        setHttpAuthorization(properties.httpAuthorization);
        setDistributedAggregationMemoryEfficient(properties.distributedAggregationMemoryEfficient);
        setMaxBytesBeforeExternalGroupBy(properties.maxBytesBeforeExternalGroupBy);
        setMaxBytesBeforeExternalSort(properties.maxBytesBeforeExternalSort);
        setMaxMemoryUsage(properties.maxMemoryUsage);
        setMaxMemoryUsageForUser(properties.maxMemoryUsageForUser);
        setMaxMemoryUsageForAllQueries(properties.maxMemoryUsageForAllQueries);
        setSessionCheck(properties.sessionCheck);
        setSessionId(properties.sessionId);
        setSessionTimeout(properties.sessionTimeout);
        setInsertQuorum(properties.insertQuorum);
        setInsertQuorumTimeout(properties.insertQuorumTimeout);
        setSelectSequentialConsistency(properties.selectSequentialConsistency);
        setPreferredBlockSizeBytes(properties.preferredBlockSizeBytes);
        setMaxQuerySize(properties.maxQuerySize);
        setMaxAstElements(properties.maxAstElements);
        setEnableOptimizePredicateExpression(properties.enableOptimizePredicateExpression);
        setMaxInsertBlockSize(properties.maxInsertBlockSize);
        setInsertDeduplicate(properties.insertDeduplicate);
        setInsertDistributedSync(properties.insertDistributedSync);
        setAnyJoinDistinctRightTableKeys(properties.anyJoinDistinctRightTableKeys);
        setSendProgressInHttpHeaders(properties.sendProgressInHttpHeaders);
        setWaitEndOfQuery(properties.waitEndOfQuery);
    }

    public Map<ProtonQueryParam, String> buildQueryParams(boolean ignoreDatabase){
        Map<ProtonQueryParam, String> params = new HashMap<>();

        if (maxParallelReplicas != null) {
            params.put(ProtonQueryParam.MAX_PARALLEL_REPLICAS, String.valueOf(maxParallelReplicas));
        }
        if (maxPartitionsPerInsertBlock != null) {
            params.put(ProtonQueryParam.MAX_PARTITIONS_PER_INSERT_BLOCK, String.valueOf(maxPartitionsPerInsertBlock));
        }
        if (maxRowsToGroupBy != null) {
            params.put(ProtonQueryParam.MAX_ROWS_TO_GROUP_BY, String.valueOf(maxRowsToGroupBy));
        }
        if (totalsMode != null) {
            params.put(ProtonQueryParam.TOTALS_MODE, totalsMode);
        }
        if (quotaKey != null) {
            params.put(ProtonQueryParam.QUOTA_KEY, quotaKey);
        }
        if (priority != null) {
            params.put(ProtonQueryParam.PRIORITY, String.valueOf(priority));
        }

        if (!ProtonChecker.isNullOrBlank(database) && !ignoreDatabase) {
            params.put(ProtonQueryParam.DATABASE, getDatabase());
        }

        if (compress) {
            params.put(ProtonQueryParam.COMPRESS, "1");
        }
        if (decompress) {
            params.put(ProtonQueryParam.DECOMPRESS, "1");
        }


        if (extremes) {
            params.put(ProtonQueryParam.EXTREMES, "1");
        }

        if (ProtonChecker.isNullOrBlank(profile)) {
            if (getMaxThreads() != null) {
                params.put(ProtonQueryParam.MAX_THREADS, String.valueOf(maxThreads));
            }

            // in seconds there
            if (getMaxExecutionTime() != null) {
                params.put(ProtonQueryParam.MAX_EXECUTION_TIME, String.valueOf((maxExecutionTime)));
            }

            if (getMaxBlockSize() != null) {
                params.put(ProtonQueryParam.MAX_BLOCK_SIZE, String.valueOf(getMaxBlockSize()));
            }
        } else {
            params.put(ProtonQueryParam.PROFILE, profile);
        }

        if (distributedAggregationMemoryEfficient) {
            params.put(ProtonQueryParam.DISTRIBUTED_AGGREGATION_MEMORY_EFFICIENT, "1");
        }

        if (maxBytesBeforeExternalGroupBy != null) {
            params.put(ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY, String.valueOf(maxBytesBeforeExternalGroupBy));
        }
        if (maxBytesBeforeExternalSort != null) {
            params.put(ProtonQueryParam.MAX_BYTES_BEFORE_EXTERNAL_SORT, String.valueOf(maxBytesBeforeExternalSort));
        }
        if (maxMemoryUsage != null) {
            params.put(ProtonQueryParam.MAX_MEMORY_USAGE, String.valueOf(maxMemoryUsage));
        }
        if (maxMemoryUsageForUser != null) {
            params.put(ProtonQueryParam.MAX_MEMORY_USAGE_FOR_USER, String.valueOf(maxMemoryUsageForUser));
        }
        if (maxMemoryUsageForAllQueries != null) {
            params.put(ProtonQueryParam.MAX_MEMORY_USAGE_FOR_ALL_QUERIES, String.valueOf(maxMemoryUsageForAllQueries));
        }
        if (preferredBlockSizeBytes != null) {
            params.put(ProtonQueryParam.PREFERRED_BLOCK_SIZE_BYTES, String.valueOf(preferredBlockSizeBytes));
        }
        if (maxQuerySize != null) {
            params.put(ProtonQueryParam.MAX_QUERY_SIZE, String.valueOf(maxQuerySize));
        }
        if (maxAstElements != null) {
            params.put(ProtonQueryParam.MAX_AST_ELEMENTS, String.valueOf(maxAstElements));
        }

        if (sessionCheck) {
            params.put(ProtonQueryParam.SESSION_CHECK, "1");
        }

        if (sessionId != null) {
            params.put(ProtonQueryParam.SESSION_ID, String.valueOf(sessionId));
        }

        if (sessionTimeout != null) {
            params.put(ProtonQueryParam.SESSION_TIMEOUT, String.valueOf(sessionTimeout));
        }

        addQueryParam(insertQuorum, ProtonQueryParam.INSERT_QUORUM, params);
        addQueryParam(insertQuorumTimeout, ProtonQueryParam.INSERT_QUORUM_TIMEOUT, params);
        addQueryParam(selectSequentialConsistency, ProtonQueryParam.SELECT_SEQUENTIAL_CONSISTENCY, params);
        addQueryParam(maxInsertBlockSize, ProtonQueryParam.MAX_INSERT_BLOCK_SIZE, params);
        addQueryParam(insertDeduplicate, ProtonQueryParam.INSERT_DEDUPLICATE, params);
        addQueryParam(insertDistributedSync, ProtonQueryParam.INSERT_DISTRIBUTED_SYNC, params);
        addQueryParam(anyJoinDistinctRightTableKeys, ProtonQueryParam.ANY_JOIN_DISTINCT_RIGHT_TABLE_KEYS, params);

        if (enableOptimizePredicateExpression != null) {
            params.put(ProtonQueryParam.ENABLE_OPTIMIZE_PREDICATE_EXPRESSION, enableOptimizePredicateExpression ? "1" : "0");
        }

        addQueryParam(sendProgressInHttpHeaders, ProtonQueryParam.SEND_PROGRESS_IN_HTTP_HEADERS, params);
        addQueryParam(waitEndOfQuery, ProtonQueryParam.WAIT_END_OF_QUERY, params);

        return params;
    }

    private void addQueryParam(Object param, ProtonQueryParam definition, Map<ProtonQueryParam, String> params) {
        if (param != null) {
            if (definition.getClazz() == Boolean.class || definition.getClazz() == boolean.class) {
                params.put(definition, ((Boolean) param) ? "1" : "0");
            } else {
                params.put(definition, String.valueOf(param));
            }
        }
    }

    public ProtonProperties withCredentials(String user, String password){
        ProtonProperties copy = new ProtonProperties(this);
        copy.setUser(user);
        copy.setPassword(password);
        return copy;
    }


    private <T> T getSetting(Properties info, ProtonQueryParam param){
        return getSetting(info, param.getKey(), param.getDefaultValue(), param.getClazz());
    }

    private <T> T getSetting(Properties info, ProtonConnectionSettings settings){
        return getSetting(info, settings.getKey(), settings.getDefaultValue(), settings.getClazz());
    }

    @SuppressWarnings("unchecked")
    private <T> T getSetting(Properties info, String key, Object defaultValue, Class clazz){
        String val = info.getProperty(key);
        if (val == null) {
            return (T)defaultValue;
        }
        if (clazz == int.class || clazz == Integer.class) {
            return (T) clazz.cast(Integer.valueOf(val));
        }
        if (clazz == long.class || clazz == Long.class) {
            return (T) clazz.cast(Long.valueOf(val));
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            final Boolean boolValue;
            if ("1".equals(val) || "0".equals(val)) {
                boolValue = "1".equals(val);
            } else {
                boolValue = Boolean.valueOf(val);
            }
            return (T) clazz.cast(boolValue);
        }
        return (T) clazz.cast(val);
    }


    public String getProfile() {
        return profile;
    }

    public void setProfile(String profile) {
        this.profile = profile;
    }

    public boolean isCompress() {
        return compress;
    }

    public void setCompress(boolean compress) {
        this.compress = compress;
    }

    public boolean isDecompress() {
        return decompress;
    }

    public void setDecompress(boolean decompress) {
        this.decompress = decompress;
    }

    public boolean isAsync() {
        return async;
    }

    public void setAsync(boolean async) {
        this.async = async;
    }

    public Integer getMaxThreads() {
        return maxThreads;
    }

    public void setMaxThreads(Integer maxThreads) {
        this.maxThreads = maxThreads;
    }

    public Integer getMaxBlockSize() {
        return maxBlockSize;
    }

    public void setMaxBlockSize(Integer maxBlockSize) {
        this.maxBlockSize = maxBlockSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getApacheBufferSize() {
        return apacheBufferSize;
    }

    public void setApacheBufferSize(int apacheBufferSize) {
        this.apacheBufferSize = apacheBufferSize;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public int getDataTransferTimeout() {
        return dataTransferTimeout;
    }

    public void setDataTransferTimeout(int dataTransferTimeout) {
        this.dataTransferTimeout = dataTransferTimeout;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public int getTimeToLiveMillis() {
        return timeToLiveMillis;
    }

    public void setTimeToLiveMillis(int timeToLiveMillis) {
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public int getValidateAfterInactivityMillis() {
        return validateAfterInactivityMillis;
    }

    public void setValidateAfterInactivityMillis(int validateAfterInactivityMillis) {
        this.validateAfterInactivityMillis = validateAfterInactivityMillis;
    }

    public int getDefaultMaxPerRoute() {
        return defaultMaxPerRoute;
    }

    public void setDefaultMaxPerRoute(int defaultMaxPerRoute) {
        this.defaultMaxPerRoute = defaultMaxPerRoute;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public int getMaxCompressBufferSize() {
        return maxCompressBufferSize;
    }

    public void setMaxCompressBufferSize(int maxCompressBufferSize) {
        this.maxCompressBufferSize = maxCompressBufferSize;
    }

    public boolean getSsl() {
        return ssl;
    }

    public void setSsl(boolean ssl) {
        this.ssl = ssl;
    }

    public String getSslRootCertificate() {
        return sslRootCertificate;
    }

    public void setSslRootCertificate(String sslRootCertificate) {
        this.sslRootCertificate = sslRootCertificate;
    }

    public String getSslMode() {
        return sslMode;
    }

    public void setSslMode(String sslMode) {
        this.sslMode = sslMode;
    }

    public int getMaxRedirects() {
        return maxRedirects;
    }

    public void setMaxRedirects(int maxRedirects) {
        this.maxRedirects = maxRedirects;
    }

    public boolean isCheckForRedirects() {
        return checkForRedirects;
    }

    public void setCheckForRedirects(boolean checkForRedirects) {
        this.checkForRedirects = checkForRedirects;
    }
    public boolean isUseServerTimeZone() {
        return useServerTimeZone;
    }

    public void setUseServerTimeZone(boolean useServerTimeZone) {
        this.useServerTimeZone = useServerTimeZone;
    }

    public String getUseTimeZone() {
        return useTimeZone;
    }

    public void setUseTimeZone(String useTimeZone) {
        this.useTimeZone = useTimeZone;
    }

    public boolean isUseObjectsInArrays() {
        return useObjectsInArrays;
    }

    public void setUseObjectsInArrays(boolean useObjectsInArrays) {
        this.useObjectsInArrays = useObjectsInArrays;
    }

    public boolean isUseSharedCookieStore() {
        return useSharedCookieStore;
    }

    public void setUseSharedCookieStore(boolean useSharedCookieStore) {
        this.useSharedCookieStore = useSharedCookieStore;
    }

    public String getClientName() {
        return this.clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public boolean isUseServerTimeZoneForDates() {
        return useServerTimeZoneForDates;
    }

    public void setUseServerTimeZoneForDates(boolean useServerTimeZoneForDates) {
        this.useServerTimeZoneForDates = useServerTimeZoneForDates;
    }

    public Integer getMaxParallelReplicas() {
        return maxParallelReplicas;
    }

    public void setMaxParallelReplicas(Integer maxParallelReplicas) {
        this.maxParallelReplicas = maxParallelReplicas;
    }

    public Integer getMaxPartitionsPerInsertBlock() {
        return maxPartitionsPerInsertBlock;
    }

    public void setMaxPartitionsPerInsertBlock(Integer maxPartitionsPerInsertBlock) {
        this.maxPartitionsPerInsertBlock = maxPartitionsPerInsertBlock;
    }

    public String getTotalsMode() {
        return totalsMode;
    }

    public void setTotalsMode(String totalsMode) {
        this.totalsMode = totalsMode;
    }

    public String getQuotaKey() {
        return quotaKey;
    }

    public void setQuotaKey(String quotaKey) {
        this.quotaKey = quotaKey;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public boolean isExtremes() {
        return extremes;
    }

    public void setExtremes(boolean extremes) {
        this.extremes = extremes;
    }

    public Integer getMaxExecutionTime() {
        return maxExecutionTime;
    }

    public void setMaxExecutionTime(Integer maxExecutionTime) {
        this.maxExecutionTime = maxExecutionTime;
    }

    public Integer getMaxRowsToGroupBy() {
        return maxRowsToGroupBy;
    }

    public void setMaxRowsToGroupBy(Integer maxRowsToGroupBy) {
        this.maxRowsToGroupBy = maxRowsToGroupBy;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHttpAuthorization() {
        return httpAuthorization;
    }

    public void setHttpAuthorization(String httpAuthorization) {
        this.httpAuthorization = httpAuthorization;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public ProtonProtocol getProtocol() {
        return this.protocol;
    }

    public void setProtocol(ProtonProtocol protocol) {
        this.protocol = protocol;
    }

    public boolean isUsePathAsDb() {
        return usePathAsDb;
    }

    public void setUsePathAsDb(boolean usePathAsDb) {
        this.usePathAsDb = usePathAsDb;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean isDistributedAggregationMemoryEfficient() {
        return distributedAggregationMemoryEfficient;
    }

    public void setDistributedAggregationMemoryEfficient(boolean distributedAggregationMemoryEfficient) {
        this.distributedAggregationMemoryEfficient = distributedAggregationMemoryEfficient;
    }

    public Long getMaxBytesBeforeExternalGroupBy() {
        return maxBytesBeforeExternalGroupBy;
    }

    public void setMaxBytesBeforeExternalGroupBy(Long maxBytesBeforeExternalGroupBy) {
        this.maxBytesBeforeExternalGroupBy = maxBytesBeforeExternalGroupBy;
    }

    public Long getMaxBytesBeforeExternalSort() {
        return maxBytesBeforeExternalSort;
    }

    public void setMaxBytesBeforeExternalSort(Long maxBytesBeforeExternalSort) {
        this.maxBytesBeforeExternalSort = maxBytesBeforeExternalSort;
    }

    public Long getMaxMemoryUsage() {
        return maxMemoryUsage;
    }

    public void setMaxMemoryUsage(Long maxMemoryUsage) {
        this.maxMemoryUsage = maxMemoryUsage;
    }

    public Long getMaxMemoryUsageForUser() {
        return maxMemoryUsageForUser;
    }

    public void setMaxMemoryUsageForUser(Long maxMemoryUsageForUser) {
        this.maxMemoryUsageForUser = maxMemoryUsageForUser;
    }

    public Long getMaxMemoryUsageForAllQueries() {
        return maxMemoryUsageForAllQueries;
    }

    public void setMaxMemoryUsageForAllQueries(Long maxMemoryUsageForAllQueries) {
        this.maxMemoryUsageForAllQueries = maxMemoryUsageForAllQueries;
    }

    public Long getPreferredBlockSizeBytes() {
        return preferredBlockSizeBytes;
    }

    public void setPreferredBlockSizeBytes(Long preferredBlockSizeBytes) {
        this.preferredBlockSizeBytes = preferredBlockSizeBytes;
    }

    public Long getMaxQuerySize() {
        return maxQuerySize;
    }

    public void setMaxQuerySize(Long maxQuerySize) {
        this.maxQuerySize = maxQuerySize;
    }

    public void setMaxAstElements(Long maxAstElements) {
        this.maxAstElements = maxAstElements;
    }

    public Long getMaxAstElements() {
        return this.maxAstElements;
    }

    public boolean isSessionCheck() { return sessionCheck; }

    public void setSessionCheck(boolean sessionCheck) { this.sessionCheck = sessionCheck; }

    public String getSessionId() { return sessionId; }

    public void setSessionId(String sessionId) { this.sessionId = sessionId; }

    public Long getSessionTimeout() { return sessionTimeout; }

    public void setSessionTimeout(Long sessionTimeout) { this.sessionTimeout = sessionTimeout; }

    public Long getInsertQuorum() {
        return insertQuorum;
    }

    public void setInsertQuorum(Long insertQuorum) {
        this.insertQuorum = insertQuorum;
    }

    public Long getInsertQuorumTimeout() {
        return insertQuorumTimeout;
    }

    public void setInsertQuorumTimeout(Long insertQuorumTimeout) {
        this.insertQuorumTimeout = insertQuorumTimeout;
    }

    public Long getSelectSequentialConsistency() {
        return selectSequentialConsistency;
    }

    public void setSelectSequentialConsistency(Long selectSequentialConsistency) {
        this.selectSequentialConsistency = selectSequentialConsistency;
    }

    public Boolean getEnableOptimizePredicateExpression() {
        return enableOptimizePredicateExpression;
    }

    public void setEnableOptimizePredicateExpression(Boolean enableOptimizePredicateExpression) {
        this.enableOptimizePredicateExpression = enableOptimizePredicateExpression;
    }

    public Long getMaxInsertBlockSize() {
        return maxInsertBlockSize;
    }

    public void setMaxInsertBlockSize(Long maxInsertBlockSize) {
        this.maxInsertBlockSize = maxInsertBlockSize;
    }

    public Boolean getInsertDeduplicate() {
        return insertDeduplicate;
    }

    public void setInsertDeduplicate(Boolean insertDeduplicate) {
        this.insertDeduplicate = insertDeduplicate;
    }

    public Boolean getInsertDistributedSync() {
        return insertDistributedSync;
    }

    public void setInsertDistributedSync(Boolean insertDistributedSync) {
        this.insertDistributedSync = insertDistributedSync;
    }

    public void setAnyJoinDistinctRightTableKeys(Boolean anyJoinDistinctRightTableKeys) {
        this.anyJoinDistinctRightTableKeys = anyJoinDistinctRightTableKeys;
    }

    public Boolean getAnyJoinDistinctRightTableKeys() {
        return anyJoinDistinctRightTableKeys;
    }

    public Boolean getSendProgressInHttpHeaders() {
        return sendProgressInHttpHeaders;
    }

    public void setSendProgressInHttpHeaders(Boolean sendProgressInHttpHeaders) {
        this.sendProgressInHttpHeaders = sendProgressInHttpHeaders;
    }

    public Boolean getWaitEndOfQuery() {
        return waitEndOfQuery;
    }

    public void setWaitEndOfQuery(Boolean waitEndOfQuery) {
        this.waitEndOfQuery = waitEndOfQuery;
    }

    private static class PropertiesBuilder {
        private final Properties properties;
        public PropertiesBuilder() {
            properties = new Properties();
        }

        public void put(String key, int value) {
            properties.put(key, value);
        }

        public void put(String key, Integer value) {
            if (value != null) {
                properties.put(key, value.toString());
            }
        }

        public void put(String key, Long value) {
            if (value != null) {
                properties.put(key, value.toString());
            }
        }

        public void put(String key, Boolean value) {
            if (value != null) {
                properties.put(key, value.toString());
            }
        }

        public void put(String key, String value) {
            if (value != null) {
                properties.put(key, value);
            }
        }

        public Properties getProperties() {
            return properties;
        }
    }

    public ProtonProperties merge(ProtonProperties second){
        Properties properties = this.asProperties();
        for (Map.Entry<Object, Object> entry : second.asProperties().entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }

        return new ProtonProperties(properties);
    }

    public ProtonProperties merge(Properties other){
        Properties properties = this.asProperties();
        for (Map.Entry<Object, Object> entry : other.entrySet()) {
            properties.put(entry.getKey(), entry.getValue());
        }

        return new ProtonProperties(properties);
    }

}
