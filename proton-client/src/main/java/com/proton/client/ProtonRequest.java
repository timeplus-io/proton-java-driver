package com.proton.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;

import com.proton.client.config.ProtonClientOption;
import com.proton.client.config.ProtonOption;
import com.proton.client.config.ProtonDefaults;
import com.proton.client.data.ProtonExternalTable;

/**
 * Request object holding references to {@link ProtonClient},
 * {@link ProtonNode}, format, sql, options and settings etc. for execution.
 */
@SuppressWarnings("squid:S119")
public class ProtonRequest<SelfT extends ProtonRequest<SelfT>> implements Serializable {
    private static final String TYPE_EXTERNAL_TABLE = "ExternalTable";

    /**
     * Mutation request.
     */
    public static class Mutation extends ProtonRequest<Mutation> {
        protected Mutation(ProtonRequest<?> request, boolean sealed) {
            super(request.getClient(), request.server, sealed);

            this.options.putAll(request.options);
            this.settings.putAll(request.settings);

            this.sessionId = request.sessionId;
        }

        @Override
        protected String getQuery() {
            if (input != null && sql != null) {
                return new StringBuilder().append(sql).append(" FORMAT ").append(getFormat().name()).toString();
            }

            return super.getQuery();
        }

        @Override
        public Mutation format(ProtonFormat format) {
            if (!ProtonChecker.nonNull(format, "format").supportsInput()) {
                throw new IllegalArgumentException("Only input format is allowed for mutation.");
            }

            return super.format(format);
        }

        /**
         * Loads data from given file which may or may not be compressed.
         *
         * @param file absolute or relative path of the file, file extension will be
         *             used to determine if it's compressed or not
         * @return mutation request
         */
        public Mutation data(String file) {
            return data(file, ProtonCompression.fromFileName(file));
        }

        /**
         * Loads compressed data from given file.
         *
         * @param file        absolute or relative path of the file
         * @param compression compression algorithm, {@link ProtonCompression#NONE}
         *                    means no compression
         * @return mutation request
         */
        @SuppressWarnings("squid:S2095")
        public Mutation data(String file, ProtonCompression compression) {
            checkSealed();

            FileInputStream fileInput = null;

            try {
                fileInput = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                throw new IllegalArgumentException(e);
            }

            if (compression != null && compression != ProtonCompression.NONE) {
                // TODO create input stream
            } else {
                this.input = CompletableFuture.completedFuture(fileInput);
            }

            return this;
        }

        /**
         * Loads data from input stream.
         *
         * @param input input stream
         * @return mutation requets
         */
        public Mutation data(InputStream input) {
            checkSealed();

            this.input = CompletableFuture.completedFuture(input);

            return this;
        }

        /**
         * Sends mutation requets for execution. Same as
         * {@code client.execute(request.seal())}.
         *
         * @return non-null future to get response
         * @throws CompletionException when error occurred
         */
        public CompletableFuture<ProtonResponse> send() {
            return execute();
        }

        /**
         * Synchronous version of {@link #send()}.
         *
         * @return non-null response
         * @throws ProtonException when error occurred during execution
         */
        public ProtonResponse sendAndWait() throws ProtonException {
            return executeAndWait();
        }

        @Override
        public Mutation table(String table, String queryId) {
            checkSealed();

            this.queryId = queryId;

            String sql = "INSERT INTO " + ProtonChecker.nonBlank(table, "table");
            if (!sql.equals(this.sql)) {
                this.sql = sql;
                this.preparedQuery = null;
                resetCache();
            }

            return this;
        }

        @Override
        public Mutation seal() {
            Mutation req = this;

            if (!isSealed()) {
                // no idea which node we'll connect to until now
                req = new Mutation(this, true);
                req.externalTables.addAll(externalTables);
                req.options.putAll(options);
                req.settings.putAll(settings);

                req.namedParameters.putAll(namedParameters);

                req.input = input;
                req.queryId = queryId;
                req.sessionId = sessionId;
                req.sql = sql;

                req.preparedQuery = preparedQuery;
            }

            return req;
        }
    }

    private static final long serialVersionUID = 4990313525960702287L;

    private final boolean sealed;

    private transient ProtonClient client;

    protected final ProtonConfig clientConfig;
    protected final Function<ProtonNodeSelector, ProtonNode> server;
    protected final transient List<ProtonExternalTable> externalTables;
    protected final Map<ProtonOption, Serializable> options;
    protected final Map<String, Serializable> settings;

    protected final Map<String, String> namedParameters;

    protected transient CompletableFuture<InputStream> input;
    protected String queryId;
    protected String sessionId;
    protected String sql;
    protected ProtonParameterizedQuery preparedQuery;

    // cache
    protected transient ProtonConfig config;
    protected transient List<String> statements;

    @SuppressWarnings("unchecked")
    protected ProtonRequest(ProtonClient client, Function<ProtonNodeSelector, ProtonNode> server,
            boolean sealed) {
        if (client == null || server == null) {
            throw new IllegalArgumentException("Non-null client and server are required");
        }

        this.client = client;
        this.clientConfig = client.getConfig();
        this.server = (Function<ProtonNodeSelector, ProtonNode> & Serializable) server::apply;
        this.sealed = sealed;

        this.externalTables = new LinkedList<>();
        this.options = new HashMap<>();
        this.settings = new LinkedHashMap<>();

        this.namedParameters = new HashMap<>();
    }

    protected void checkSealed() {
        if (sealed) {
            throw new IllegalStateException("Sealed request is immutable");
        }
    }

    protected ProtonClient getClient() {
        if (client == null) {
            client = ProtonClient.builder().config(clientConfig).build();
        }

        return client;
    }

    /**
     * Gets query, either set by {@code query()} or {@code table()}.
     *
     * @return sql query
     */
    protected String getQuery() {
        return this.sql;
    }

    protected void resetCache() {
        if (config != null) {
            config = null;
        }

        if (statements != null) {
            statements = null;
        }
    }

    /**
     * Creates a copy of this request object.
     *
     * @return copy of this request
     */
    public ProtonRequest<SelfT> copy() {
        ProtonRequest<SelfT> req = new ProtonRequest<>(getClient(), server, false);
        req.externalTables.addAll(externalTables);
        req.options.putAll(options);
        req.settings.putAll(settings);
        req.namedParameters.putAll(namedParameters);
        req.input = input;
        req.queryId = queryId;
        req.sessionId = sessionId;
        req.sql = sql;
        req.preparedQuery = preparedQuery;
        return req;
    }

    /**
     * Checks if the request is sealed(immutable).
     *
     * @return true if the request is sealed; false otherwise
     */
    public boolean isSealed() {
        return this.sealed;
    }

    /**
     * Checks if the request contains any input stream.
     *
     * @return true if there's input stream; false otherwise
     */
    public boolean hasInputStream() {
        return this.input != null || !this.externalTables.isEmpty();
    }

    /**
     * Depending on the {@link java.util.function.Function} passed to the
     * constructor, this method may return different node for each call.
     * 
     * @return node defined by {@link java.util.function.Function}
     */
    public final ProtonNode getServer() {
        return this.server.apply(getConfig().getNodeSelector());
    }

    /**
     * Gets request configuration.
     *
     * @return request configuration
     */
    public ProtonConfig getConfig() {
        if (config == null) {
            if (options.isEmpty()) {
                config = clientConfig;
            } else {
                Map<ProtonOption, Serializable> merged = new HashMap<>();
                merged.putAll(clientConfig.getAllOptions());
                merged.putAll(options);
                config = new ProtonConfig(merged, clientConfig.getDefaultCredentials(),
                        clientConfig.getNodeSelector(), clientConfig.getMetricRegistry().orElse(null));
            }
        }

        return config;
    }

    /**
     * Gets input stream.
     *
     * @return input stream
     */
    public Optional<InputStream> getInputStream() {
        try {
            return Optional.ofNullable(input != null ? input.get() : null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        }
    }

    /**
     * Gets immutable list of external tables.
     *
     * @return immutable list of external tables
     */
    public List<ProtonExternalTable> getExternalTables() {
        return Collections.unmodifiableList(externalTables);
    }

    /**
     * Gets data format used for communication between server and client.
     *
     * @return data format used for communication between server and client
     */
    public ProtonFormat getFormat() {
        return getConfig().getFormat();
    }

    /**
     * Gets query id.
     *
     * @return query id
     */
    public Optional<String> getQueryId() {
        return ProtonChecker.isNullOrEmpty(queryId) ? Optional.empty() : Optional.of(queryId);
    }

    /**
     * Gets prepared query, which is a loosely parsed query with the origianl query
     * and list of parameters.
     *
     * @return prepared query
     */
    public ProtonParameterizedQuery getPreparedQuery() {
        if (preparedQuery == null) {
            preparedQuery = ProtonParameterizedQuery.of(getConfig(), getQuery());
        }

        return preparedQuery;
    }

    /**
     * Gets immutable settings.
     *
     * @return immutable settings
     */
    public Map<String, Object> getSettings() {
        return Collections.unmodifiableMap(settings);
    }

    /**
     * Gets session id.
     *
     * @return session id
     */
    public Optional<String> getSessionId() {
        return ProtonChecker.isNullOrEmpty(sessionId) ? Optional.empty() : Optional.of(sessionId);
    }

    /**
     * Gets list of SQL statements. Same as {@code getStatements(true)}.
     *
     * @return list of SQL statements
     */
    public List<String> getStatements() {
        return getStatements(true);
    }

    /**
     * Gets list of SQL statements.
     *
     * @param withSettings true to treat settings as SQL statement; false otherwise
     * @return list of SQL statements
     */
    public List<String> getStatements(boolean withSettings) {
        if (statements == null) {
            statements = new ArrayList<>();

            if (withSettings) {
                for (Entry<String, Serializable> entry : settings.entrySet()) {
                    Serializable value = entry.getValue();
                    StringBuilder sb = new StringBuilder().append("SET ").append(entry.getKey()).append('=');
                    if (value instanceof String) {
                        sb.append('\'').append(value).append('\'');
                    } else if (value instanceof Boolean) {
                        sb.append((boolean) value ? 1 : 0);
                    } else {
                        sb.append(value);
                    }
                    statements.add(sb.toString());
                }
            }

            String stmt = getQuery();
            if (!ProtonChecker.isNullOrEmpty(stmt)) {
                StringBuilder builder = new StringBuilder();
                if (preparedQuery == null) {
                    ProtonParameterizedQuery.apply(builder, stmt, namedParameters);
                } else {
                    preparedQuery.apply(builder, namedParameters);
                }
                statements.add(builder.toString());
            }
        }

        return Collections.unmodifiableList(statements);
    }

    /**
     * Enable or disable compression of server response. Pay attention that
     * {@link ProtonClientOption#COMPRESS_ALGORITHM} and
     * {@link ProtonClientOption#COMPRESS_LEVEL} will be used.
     *
     * @param enable true to enable compression of server response; false otherwise
     * @return the request itself
     */
    public SelfT compressServerResponse(boolean enable) {
        return compressServerResponse(enable, null,
                (int) ProtonClientOption.COMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of server response. Pay attention that
     * {@link ProtonClientOption#COMPRESS_LEVEL} will be used.
     *
     * @param enable            true to enable compression of server response; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ProtonCompression#NONE} or
     *                          {@link ProtonClientOption#COMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @return the request itself
     */
    public SelfT compressServerResponse(boolean enable, ProtonCompression compressAlgorithm) {
        return compressServerResponse(enable, compressAlgorithm,
                (int) ProtonClientOption.COMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of server response.
     *
     * @param enable            true to enable compression of server response; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ProtonCompression#NONE} or
     *                          {@link ProtonClientOption#COMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @param compressLevel     compression level
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT compressServerResponse(boolean enable, ProtonCompression compressAlgorithm, int compressLevel) {
        checkSealed();

        if (compressAlgorithm == null) {
            compressAlgorithm = enable
                    ? (ProtonCompression) ProtonClientOption.COMPRESS_ALGORITHM.getEffectiveDefaultValue()
                    : ProtonCompression.NONE;
        }

        if (compressLevel < 0) {
            compressLevel = 0;
        } else if (compressLevel > 9) {
            compressLevel = 9;
        }

        Object oldSwitch = options.put(ProtonClientOption.COMPRESS, enable);
        Object oldAlgorithm = options.put(ProtonClientOption.COMPRESS_ALGORITHM, compressAlgorithm);
        Object oldLevel = options.put(ProtonClientOption.COMPRESS_LEVEL, compressLevel);
        if (oldSwitch == null || !oldSwitch.equals(enable) || oldAlgorithm == null
                || !oldAlgorithm.equals(compressAlgorithm) || oldLevel == null || !oldLevel.equals(compressLevel)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Enable or disable compression of client request. Pay attention that
     * {@link ProtonClientOption#DECOMPRESS_ALGORITHM} and
     * {@link ProtonClientOption#DECOMPRESS_LEVEL} will be used.
     *
     * @param enable true to enable compression of client request; false otherwise
     * @return the request itself
     */
    public SelfT decompressClientRequest(boolean enable) {
        return decompressClientRequest(enable, null,
                (int) ProtonClientOption.DECOMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of client request. Pay attention that
     * {@link ProtonClientOption#DECOMPRESS_LEVEL} will be used.
     *
     * @param enable            true to enable compression of client request; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ProtonCompression#NONE} or
     *                          {@link ProtonClientOption#DECOMPRESS_ALGORITHM}
     *                          depending on whether enabled
     * @return the request itself
     */
    public SelfT decompressClientRequest(boolean enable, ProtonCompression compressAlgorithm) {
        return decompressClientRequest(enable, compressAlgorithm,
                (int) ProtonClientOption.DECOMPRESS_LEVEL.getEffectiveDefaultValue());
    }

    /**
     * Enable or disable compression of client request.
     *
     * @param enable            true to enable compression of client request; false
     *                          otherwise
     * @param compressAlgorithm compression algorithm, null is treated as
     *                          {@link ProtonCompression#NONE}
     * @param compressLevel     compression level
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT decompressClientRequest(boolean enable, ProtonCompression compressAlgorithm, int compressLevel) {
        checkSealed();

        if (compressAlgorithm == null) {
            compressAlgorithm = enable
                    ? (ProtonCompression) ProtonClientOption.DECOMPRESS_ALGORITHM.getEffectiveDefaultValue()
                    : ProtonCompression.NONE;
        }

        if (compressLevel < 0) {
            compressLevel = 0;
        } else if (compressLevel > 9) {
            compressLevel = 9;
        }

        Object oldSwitch = options.put(ProtonClientOption.DECOMPRESS, enable);
        Object oldAlgorithm = options.put(ProtonClientOption.DECOMPRESS_ALGORITHM, compressAlgorithm);
        Object oldLevel = options.put(ProtonClientOption.DECOMPRESS_LEVEL, compressLevel);
        if (oldSwitch == null || !oldSwitch.equals(enable) || oldAlgorithm == null
                || !oldAlgorithm.equals(compressAlgorithm) || oldLevel == null || !oldLevel.equals(compressLevel)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Adds an external table.
     *
     * @param table non-null external table
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT addExternal(ProtonExternalTable table) {
        checkSealed();

        if (externalTables.add(ProtonChecker.nonNull(table, TYPE_EXTERNAL_TABLE))) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets one or more external tables.
     *
     * @param table non-null external table
     * @param more  more external tables
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT external(ProtonExternalTable table, ProtonExternalTable... more) {
        checkSealed();

        externalTables.clear();
        externalTables.add(ProtonChecker.nonNull(table, TYPE_EXTERNAL_TABLE));
        if (more != null) {
            for (ProtonExternalTable e : more) {
                externalTables.add(ProtonChecker.nonNull(e, TYPE_EXTERNAL_TABLE));
            }
        }

        return (SelfT) this;
    }

    /**
     * Sets external tables.
     *
     * @param tables non-null external tables
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT external(Collection<ProtonExternalTable> tables) {
        checkSealed();

        externalTables.clear();
        if (tables != null) {
            for (ProtonExternalTable e : tables) {
                externalTables.add(ProtonChecker.nonNull(e, TYPE_EXTERNAL_TABLE));
            }
        }

        return (SelfT) this;
    }

    /**
     * Sets format to be used for communication between server and client.
     *
     * @param format non-null format
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT format(ProtonFormat format) {
        checkSealed();

        Object oldValue = options.put(ProtonClientOption.FORMAT,
                format != null ? format : (ProtonFormat) ProtonDefaults.FORMAT.getEffectiveDefaultValue());
        if (oldValue == null || !oldValue.equals(format)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets an option. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param option option
     * @param value  value
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT option(ProtonOption option, Serializable value) {
        checkSealed();

        Object oldValue = options.put(ProtonChecker.nonNull(option, "option"),
                ProtonChecker.nonNull(value, "value"));
        if (oldValue == null || !oldValue.equals(value)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets all options. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param options options
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT options(Map<ProtonOption, Serializable> options) {
        checkSealed();

        this.options.clear();
        if (options != null) {
            this.options.putAll(options);
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets all options. {@code option} is for configuring client's behaviour, while
     * {@code setting} is for server.
     *
     * @param options options
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT options(Properties options) {
        checkSealed();

        this.options.clear();
        if (options != null) {
            for (Entry<Object, Object> e : options.entrySet()) {
                Object key = e.getKey();
                Object value = e.getValue();
                if (key == null || value == null) {
                    continue;
                }

                ProtonClientOption o = ProtonClientOption.fromKey(key.toString());
                if (o != null) {
                    this.options.put(o, ProtonOption.fromString(value.toString(), o.getValueType()));
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters. Be aware of SQL injection risk as mentioned in
     * {@link #params(String, String...)}.
     *
     * @param values stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Collection<String> values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && !values.isEmpty()) {
            List<String> names = getPreparedQuery().getParameters();
            int size = names.size();
            int index = 0;
            for (String v : values) {
                namedParameters.put(names.get(index), v);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameters wrapped by {@link ProtonValue}. Safer but a bit slower
     * than {@link #params(String, String...)}. Consider to reuse ProtonValue
     * object and its update methods for less overhead in batch processing.
     *
     * @param value parameter
     * @param more  more parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(ProtonValue value, ProtonValue... more) {
        checkSealed();

        namedParameters.clear();

        if (value != null) { // it doesn't make sense to pass null as first parameter
            List<String> names = getPreparedQuery().getParameters();
            int size = names.size();
            int index = 0;

            namedParameters.put(names.get(index++), value.toSqlExpression());

            if (more != null && more.length > 0) {
                for (ProtonValue v : more) {
                    if (index >= size) {
                        break;
                    }
                    namedParameters.put(names.get(index++),
                            v != null ? v.toSqlExpression() : ProtonValues.NULL_EXPR);
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameters wrapped by {@link ProtonValue}. Safer but a bit slower
     * than {@link #params(String, String...)}. Consider to reuse ProtonValue
     * object and its update methods for less overhead in batch processing.
     *
     * @param values parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(ProtonValue[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getParameters();
            int size = names.size();
            int index = 0;
            for (ProtonValue v : values) {
                namedParameters.put(names.get(index), v != null ? v.toSqlExpression() : ProtonValues.NULL_EXPR);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters which are used to substitude named parameters in
     * SQL query without further transformation and validation. Keep in mind that
     * stringified parameter is a SQL expression, meaning it could be a
     * sub-query(SQL injection) in addition to value like number and string.
     *
     * @param value stringified parameter
     * @param more  more stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(String value, String... more) {
        checkSealed();

        namedParameters.clear();

        List<String> names = getPreparedQuery().getParameters();
        int size = names.size();
        int index = 0;
        namedParameters.put(names.get(index++), value);

        if (more != null && more.length > 0) {
            for (String v : more) {
                if (index >= size) {
                    break;
                }
                namedParameters.put(names.get(index++), v);
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets stringified parameters which are used to substitude named parameters in
     * SQL query without further transformation and validation. Keep in mind that
     * stringified parameter is a SQL expression, meaning it could be a
     * sub-query(SQL injection) in addition to value like number and string.
     *
     * @param values stringified parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(String[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getParameters();
            int size = names.size();
            int index = 0;
            for (String v : values) {
                namedParameters.put(names.get(index), v);
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Set raw parameters, which will later be stringified using
     * {@link ProtonValues#convertToSqlExpression(Object)}. Although it is
     * convenient to use, it's NOT recommended in most cases except for a few
     * parameters and/or testing.
     *
     * @param value raw parameter
     * @param more  more raw parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Object value, Object... more) {
        checkSealed();

        namedParameters.clear();

        List<String> names = getPreparedQuery().getParameters();
        int size = names.size();
        int index = 0;
        namedParameters.put(names.get(index++), ProtonValues.convertToSqlExpression(value));

        if (more != null && more.length > 0) {
            for (Object v : more) {
                if (index >= size) {
                    break;
                }
                namedParameters.put(names.get(index++), ProtonValues.convertToSqlExpression(v));
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Set raw parameters, which will later be stringified using
     * {@link ProtonValues#convertToSqlExpression(Object)}. Although it is
     * convenient to use, it's NOT recommended in most cases except for a few
     * parameters and/or testing.
     *
     * @param values raw parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Object[] values) {
        checkSealed();

        namedParameters.clear();

        if (values != null && values.length > 0) {
            List<String> names = getPreparedQuery().getParameters();
            int size = names.size();
            int index = 0;
            for (Object v : values) {
                namedParameters.put(names.get(index), ProtonValues.convertToSqlExpression(v));
                if (++index >= size) {
                    break;
                }
            }
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets named parameters. Be aware of SQL injection risk as mentioned in
     * {@link #params(String, String...)}.
     *
     * @param namedParams named parameters
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT params(Map<String, String> namedParams) {
        checkSealed();

        namedParameters.clear();

        if (namedParams != null) {
            namedParameters.putAll(namedParams);
        }

        resetCache();

        return (SelfT) this;
    }

    /**
     * Sets parameterized query. Same as {@code query(query, null)}.
     *
     * @param query non-null parameterized query
     * @return the request itself
     */
    public SelfT query(ProtonParameterizedQuery query) {
        return query(query, null);
    }

    /**
     * Sets query. Same as {@code query(sql, null)}.
     *
     * @param sql non-empty query
     * @return the request itself
     */
    public SelfT query(String sql) {
        return query(sql, null);
    }

    /**
     * Sets parameterized query and optinally query id.
     *
     * @param query   non-null parameterized query
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT query(ProtonParameterizedQuery query, String queryId) {
        checkSealed();

        if (!ProtonChecker.nonNull(query, "query").equals(this.preparedQuery)) {
            this.preparedQuery = query;
            this.sql = query.getOriginalQuery();
            resetCache();
        }

        this.queryId = queryId;

        return (SelfT) this;
    }

    /**
     * Sets query and optinally query id.
     *
     * @param sql     non-empty query
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT query(String sql, String queryId) {
        checkSealed();

        if (!ProtonChecker.nonBlank(sql, "sql").equals(this.sql)) {
            this.sql = sql;
            this.preparedQuery = null;
            resetCache();
        }

        this.queryId = queryId;

        return (SelfT) this;
    }

    /**
     * Clears session configuration including session id, whether to validate the id
     * and session timeout.
     *
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT clearSession() {
        checkSealed();

        boolean changed = this.sessionId != null;
        this.sessionId = null;

        Object oldValue = null;
        oldValue = options.remove(ProtonClientOption.SESSION_CHECK);
        changed = changed || oldValue != null;

        oldValue = options.remove(ProtonClientOption.SESSION_TIMEOUT);
        changed = changed || oldValue != null;

        if (changed) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets current session using custom id. Same as
     * {@code session(sessionId, null, null)}.
     *
     * @param sessionId session id, null means no session
     * @return the request itself
     */
    public SelfT session(String sessionId) {
        return session(sessionId, null, null);
    }

    /**
     * Sets session. Same as {@code session(sessionId, check, null)}.
     *
     * @param sessionId session id, null means no session
     * @param check     whether the server should check if the session id exists or
     *                  not
     * @return the request itself
     */
    public SelfT session(String sessionId, Boolean check) {
        return session(sessionId, check, null);
    }

    /**
     * Sets current session. Same as {@code session(sessionId, null, timeout)}.
     *
     * @param sessionId session id, null means no session
     * @param timeout   timeout in milliseconds
     * @return the request itself
     */
    public SelfT session(String sessionId, Integer timeout) {
        return session(sessionId, null, timeout);
    }

    /**
     * Sets current session.
     *
     * @param sessionId session id, null means no session
     * @param check     whether the server should check if the session id exists or
     *                  not
     * @param timeout   timeout in milliseconds
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT session(String sessionId, Boolean check, Integer timeout) {
        checkSealed();

        boolean changed = !Objects.equals(this.sessionId, sessionId);
        this.sessionId = sessionId;

        Object oldValue = null;
        if (check != null) {
            oldValue = options.put(ProtonClientOption.SESSION_CHECK, check);
            changed = oldValue == null || !oldValue.equals(check);
        }

        if (timeout != null) {
            oldValue = options.put(ProtonClientOption.SESSION_TIMEOUT, timeout);
            changed = changed || oldValue == null || !oldValue.equals(timeout);
        }

        if (changed) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets a setting. See
     * https://proton.tech/docs/en/operations/settings/settings/ for more
     * information.
     *
     * @param setting non-empty setting to set
     * @param value   value of the setting
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT set(String setting, Serializable value) {
        checkSealed();

        Serializable oldValue = settings.put(ProtonChecker.nonBlank(setting, "setting"),
                ProtonChecker.nonNull(value, "value"));
        if (oldValue == null || !oldValue.equals(value)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Sets a setting. See
     * https://proton.tech/docs/en/operations/settings/settings/ for more
     * information.
     *
     * @param setting non-empty setting to set
     * @param value   value of the setting
     * @return the request itself
     */
    public SelfT set(String setting, String value) {
        checkSealed();

        return set(setting, (Serializable) ProtonUtils.escape(value, '\''));
    }

    /**
     * Sets target table. Same as {@code table(table, null)}.
     *
     * @param table non-empty table name
     * @return the request itself
     */
    public SelfT table(String table) {
        return table(table, null);
    }

    /**
     * Sets target table and optionally query id. This will generate a query like
     * {@code SELECT * FROM [table]} and override the one set by
     * {@link #query(String, String)}.
     *
     * @param table   non-empty table name
     * @param queryId query id, null means no query id
     * @return the request itself
     */
    public SelfT table(String table, String queryId) {
        return query("SELECT * FROM " + ProtonChecker.nonBlank(table, "table"), queryId);
    }

    /**
     * Changes current database.
     *
     * @param database non-empty database name
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT use(String database) {
        checkSealed();

        Object oldValue = options.put(ProtonClientOption.DATABASE,
                ProtonChecker.nonNull(database, "database"));
        if (oldValue == null || !oldValue.equals(database)) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes an external table.
     *
     * @param external non-null external table
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeExternal(ProtonExternalTable external) {
        checkSealed();

        if (externalTables.remove(ProtonChecker.nonNull(external, TYPE_EXTERNAL_TABLE))) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes an external table by name.
     *
     * @param name non-empty external table name
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeExternal(String name) {
        checkSealed();

        if (!ProtonChecker.isNullOrEmpty(name)) {
            boolean removed = false;
            Iterator<ProtonExternalTable> i = externalTables.iterator();
            while (i.hasNext()) {
                ProtonExternalTable e = i.next();
                if (name.equals(e.getName())) {
                    i.remove();
                    removed = true;
                }
            }

            if (removed) {
                resetCache();
            }
        }

        return (SelfT) this;
    }

    /**
     * Removes an option.
     *
     * @param option option to be removed
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeOption(ProtonOption option) {
        checkSealed();

        if (options.remove(ProtonChecker.nonNull(option, "option")) != null) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Removes a setting.
     *
     * @param setting name of the setting
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT removeSetting(String setting) {
        checkSealed();

        if (settings.remove(ProtonChecker.nonBlank(setting, "setting")) != null) {
            resetCache();
        }

        return (SelfT) this;
    }

    /**
     * Resets the request to start all over.
     *
     * @return the request itself
     */
    @SuppressWarnings("unchecked")
    public SelfT reset() {
        checkSealed();

        this.externalTables.clear();
        this.options.clear();
        this.settings.clear();

        this.namedParameters.clear();

        this.input = null;
        this.sql = null;
        this.preparedQuery = null;
        this.queryId = null;
        this.sessionId = null;

        resetCache();

        return (SelfT) this;
    }

    /**
     * Creates a sealed request, which is an immutable copy of the current request.
     *
     * @return sealed request, an immutable copy of the current request
     */
    public ProtonRequest<SelfT> seal() {
        ProtonRequest<SelfT> req = this;

        if (!isSealed()) {
            // no idea which node we'll connect to until now
            req = new ProtonRequest<>(client, getServer(), true);
            req.externalTables.addAll(externalTables);
            req.options.putAll(options);
            req.settings.putAll(settings);

            req.namedParameters.putAll(namedParameters);

            req.input = input;
            req.queryId = queryId;
            req.sessionId = sessionId;
            req.sql = sql;
            req.preparedQuery = preparedQuery;
        }

        return req;
    }

    /**
     * Creates a new request for mutation.
     *
     * @return request for mutation
     */
    public Mutation write() {
        checkSealed();

        return new Mutation(this, false);
    }

    /**
     * Executes the request. Same as {@code client.execute(request.seal())}.
     * 
     * @return non-null future to get response
     * @throws CompletionException when error occurred during execution
     */
    public CompletableFuture<ProtonResponse> execute() {
        return getClient().execute(isSealed() ? this : seal());
    }

    /**
     * Synchronous version of {@link #execute()}.
     *
     * @return non-null response
     * @throws ProtonException when error occurred during execution
     */
    public ProtonResponse executeAndWait() throws ProtonException {
        return getClient().executeAndWait(isSealed() ? this : seal());
    }
}
