package com.proton.jdbc.internal;

import java.net.URI;
import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonClient;
import com.proton.client.ProtonColumn;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonNodeSelector;
import com.proton.client.ProtonParameterizedQuery;
import com.proton.client.ProtonRecord;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValues;
import com.proton.client.ProtonVersion;
import com.proton.client.config.ProtonClientOption;
import com.proton.client.http.config.ProtonHttpOption;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;
import com.proton.jdbc.ProtonConnection;
import com.proton.jdbc.ProtonDatabaseMetaData;
import com.proton.jdbc.ProtonDriver;
import com.proton.jdbc.ProtonStatement;
import com.proton.jdbc.JdbcConfig;
import com.proton.jdbc.JdbcParameterizedQuery;
import com.proton.jdbc.JdbcParseHandler;
import com.proton.jdbc.SqlExceptionUtils;
import com.proton.jdbc.JdbcWrapper;
import com.proton.jdbc.internal.ProtonJdbcUrlParser.ConnectionInfo;
import com.proton.jdbc.internal.FakeTransaction.FakeSavepoint;
import com.proton.jdbc.parser.ProtonSqlParser;
import com.proton.jdbc.parser.ProtonSqlStatement;
import com.proton.jdbc.parser.StatementType;

public class ProtonConnectionImpl extends JdbcWrapper implements ProtonConnection {
    private static final Logger log = LoggerFactory.getLogger(ProtonConnectionImpl.class);

    private static final String CREATE_DB = "create database if not exists `";

    protected static ProtonRecord getServerInfo(ProtonNode node, ProtonRequest<?> request,
            boolean createDbIfNotExist) throws SQLException {
        ProtonRequest<?> newReq = request.copy();
        if (!createDbIfNotExist) { // in case the database does not exist
            newReq.option(ProtonClientOption.DATABASE, "");
        }
        try (ProtonResponse response = newReq.option(ProtonClientOption.ASYNC, false)
                .option(ProtonClientOption.COMPRESS, false).option(ProtonClientOption.DECOMPRESS, false)
                .option(ProtonClientOption.FORMAT, ProtonFormat.RowBinaryWithNamesAndTypes)
                .query("select current_user(), timezone(), version(), "
                        + "if_null((select to_uint8(value) from system.settings where name='readonly'), 0) AS readonly "
                        + "FORMAT RowBinaryWithNamesAndTypes")
                .execute().get()) {
            return response.firstRecord();
        } catch (InterruptedException | CancellationException e) {
            // not going to happen as it's synchronous call
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            SQLException sqlExp = SqlExceptionUtils.handle(e);
            if (createDbIfNotExist && sqlExp.getErrorCode() == 81) {
                String db = node.getDatabase(request.getConfig());
                try (ProtonResponse resp = newReq.use("")
                        .query(new StringBuilder(CREATE_DB.length() + 1 + db.length()).append(CREATE_DB).append(db)
                                .append('`').toString())
                        .execute().get()) {
                    return getServerInfo(node, request, false);
                } catch (InterruptedException | CancellationException ex) {
                    // not going to happen as it's synchronous call
                    Thread.currentThread().interrupt();
                    throw SqlExceptionUtils.forCancellation(ex);
                } catch (SQLException ex) {
                    throw ex;
                } catch (Exception ex) {
                    throw SqlExceptionUtils.handle(ex);
                }
            } else {
                throw sqlExp;
            }
        }
    }

    private final JdbcConfig jdbcConf;

    private final ProtonClient client;
    private final ProtonRequest<?> clientRequest;

    private boolean autoCommit;
    private boolean closed;
    private String database;
    private boolean readOnly;
    private int networkTimeout;
    private int rsHoldability;
    private int txIsolation;

    private final Optional<TimeZone> clientTimeZone;
    private final Calendar defaultCalendar;
    private final TimeZone jvmTimeZone;
    private final TimeZone serverTimeZone;
    private final ProtonVersion serverVersion;
    private final String user;
    private final int initialReadOnly;

    private final Map<String, Class<?>> typeMap;

    private final AtomicReference<FakeTransaction> fakeTransaction;

    private URI uri;

    /**
     * Checks if the connection is open or not.
     *
     * @throws SQLException when the connection is closed
     */
    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed connection");
        }
    }

    /**
     * Checks if a feature can be supported or not.
     *
     * @param feature non-empty feature name
     * @param silent  whether to show warning in log or throw unsupported exception
     * @throws SQLException when the feature is not supported and silent is
     *                      {@code false}
     */
    protected void ensureSupport(String feature, boolean silent) throws SQLException {
        String msg = feature + " is not supported";

        if (jdbcConf.isJdbcCompliant()) {
            if (silent) {
                log.debug("[JDBC Compliant Mode] %s. Change %s to false to throw SQLException instead.", msg,
                        JdbcConfig.PROP_JDBC_COMPLIANT);
            } else {
                log.warn("[JDBC Compliant Mode] %s. Change %s to false to throw SQLException instead.", msg,
                        JdbcConfig.PROP_JDBC_COMPLIANT);
            }
        } else if (!silent) {
            throw SqlExceptionUtils.unsupportedError(msg);
        }
    }

    protected void ensureTransactionSupport() throws SQLException {
        ensureSupport("Transaction", false);
    }

    protected List<ProtonColumn> getTableColumns(String dbName, String tableName, String columns)
            throws SQLException {
        if (tableName == null || columns == null) {
            throw SqlExceptionUtils.clientError("Failed to extract table and columns from the query");
        }

        if (columns.isEmpty()) {
            columns = "*";
        } else {
            columns = columns.substring(1); // remove the leading bracket
        }
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ").append(columns).append(" FROM ");
        if (!ProtonChecker.isNullOrEmpty(dbName)) {
            builder.append('`').append(ProtonUtils.escape(dbName, '`')).append('`').append('.');
        }
        builder.append('`').append(ProtonUtils.escape(tableName, '`')).append('`').append(" WHERE 0 LIMIT 0");
        List<ProtonColumn> list;
        try (ProtonResponse resp = clientRequest.copy().query(builder.toString()).execute().get()) {
            list = resp.getColumns();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
        return list;
    }

    // for testing only
    final FakeTransaction getTransaction() {
        return fakeTransaction.get();
    }

    public ProtonConnectionImpl(String url) throws SQLException {
        this(url, new Properties());
    }

    public ProtonConnectionImpl(String url, Properties properties) throws SQLException {
        this(ProtonJdbcUrlParser.parse(url, properties));
    }

    public ProtonConnectionImpl(ConnectionInfo connInfo) throws SQLException {
        jdbcConf = connInfo.getJdbcConfig();

        autoCommit = !jdbcConf.isJdbcCompliant() || jdbcConf.isAutoCommit();

        this.uri = connInfo.getUri();

        log.debug("Creating a new connection to %s", connInfo.getUri());
        ProtonNode node = connInfo.getServer();
        log.debug("Target node: %s", node);

        jvmTimeZone = TimeZone.getDefault();

        client = ProtonClient.builder().options(ProtonDriver.toClientOptions(connInfo.getProperties()))
                .nodeSelector(ProtonNodeSelector.of(node.getProtocol())).build();
        clientRequest = client.connect(node);
        ProtonConfig config = clientRequest.getConfig();
        String currentUser = null;
        TimeZone timeZone = null;
        ProtonVersion version = null;
        if (config.hasServerInfo()) { // when both serverTimeZone and serverVersion are configured
            timeZone = config.getServerTimeZone();
            version = config.getServerVersion();
            if (jdbcConf.isCreateDbIfNotExist()) {
                initialReadOnly = getServerInfo(node, clientRequest, true).getValue(3).asInteger();
            } else {
                initialReadOnly = (int) clientRequest.getSettings().getOrDefault("readonly", 0);
            }
        } else {
            ProtonRecord r = getServerInfo(node, clientRequest, jdbcConf.isCreateDbIfNotExist());
            currentUser = r.getValue(0).asString();
            String tz = r.getValue(1).asString();
            String ver = r.getValue(2).asString();
            version = ProtonVersion.of(ver);
            // https://github.com/Clickhouse/Clickhouse/commit/486d63864bcc6e15695cd3e9f9a3f83a84ec4009
            /*if (version.check("(,20.7)")) {
                throw SqlExceptionUtils
                        .unsupportedError("Sorry this driver only supports Proton server 20.7 or above");
            }*/
            if (ProtonChecker.isNullOrBlank(tz)) {
                tz = "UTC";
            }
            // tsTimeZone.hasSameRules(ProtonValues.UTC_TIMEZONE)
            timeZone = "UTC".equals(tz) ? ProtonValues.UTC_TIMEZONE : TimeZone.getTimeZone(tz);
            initialReadOnly = r.getValue(3).asInteger();

            // update request and corresponding config
            clientRequest.option(ProtonClientOption.SERVER_TIME_ZONE, tz)
                    .option(ProtonClientOption.SERVER_VERSION, ver);
        }

        this.autoCommit = true;
        this.closed = false;
        this.database = config.getDatabase();
        this.clientRequest.use(this.database);
        this.readOnly = initialReadOnly != 0;
        this.networkTimeout = 0;
        this.rsHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        this.txIsolation = jdbcConf.isJdbcCompliant() ? Connection.TRANSACTION_READ_COMMITTED
                : Connection.TRANSACTION_NONE;

        this.user = currentUser != null ? currentUser : node.getCredentials(config).getUserName();
        this.serverTimeZone = timeZone;
        if (config.isUseServerTimeZone()) {
            clientTimeZone = Optional.empty();
            // with respect of default locale
            defaultCalendar = new GregorianCalendar();
        } else {
            clientTimeZone = Optional.of(config.getUseTimeZone());
            defaultCalendar = new GregorianCalendar(clientTimeZone.get());
        }
        this.serverVersion = version;
        this.typeMap = new HashMap<>(jdbcConf.getTypeMap());
        this.fakeTransaction = new AtomicReference<>();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        ensureOpen();

        // get rewritten query?
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        ensureOpen();

        if (this.autoCommit == autoCommit) {
            return;
        }

        ensureTransactionSupport();
        if (this.autoCommit = autoCommit) { // commit
            FakeTransaction tx = fakeTransaction.getAndSet(null);
            if (tx != null) {
                tx.logTransactionDetails(log, FakeTransaction.ACTION_COMMITTED);
                tx.clear();
            }
        } else { // start new transaction
            if (!fakeTransaction.compareAndSet(null, new FakeTransaction())) {
                log.warn("[JDBC Compliant Mode] not able to start a new transaction, reuse the exist one");
            }
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        ensureOpen();

        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot commit in auto-commit mode");
        }

        ensureTransactionSupport();

        FakeTransaction tx = fakeTransaction.getAndSet(new FakeTransaction());
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(FakeTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            tx.logTransactionDetails(log, FakeTransaction.ACTION_COMMITTED);
            tx.clear();
        }
    }

    @Override
    public void rollback() throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot rollback in auto-commit mode");
        }

        ensureTransactionSupport();

        FakeTransaction tx = fakeTransaction.getAndSet(new FakeTransaction());
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(FakeTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            tx.logTransactionDetails(log, FakeTransaction.ACTION_ROLLBACK);
            tx.clear();
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            this.client.close();
        } catch (Exception e) {
            log.warn("Failed to close connection due to %s", e.getMessage());
            throw SqlExceptionUtils.handle(e);
        } finally {
            this.closed = true;
        }

        FakeTransaction tx = fakeTransaction.getAndSet(null);
        if (tx != null) {
            tx.logTransactionDetails(log, FakeTransaction.ACTION_COMMITTED);
            tx.clear();
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new ProtonDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        ensureOpen();

        if (initialReadOnly != 0) {
            if (!readOnly) {
                throw SqlExceptionUtils.clientError("Cannot change the setting on a read-only connection");
            }
        } else {
            if (readOnly) {
                clientRequest.set("readonly", 2);
            } else {
                clientRequest.removeSetting("readonly");
            }
            this.readOnly = readOnly;
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        ensureOpen();

        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        ensureOpen();

        log.warn("Proton does not support catalog, please use setSchema instead");
    }

    @Override
    public String getCatalog() throws SQLException {
        ensureOpen();

        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        ensureOpen();

        if (level == Connection.TRANSACTION_READ_UNCOMMITTED || level == Connection.TRANSACTION_READ_COMMITTED
                || level == Connection.TRANSACTION_REPEATABLE_READ || level == Connection.TRANSACTION_SERIALIZABLE) {
            txIsolation = level;
        } else {
            throw new SQLException("Invalid transaction isolation level: " + level);
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        ensureOpen();

        return txIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        ensureOpen();

        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        ensureOpen();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        ensureOpen();

        return new HashMap<>(typeMap);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        ensureOpen();

        if (map != null) {
            typeMap.putAll(map);
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        ensureOpen();

        if (holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT || holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            rsHoldability = holdability;
        } else {
            throw new SQLException("Invalid holdability: " + holdability);
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        ensureOpen();

        return rsHoldability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return setSavepoint(null);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot set savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("setSavepoint not implemented");
        }

        FakeTransaction tx = fakeTransaction.updateAndGet(current -> current != null ? current : new FakeTransaction());
        return tx.newSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot rollback to savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("rollback not implemented");
        }

        if (!(savepoint instanceof FakeSavepoint)) {
            throw SqlExceptionUtils.clientError("Unsupported type of savepoint: " + savepoint);
        }

        FakeTransaction tx = fakeTransaction.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(FakeTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            FakeSavepoint s = (FakeSavepoint) savepoint;
            tx.logSavepointDetails(log, s, FakeTransaction.ACTION_ROLLBACK);
            tx.toSavepoint(s);
        }
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        ensureOpen();

        if (getAutoCommit()) {
            throw SqlExceptionUtils.clientError("Cannot release savepoint in auto-commit mode");
        }

        if (!jdbcConf.isJdbcCompliant()) {
            throw SqlExceptionUtils.unsupportedError("rollback not implemented");
        }

        if (!(savepoint instanceof FakeSavepoint)) {
            throw SqlExceptionUtils.clientError("Unsupported type of savepoint: " + savepoint);
        }

        FakeTransaction tx = fakeTransaction.get();
        if (tx == null) {
            // invalid transaction state
            throw new SQLException(FakeTransaction.ERROR_TX_NOT_STARTED, SqlExceptionUtils.SQL_STATE_INVALID_TX_STATE);
        } else {
            FakeSavepoint s = (FakeSavepoint) savepoint;
            tx.logSavepointDetails(log, s, "released");
            tx.toSavepoint(s);
        }
    }

    @Override
    public ProtonStatement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        ensureOpen();

        return new ProtonStatementImpl(this, clientRequest.copy(), resultSetType, resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        ensureOpen();

        ProtonConfig config = clientRequest.getConfig();
        // TODO remove the extra parsing
        ProtonSqlStatement[] stmts = parse(sql, config);
        if (stmts.length != 1) {
            throw SqlExceptionUtils
                    .clientError("Prepared statement only supports one query but we got: " + stmts.length);
        }
        ProtonSqlStatement parsedStmt = stmts[0];

        ProtonParameterizedQuery preparedQuery;
        try {
            preparedQuery = jdbcConf.useNamedParameter()
                    ? ProtonParameterizedQuery.of(clientRequest.getConfig(), parsedStmt.getSQL())
                    : JdbcParameterizedQuery.of(config, parsedStmt.getSQL());
        } catch (RuntimeException e) {
            throw SqlExceptionUtils.clientError(e);
        }

        PreparedStatement ps = null;
        if (preparedQuery.hasParameter()) {
            if (parsedStmt.hasTempTable() || parsedStmt.hasInput()) {
                throw SqlExceptionUtils
                        .clientError(
                                "External table, input function, and query parameter cannot be used together in PreparedStatement.");
            } else if (parsedStmt.getStatementType() == StatementType.INSERT &&
                    !parsedStmt.containsKeyword("SELECT") && parsedStmt.hasValues() &&
                    (!parsedStmt.hasFormat() || clientRequest.getFormat().name().equals(parsedStmt.getFormat()))) {
                String query = parsedStmt.getSQL();
                int startIndex = parsedStmt.getPositions().get(ProtonSqlStatement.KEYWORD_VALUES_START);
                int endIndex = parsedStmt.getPositions().get(ProtonSqlStatement.KEYWORD_VALUES_END);
                boolean useStream = true;
                for (int i = startIndex + 1; i < endIndex; i++) {
                    char ch = query.charAt(i);
                    if (ch != '?' && ch != ',' && !Character.isWhitespace(ch)) {
                        useStream = false;
                        break;
                    }
                }

                if (useStream) {
                    ps = new InputBasedPreparedStatement(this,
                            clientRequest.write().query(query.substring(0, parsedStmt.getStartPosition("VALUES")),
                                    newQueryId()),
                            getTableColumns(parsedStmt.getDatabase(), parsedStmt.getTable(),
                                    parsedStmt.getContentBetweenKeywords(
                                            ProtonSqlStatement.KEYWORD_STREAM_COLUMNS_START,
                                            ProtonSqlStatement.KEYWORD_STREAM_COLUMNS_END)),
                            resultSetType, resultSetConcurrency, resultSetHoldability);
                }
            }
        } else {
            if (parsedStmt.hasTempTable()) {
                // queries using external/temporary table
                ps = new TableBasedPreparedStatement(this,
                        clientRequest.write().query(parsedStmt.getSQL(), newQueryId()), parsedStmt,
                        resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if (parsedStmt.getStatementType() == StatementType.INSERT) {
                if (!ProtonChecker.isNullOrBlank(parsedStmt.getInput())) {
                    // insert query using input function
                    ps = new InputBasedPreparedStatement(this,
                            clientRequest.write().query(parsedStmt.getSQL(), newQueryId()),
                            ProtonColumn.parse(parsedStmt.getInput()), resultSetType,
                            resultSetConcurrency, resultSetHoldability);
                } else if (!parsedStmt.containsKeyword("SELECT") && !parsedStmt.hasValues() &&
                        (!parsedStmt.hasFormat() || clientRequest.getFormat().name().equals(parsedStmt.getFormat()))) {
                    ps = new InputBasedPreparedStatement(this,
                            clientRequest.write().query(parsedStmt.getSQL(), newQueryId()),
                            getTableColumns(parsedStmt.getDatabase(), parsedStmt.getTable(),
                                    parsedStmt.getContentBetweenKeywords(
                                            ProtonSqlStatement.KEYWORD_STREAM_COLUMNS_START,
                                            ProtonSqlStatement.KEYWORD_STREAM_COLUMNS_END)),
                            resultSetType, resultSetConcurrency, resultSetHoldability);
                }
            }
        }

        return ps != null ? ps
                : new SqlBasedPreparedStatement(this, clientRequest.copy().query(preparedQuery, newQueryId()),
                        stmts[0], resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public NClob createNClob() throws SQLException {
        ensureOpen();

        return createClob();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw SqlExceptionUtils.clientError("Negative milliseconds is not allowed");
        }

        if (isClosed()) {
            return false;
        }

        return client.ping(clientRequest.getServer(), (int) TimeUnit.SECONDS.toMillis(timeout));
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            ensureOpen();
        } catch (SQLException e) {
            Map<String, ClientInfoStatus> failedProps = new HashMap<>();
            failedProps.put(PROP_APPLICATION_NAME, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_HEADERS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_PARAMS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            throw new SQLClientInfoException(e.getMessage(), failedProps);
        }

        if (PROP_APPLICATION_NAME.equals(name)) {
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonClientOption.CLIENT_NAME);
            } else {
                clientRequest.option(ProtonClientOption.CLIENT_NAME, value);
            }
        } else if (PROP_CUSTOM_HTTP_HEADERS.equals(name)) {
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonHttpOption.CUSTOM_HEADERS);
            } else {
                clientRequest.option(ProtonHttpOption.CUSTOM_HEADERS, value);
            }
        } else if (PROP_CUSTOM_HTTP_PARAMS.equals(name)) {
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonHttpOption.CUSTOM_PARAMS);
            } else {
                clientRequest.option(ProtonHttpOption.CUSTOM_PARAMS, value);
            }
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            ensureOpen();
        } catch (SQLException e) {
            Map<String, ClientInfoStatus> failedProps = new HashMap<>();
            failedProps.put(PROP_APPLICATION_NAME, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_HEADERS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            failedProps.put(PROP_CUSTOM_HTTP_PARAMS, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
            throw new SQLClientInfoException(e.getMessage(), failedProps);
        }

        if (properties != null) {
            String value = properties.getProperty(PROP_APPLICATION_NAME);
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonClientOption.CLIENT_NAME);
            } else {
                clientRequest.option(ProtonClientOption.CLIENT_NAME, value);
            }

            value = properties.getProperty(PROP_CUSTOM_HTTP_HEADERS);
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonHttpOption.CUSTOM_HEADERS);
            } else {
                clientRequest.option(ProtonHttpOption.CUSTOM_HEADERS, value);
            }

            value = properties.getProperty(PROP_CUSTOM_HTTP_PARAMS);
            if (ProtonChecker.isNullOrBlank(value)) {
                clientRequest.removeOption(ProtonHttpOption.CUSTOM_PARAMS);
            } else {
                clientRequest.option(ProtonHttpOption.CUSTOM_PARAMS, value);
            }
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        ensureOpen();

        ProtonConfig config = clientRequest.getConfig();
        String value = null;
        if (PROP_APPLICATION_NAME.equals(name)) {
            value = config.getClientName();
        } else if (PROP_CUSTOM_HTTP_HEADERS.equals(name)) {
            value = (String) config.getOption(ProtonHttpOption.CUSTOM_HEADERS);
        } else if (PROP_CUSTOM_HTTP_PARAMS.equals(name)) {
            value = (String) config.getOption(ProtonHttpOption.CUSTOM_PARAMS);
        }
        return value;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        ensureOpen();

        ProtonConfig config = clientRequest.getConfig();
        Properties props = new Properties();
        props.setProperty(PROP_APPLICATION_NAME, config.getClientName());
        props.setProperty(PROP_CUSTOM_HTTP_HEADERS, (String) config.getOption(ProtonHttpOption.CUSTOM_HEADERS));
        props.setProperty(PROP_CUSTOM_HTTP_PARAMS, (String) config.getOption(ProtonHttpOption.CUSTOM_PARAMS));
        return props;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        ensureOpen();

        if (schema == null || schema.isEmpty()) {
            throw new SQLException("Non-empty schema name is required", SqlExceptionUtils.SQL_STATE_INVALID_SCHEMA);
        } else if (!schema.equals(database)) {
            this.database = schema;
            clientRequest.use(schema);
        }
    }

    @Override
    public String getSchema() throws SQLException {
        ensureOpen();

        return getCurrentDatabase();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (executor == null) {
            throw SqlExceptionUtils.clientError("Non-null executor is required");
        }

        executor.execute(() -> {
            try {
                // try harder please
                this.client.close();
            } finally {
                this.closed = true;
            }
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        ensureOpen();

        if (executor == null) {
            throw SqlExceptionUtils.clientError("Non-null executor is required");
        }

        if (milliseconds < 0) {
            throw SqlExceptionUtils.clientError("Negative milliseconds is not allowed");
        }

        executor.execute(() -> {
            // TODO close this connection when any statement timed out after this amount of
            // time
            networkTimeout = milliseconds;
        });
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        ensureOpen();

        return networkTimeout;
    }

    @Override
    public ProtonConfig getConfig() {
        return clientRequest.getConfig();
    }

    @Override
    public String getCurrentDatabase() {
        return database;
    }

    @Override
    public String getCurrentUser() {
        return user;
    }

    @Override
    public Calendar getDefaultCalendar() {
        return defaultCalendar;
    }

    @Override
    public Optional<TimeZone> getEffectiveTimeZone() {
        return clientTimeZone;
    }

    @Override
    public TimeZone getJvmTimeZone() {
        return jvmTimeZone;
    }

    @Override
    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    @Override
    public ProtonVersion getServerVersion() {
        return serverVersion;
    }

    @Override
    public URI getUri() {
        return uri;
    }

    @Override
    public JdbcConfig getJdbcConfig() {
        return jdbcConf;
    }

    @Override
    public String newQueryId() {
        FakeTransaction tx = fakeTransaction.get();
        return tx != null ? tx.newQuery(null) : UUID.randomUUID().toString();
    }

    @Override
    public ProtonSqlStatement[] parse(String sql, ProtonConfig config) {
        return ProtonSqlParser.parse(sql, config != null ? config : clientRequest.getConfig(),
                jdbcConf.isJdbcCompliant() ? JdbcParseHandler.INSTANCE : null);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ProtonClient.class || iface == ProtonRequest.class
                || super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == ProtonClient.class) {
            return iface.cast(client);
        } else if (iface == ProtonRequest.class) {
            return iface.cast(clientRequest);
        }

        return super.unwrap(iface);
    }
}
