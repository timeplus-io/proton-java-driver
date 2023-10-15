package ru.yandex.proton;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;

import ru.yandex.proton.domain.ProtonDataType;
import ru.yandex.proton.except.ProtonUnknownException;
import ru.yandex.proton.settings.ProtonConnectionSettings;
import ru.yandex.proton.settings.ProtonProperties;
import ru.yandex.proton.settings.ProtonQueryParam;
import ru.yandex.proton.util.ProtonHttpClientBuilder;
import ru.yandex.proton.util.LogProxy;
import ru.yandex.proton.util.Utils;

public class ProtonConnectionImpl implements ProtonConnection {

	private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final Logger log = LoggerFactory.getLogger(ProtonConnectionImpl.class);

    private final CloseableHttpClient httpclient;

    private final ProtonProperties properties;

    private String url;

    private boolean closed = false;

    private TimeZone serverTimeZone;
    private TimeZone timezone;
    private String serverVersion;

    public ProtonConnectionImpl(String url) throws SQLException {
        this(url, new ProtonProperties());
    }

    public ProtonConnectionImpl(String url, ProtonProperties properties) throws SQLException {
        this.url = url;
        try {
            this.properties = ProtonJdbcUrlParser.parse(url, properties.asProperties());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        ProtonHttpClientBuilder clientBuilder = new ProtonHttpClientBuilder(this.properties);
        log.debug("Create a new connection to %s", url);
        try {
            httpclient = clientBuilder.buildClient();
        }catch (Exception e) {
            throw  new IllegalStateException("cannot initialize http client", e);
        }
        initConnection(this.properties);
    }

    private void initConnection(ProtonProperties properties) throws SQLException {
        // timezone
        if (properties.isUseServerTimeZone() && !Utils.isNullOrEmptyString(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("only one of %s or %s must be enabled", ProtonConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), ProtonConnectionSettings.USE_TIME_ZONE.getKey()));
        }
        if (!properties.isUseServerTimeZone() && Utils.isNullOrEmptyString(properties.getUseTimeZone())) {
            throw new IllegalArgumentException(String.format("one of %s or %s must be enabled", ProtonConnectionSettings.USE_SERVER_TIME_ZONE.getKey(), ProtonConnectionSettings.USE_TIME_ZONE.getKey()));
        }

        serverTimeZone = TimeZone.getTimeZone("UTC"); // just for next query
        timezone = serverTimeZone;
        serverVersion = "";
        
        try (ProtonStatement s = createStatement();
            ResultSet rs = s.executeQuery("select timezone(), version()",
                Collections.singletonMap(ProtonQueryParam.DATABASE, ""))) {
            if (rs.next()) {
                serverTimeZone = TimeZone.getTimeZone(rs.getString(1));
                serverVersion = rs.getString(2);
            }
        }

        timezone = serverTimeZone;
        if (!properties.isUseServerTimeZone()) {
            timezone = Utils.isNullOrEmptyString(properties.getUseTimeZone())
                ? TimeZone.getDefault()
                : TimeZone.getTimeZone(properties.getUseTimeZone());
        }

        if (serverVersion == null) {
            serverVersion = "";
        }
    }

    @Override
    public ProtonStatement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULTSET_TYPE);
    }

    public ProtonStatement createStatement(int resultSetType) throws SQLException {
        return LogProxy.wrap(
            ProtonStatement.class,
            new ProtonStatementImpl(
                httpclient,
                this,
                properties,
                resultSetType));
    }

    @Override
    public TimeZone getTimeZone() {
        return timezone;
    }

    @Override
    public TimeZone getServerTimeZone() {
        return serverTimeZone;
    }

    private ProtonStatement createProtonStatement(CloseableHttpClient httpClient) throws SQLException {
        return LogProxy.wrap(
            ProtonStatement.class,
            new ProtonStatementImpl(
                httpClient,
                this,
                properties,
                DEFAULT_RESULTSET_TYPE));
    }

    public PreparedStatement createPreparedStatement(String sql, int resultSetType) throws SQLException {
        return LogProxy.wrap(
            PreparedStatement.class,
            new ProtonPreparedStatementImpl(
                httpclient,
                this,
                properties,
                sql,
                getTimeZone(),
                resultSetType));
    }

    public ProtonPreparedStatement createProtonPreparedStatement(String sql, int resultSetType) throws SQLException {
        return LogProxy.wrap(
            ProtonPreparedStatement.class,
            new ProtonPreparedStatementImpl(
                httpclient,
                this,
                properties,
                sql,
                getTimeZone(),
                resultSetType));
    }


    @Override
    public ProtonStatement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
    }

    /**
     * lazily calculates and returns server version
     * @return server version string
     * @throws SQLException if something has gone wrong
     */
    @Override
    public String getServerVersion() throws SQLException {
        return serverVersion;
    }

    @Override
    public ProtonStatement createStatement(int resultSetType, int resultSetConcurrency,
                                               int resultSetHoldability) throws SQLException {
        if (resultSetType == ResultSet.TYPE_SCROLL_SENSITIVE || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
            || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            throw new SQLFeatureNotSupportedException();
        }
        return createStatement(resultSetType);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return createPreparedStatement(sql, DEFAULT_RESULTSET_TYPE);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit) {
            return;
        }
        throw new SQLFeatureNotSupportedException("Transactions are not supported");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException("Cannot commit when auto-commit is enabled");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLException("Cannot rollback when auto-commit is enabled");
    }

    @Override
    public void close() throws SQLException {
        try {
            httpclient.close();
            closed = true;
        } catch (IOException e) {
            throw new ProtonUnknownException("HTTP client close exception", e, properties.getHost(), properties.getPort());
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return LogProxy.wrap(DatabaseMetaData.class, new ProtonDatabaseMetadata(url, this));
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        properties.setDatabase(catalog);
        URI old = URI.create(url.substring(ProtonJdbcUrlParser.JDBC_PREFIX.length()));
        try {
            url = ProtonJdbcUrlParser.JDBC_PREFIX +
                    new URI(old.getScheme(), old.getUserInfo(), old.getHost(), old.getPort(),
                            "/" + catalog, old.getQuery(), old.getFragment());
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }


    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return createPreparedStatement(sql, resultSetType);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createPreparedStatement(sql, resultSetType);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("Timeout value mustn't be less 0");
        }

        if (isClosed()) {
            return false;
        }

        boolean isAnotherHttpClient = false;
        CloseableHttpClient closeableHttpClient = null;
        try {
            if (timeout == 0) {
                closeableHttpClient = this.httpclient;
            } else {
                ProtonProperties properties = new ProtonProperties(this.properties);
                int timeoutMs = (int) TimeUnit.SECONDS.toMillis(timeout);
                properties.setConnectionTimeout(timeoutMs);
                properties.setMaxExecutionTime(timeout);
                properties.setSocketTimeout(timeoutMs);
                closeableHttpClient = new ProtonHttpClientBuilder(properties).buildClient();
                isAnotherHttpClient = true;
            }

            Statement statement = createProtonStatement(closeableHttpClient);
            statement.execute("SELECT 1");
            statement.close();
            return true;
        } catch (Exception e) {
            boolean isFailOnConnectionTimeout =
                    e instanceof ConnectTimeoutException
                            || e.getCause() instanceof ConnectTimeoutException;

            if (!isFailOnConnectionTimeout) {
                log.warn("Something had happened while validating a connection", e);
            }

            return false;
        } finally {
            if (isAnotherHttpClient) {
                try {
                    closeableHttpClient.close();
                } catch (IOException e) {
                    log.warn("Can't close a http client", e);
                }
            }
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {

    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return new ProtonArray(
            ProtonDataType.resolveDefaultArrayDataType(typeName),
            elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        properties.setDatabase(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return properties.getDatabase();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        this.close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    void cleanConnections() {
        httpclient.getConnectionManager().closeExpiredConnections();
        httpclient.getConnectionManager().closeIdleConnections(2 * properties.getSocketTimeout(), TimeUnit.MILLISECONDS);
    }

    String getUrl() {
        return url;
    }

    ProtonProperties getProperties() {
        return properties;
    }
}
