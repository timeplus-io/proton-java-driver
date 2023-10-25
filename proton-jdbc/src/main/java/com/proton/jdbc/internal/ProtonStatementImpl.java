package com.proton.jdbc.internal;

import java.io.InputStream;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonClient;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonResponseSummary;
import com.proton.client.config.ProtonClientOption;
import com.proton.client.config.ProtonOption;
import com.proton.client.data.ProtonExternalTable;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;
import com.proton.jdbc.ProtonConnection;
import com.proton.jdbc.ProtonResultSet;
import com.proton.jdbc.ProtonStatement;
import com.proton.jdbc.SqlExceptionUtils;
import com.proton.jdbc.JdbcWrapper;
import com.proton.jdbc.parser.ProtonSqlStatement;
import com.proton.jdbc.parser.StatementType;

public class ProtonStatementImpl extends JdbcWrapper implements ProtonStatement {
    private static final Logger log = LoggerFactory.getLogger(ProtonStatementImpl.class);

    private final ProtonConnection connection;
    private final ProtonRequest<?> request;

    private final int resultSetType;
    private final int resultSetConcurrency;
    private final int resultSetHoldability;

    private final List<ProtonSqlStatement> batchStmts;

    private boolean closed;
    private boolean closeOnCompletion;

    private String cursorName;
    private boolean escapeScan;
    private int fetchSize;
    private int maxFieldSize;
    private long maxRows;
    private boolean poolable;
    private volatile String queryId;
    private int queryTimeout;

    private ProtonResultSet currentResult;
    private long currentUpdateCount;

    protected ProtonSqlStatement[] parsedStmts;

    private ProtonResponse getLastResponse(Map<ProtonOption, Serializable> options,
            List<ProtonExternalTable> tables, Map<String, String> settings) throws SQLException {
        // disable extremes
        if (parsedStmts.length > 1) {
            request.session(UUID.randomUUID().toString());
        }
        ProtonResponse response = null;
        for (int i = 0, len = parsedStmts.length; i < len; i++) {
            ProtonSqlStatement stmt = parsedStmts[i];
            // TODO skip useless queries to reduce network calls and server load
            try {
                response = request.query(stmt.getSQL(), queryId = connection.newQueryId()).execute().get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw SqlExceptionUtils.forCancellation(e);
            } catch (Exception e) {
                throw SqlExceptionUtils.handle(e);
            } finally {
                if (i + 1 < len && response != null) {
                    response.close();
                }
            }
        }

        return response;
    }

    protected void ensureOpen() throws SQLException {
        if (closed) {
            throw SqlExceptionUtils.clientError("Cannot operate on a closed statement");
        }
    }

    protected ProtonResponse executeStatement(String stmt,
            Map<ProtonOption, Serializable> options, List<ProtonExternalTable> tables,
            Map<String, String> settings) throws SQLException {
        try {
            if (options != null) {
                request.options(options);
            }
            if (settings != null && !settings.isEmpty()) {
                if (!request.getSessionId().isPresent()) {
                    request.session(UUID.randomUUID().toString());
                }
                for (Entry<String, String> e : settings.entrySet()) {
                    request.set(e.getKey(), e.getValue());
                }
            }
            if (tables != null && !tables.isEmpty()) {
                List<ProtonExternalTable> list = new ArrayList<>(tables.size());
                for (ProtonExternalTable t : tables) {
                    if (t.isTempTable()) {
                        if (!request.getSessionId().isPresent()) {
                            request.session(UUID.randomUUID().toString());
                        }
                        request.query("drop temporary table if exists `" + t.getName() + "`").execute().get();
                        request.query("create temporary table `" + t.getName() + "`(" + t.getStructure() + ")")
                                .execute().get();
                        request.write()
                                .table(t.getName())
                                .format(t.getFormat() != null ? t.getFormat() : ProtonFormat.RowBinary)
                                .data(t.getContent()).send().get();
                    } else {
                        list.add(t);
                    }
                }
                request.external(list);
            }

            return request.query(stmt, queryId = connection.newQueryId()).execute().get();
        } catch (InterruptedException e) {
            log.error("can not close stream: %s", e.getMessage());
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }
    }

    protected ProtonResponse executeStatement(ProtonSqlStatement stmt,
            Map<ProtonOption, Serializable> options, List<ProtonExternalTable> tables,
            Map<String, String> settings) throws SQLException {
        return executeStatement(stmt.getSQL(), options, tables, settings);
    }

    protected int executeInsert(String sql, InputStream input) throws SQLException {
        ProtonResponseSummary summary = null;
        try (ProtonResponse resp = request.write().query(sql, queryId = connection.newQueryId())
                .format(ProtonFormat.RowBinary).data(input).execute().get();
                ResultSet rs = updateResult(new ProtonSqlStatement(sql, StatementType.INSERT), resp)) {
            summary = resp.getSummary();
        } catch (InterruptedException e) {
            log.error("can not close stream: %s", e.getMessage());
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }

        return summary != null && summary.getWrittenRows() > 0L ? (int) summary.getWrittenRows() : 1;
    }

    protected ProtonSqlStatement getLastStatement() {
        ProtonSqlStatement stmt = null;

        if (parsedStmts != null && parsedStmts.length > 0) {
            stmt = parsedStmts[parsedStmts.length - 1];
        }

        return ProtonChecker.nonNull(stmt, "ParsedStatement");
    }

    protected void setLastStatement(ProtonSqlStatement stmt) {
        if (parsedStmts != null && parsedStmts.length > 0) {
            parsedStmts[parsedStmts.length - 1] = ProtonChecker.nonNull(stmt, "ParsedStatement");
        }
    }

    protected ProtonSqlStatement parseSqlStatements(String sql) {
        parsedStmts = connection.parse(sql, getConfig());

        if (parsedStmts == null || parsedStmts.length == 0) {
            // should never happen
            throw new IllegalArgumentException("Failed to parse given SQL: " + sql);
        }

        return getLastStatement();
    }

    protected ProtonResultSet newEmptyResultSet() throws SQLException {
        return new ProtonResultSet("", "", this, ProtonResponse.EMPTY);
    }

    protected ResultSet updateResult(ProtonSqlStatement stmt, ProtonResponse response) throws SQLException {
        ResultSet rs = null;
        if (stmt.isQuery() || !response.getColumns().isEmpty()) {
            currentUpdateCount = -1L;
            currentResult = new ProtonResultSet(stmt.getDatabaseOrDefault(getConnection().getCurrentDatabase()),
                    stmt.getTable(), this, response);
            rs = currentResult;
        } else {
            currentUpdateCount = response.getSummary().getWrittenRows();
            // FIXME apparently this is not always true
            if (currentUpdateCount <= 0L) {
                currentUpdateCount = 1L;
            }
            currentResult = null;
            response.close();
        }

        return rs == null ? newEmptyResultSet() : rs;
    }

    protected ProtonStatementImpl(ProtonConnectionImpl connection, ProtonRequest<?> request,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (connection == null || request == null) {
            throw SqlExceptionUtils.clientError("Non-null connection and request are required");
        }

        this.connection = connection;
        this.request = request;

        // TODO validate resultSet attributes
        this.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
        this.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        this.resultSetHoldability = ResultSet.CLOSE_CURSORS_AT_COMMIT;

        this.closed = false;
        this.closeOnCompletion = true;

        this.fetchSize = connection.getJdbcConfig().getFetchSize();
        this.maxFieldSize = 0;
        this.maxRows = 0L;
        this.poolable = false;
        this.queryId = null;

        this.queryTimeout = 0;

        this.currentResult = null;
        this.currentUpdateCount = -1L;

        this.batchStmts = new LinkedList<>();

        ProtonConfig c = request.getConfig();
        setLargeMaxRows(c.getMaxResultRows());
        setQueryTimeout(c.getMaxExecutionTime());
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return currentResult != null;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        ensureOpen();
        if (!batchStmts.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        parseSqlStatements(sql);

        ProtonResponse response = getLastResponse(null, null, null);

        try {
            return updateResult(getLastStatement(), response);
        } catch (Exception e) {
            if (response != null) {
                response.close();
            }

            throw SqlExceptionUtils.handle(e);
        }
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        ensureOpen();
        if (!batchStmts.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        parseSqlStatements(sql);

        ProtonResponseSummary summary = null;
        try (ProtonResponse response = getLastResponse(null, null, null)) {
            summary = response.getSummary();
        } catch (Exception e) {
            throw SqlExceptionUtils.handle(e);
        }

        return summary != null ? summary.getWrittenRows() : 1L;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return (int) executeLargeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }

        this.closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        ensureOpen();

        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (max < 0) {
            throw SqlExceptionUtils.clientError("Max field size cannot be set to negative number");
        }
        ensureOpen();

        maxFieldSize = max;
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        ensureOpen();

        return maxRows;
    }

    @Override
    public int getMaxRows() throws SQLException {
        return (int) getLargeMaxRows();
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        if (max < 0L) {
            throw SqlExceptionUtils.clientError("Max rows cannot be set to negative number");
        }
        ensureOpen();

        if (this.maxRows != max) {
            if (max == 0L) {
                request.removeSetting("max_result_rows");
                request.removeSetting("result_overflow_mode");
            } else {
                request.set("max_result_rows", max);
                request.set("result_overflow_mode", "break");
            }
            this.maxRows = max;
        }
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        ensureOpen();

        this.escapeScan = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        ensureOpen();

        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw SqlExceptionUtils.clientError("Query timeout cannot be set to negative seconds");
        }
        ensureOpen();

        if (this.queryTimeout != seconds) {
            if (seconds == 0) {
                request.removeSetting("max_execution_time");
            } else {
                request.set("max_execution_time", seconds);
            }
            this.queryTimeout = seconds;
        }
    }

    @Override
    public void cancel() throws SQLException {
        final String qid;
        if ((qid = this.queryId) == null || isClosed()) {
            return;
        }

        ProtonClient.send(request.getServer(), String.format("KILL QUERY WHERE query_id='%s'", qid))
                .whenComplete((summary, exception) -> {
                    if (exception != null) {
                        log.warn("Failed to kill query [%s] due to: %s", qid, exception.getMessage());
                    } else if (summary != null) {
                        log.debug("Killed query [%s]", qid);
                    }
                });
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
    public void setCursorName(String name) throws SQLException {
        ensureOpen();

        cursorName = name;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureOpen();

        return currentResult;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        ensureOpen();

        return currentUpdateCount;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int) getLargeUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        ensureOpen();

        if (currentResult != null) {
            currentResult.close();
            currentResult = null;
        }
        currentUpdateCount = -1L;
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        ensureOpen();

        if (direction != ResultSet.FETCH_FORWARD) {
            throw SqlExceptionUtils.unsupportedError("only FETCH_FORWARD is supported in setFetchDirection");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        ensureOpen();

        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            throw SqlExceptionUtils.clientError("Fetch size cannot be negative number");
        }

        ensureOpen();

        if (fetchSize != rows) {
            fetchSize = rows;

            if (rows == 0) {
                request.removeOption(ProtonClientOption.MAX_BUFFER_SIZE);
            } else {
                request.option(ProtonClientOption.MAX_BUFFER_SIZE, rows * 1024);
            }
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        ensureOpen();

        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        ensureOpen();

        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        ensureOpen();

        return resultSetType;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        ensureOpen();

        for (ProtonSqlStatement s : connection.parse(sql, getConfig())) {
            this.batchStmts.add(s);
        }
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batchStmts.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        long[] largeUpdateCounts = executeLargeBatch();

        int len = largeUpdateCounts.length;
        int[] results = new int[len];
        for (int i = 0; i < len; i++) {
            results[i] = (int) largeUpdateCounts[i];
        }
        return results;
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        ensureOpen();
        if (batchStmts.isEmpty()) {
            throw SqlExceptionUtils.emptyBatchError();
        }

        boolean continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        long[] results = new long[batchStmts.size()];
        try {
            int i = 0;
            for (ProtonSqlStatement s : batchStmts) {
                try (ProtonResponse r = executeStatement(s, null, null, null); ResultSet rs = updateResult(s, r)) {
                    if (currentResult != null) {
                        throw SqlExceptionUtils.queryInBatchError(results);
                    }
                    results[i] = currentUpdateCount <= 0L ? 0L : currentUpdateCount;
                } catch (Exception e) {
                    results[i] = EXECUTE_FAILED;
                    if (!continueOnError) {
                        throw SqlExceptionUtils.batchUpdateError(e, results);
                    }
                    log.error("Faled to execute task %d of %d", i + 1, batchStmts.size(), e);
                } finally {
                    i++;
                }
            }
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        ensureOpen();

        switch (current) {
            case Statement.KEEP_CURRENT_RESULT:
                break;
            case Statement.CLOSE_CURRENT_RESULT:
            case Statement.CLOSE_ALL_RESULTS:
                if (currentResult != null) {
                    currentResult.close();
                }
                break;
            default:
                throw SqlExceptionUtils.clientError("Unknown statement constants: " + current);
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        ensureOpen();

        return new ProtonResultSet(request.getConfig().getDatabase(), "unknown", this, ProtonResponse.EMPTY);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        ensureOpen();

        return resultSetHoldability;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        ensureOpen();

        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        ensureOpen();

        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        ensureOpen();

        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        ensureOpen();

        return closeOnCompletion;
    }

    @Override
    public ProtonConnection getConnection() throws SQLException {
        ensureOpen();

        return connection;
    }

    @Override
    public ProtonConfig getConfig() {
        return request.getConfig();
    }

    @Override
    public ProtonRequest<?> getRequest() {
        return request;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface == ProtonRequest.class || super.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface == ProtonRequest.class ? iface.cast(request) : super.unwrap(iface);
    }
}
