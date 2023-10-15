package com.proton.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonUtils;
import com.proton.client.data.ProtonExternalTable;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;
import com.proton.jdbc.ProtonPreparedStatement;
import com.proton.jdbc.SqlExceptionUtils;
import com.proton.jdbc.parser.ProtonSqlStatement;

public class TableBasedPreparedStatement extends AbstractPreparedStatement implements ProtonPreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(TableBasedPreparedStatement.class);

    private static final String ERROR_SET_TABLE = "Please use setObject(ProtonExternalTable) method instead";

    private final ProtonSqlStatement parsedStmt;
    private final List<String> tables;
    private final ProtonExternalTable[] values;

    private final List<List<ProtonExternalTable>> batch;

    protected TableBasedPreparedStatement(ProtonConnectionImpl connection, ProtonRequest<?> request,
            ProtonSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency,
            int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        Set<String> set = parsedStmt != null ? parsedStmt.getTempTables() : null;
        if (set == null) {
            throw SqlExceptionUtils.clientError("Non-null table list is required");
        }

        this.parsedStmt = parsedStmt;
        int size = set.size();
        this.tables = new ArrayList<>(size);
        this.tables.addAll(set);
        values = new ProtonExternalTable[size];
        batch = new LinkedList<>();
    }

    protected void ensureParams() throws SQLException {
        List<String> list = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (values[i] == null) {
                list.add(tables.get(i));
            }
        }

        if (!list.isEmpty()) {
            throw SqlExceptionUtils.clientError(ProtonUtils.format("Missing table(s): %s", list));
        }
    }

    @Override
    public long[] executeAny(boolean asBatch) throws SQLException {
        ensureOpen();
        boolean continueOnError = false;
        if (asBatch) {
            if (batch.isEmpty()) {
                throw SqlExceptionUtils.emptyBatchError();
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            if (!batch.isEmpty()) {
                throw SqlExceptionUtils.undeterminedExecutionError();
            }
            addBatch();
        }

        long[] results = new long[batch.size()];
        int index = 0;
        try {
            String sql = getSql();
            for (List<ProtonExternalTable> list : batch) {
                try (ProtonResponse r = executeStatement(sql, null, list, null);
                        ResultSet rs = updateResult(parsedStmt, r)) {
                    if (asBatch && getResultSet() != null) {
                        throw SqlExceptionUtils.queryInBatchError(results);
                    }
                    long rows = getLargeUpdateCount();
                    results[index] = rows > 0L ? rows : 0L;
                } catch (Exception e) {
                    results[index] = EXECUTE_FAILED;
                    if (!continueOnError) {
                        throw SqlExceptionUtils.batchUpdateError(e, results);
                    }
                    log.error("Failed to execute batch insert at %d of %d", index + 1, batch.size(), e);
                }
                index++;
            }
        } finally {
            clearBatch();
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return values.length;
    }

    protected String getSql() {
        // why? because request can be modified so it might not always same as
        // parsedStmt.getSQL()
        return getRequest().getStatements(false).get(0);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        ProtonSqlStatement stmt = new ProtonSqlStatement(getSql());
        return updateResult(parsedStmt, executeStatement(stmt, null, Arrays.asList(values), null));
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        try (ProtonResponse r = executeStatement(getSql(), null, Arrays.asList(values), null)) {
            updateResult(parsedStmt, r);
            return getLargeUpdateCount();
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void clearParameters() throws SQLException {
        ensureOpen();

        for (int i = 0, len = values.length; i < len; i++) {
            values[i] = null;
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        ensureOpen();

        if (x instanceof ProtonExternalTable) {
            int idx = toArrayIndex(parameterIndex);
            values[idx] = (ProtonExternalTable) x;
        } else {
            throw SqlExceptionUtils.clientError("Only ProtonExternalTable is allowed");
        }
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();
        if (!batch.isEmpty()) {
            throw SqlExceptionUtils.undeterminedExecutionError();
        }

        ProtonSqlStatement stmt = new ProtonSqlStatement(getSql());
        updateResult(parsedStmt, executeStatement(stmt, null, Arrays.asList(values), null));
        return getResultSet() != null;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        ensureParams();
        List<ProtonExternalTable> list = new ArrayList<>(values.length);
        for (ProtonExternalTable v : values) {
            list.add(v);
        }
        batch.add(Collections.unmodifiableList(list));
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw SqlExceptionUtils.clientError(ERROR_SET_TABLE);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }
}
