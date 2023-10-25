package com.proton.jdbc.internal;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonParameterizedQuery;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValue;
import com.proton.client.ProtonValues;
import com.proton.client.data.ProtonDateTimeValue;
import com.proton.client.data.ProtonDateValue;
import com.proton.client.data.ProtonStringValue;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;
import com.proton.jdbc.ProtonPreparedStatement;
import com.proton.jdbc.JdbcParameterizedQuery;
import com.proton.jdbc.JdbcTypeMapping;
import com.proton.jdbc.SqlExceptionUtils;
import com.proton.jdbc.parser.ProtonSqlStatement;

public class SqlBasedPreparedStatement extends AbstractPreparedStatement implements ProtonPreparedStatement {
    private static final Logger log = LoggerFactory.getLogger(SqlBasedPreparedStatement.class);

    private final Calendar defaultCalendar;
    private final TimeZone preferredTimeZone;
    private final ZoneId timeZoneForDate;
    private final ZoneId timeZoneForTs;

    private final ProtonSqlStatement parsedStmt;
    private final String insertValuesQuery;
    private final ProtonParameterizedQuery preparedQuery;
    private final ProtonValue[] templates;
    private final String[] values;
    private final List<String[]> batch;
    private final StringBuilder builder;

    private int counter;

    protected SqlBasedPreparedStatement(ProtonConnectionImpl connection, ProtonRequest<?> request,
            ProtonSqlStatement parsedStmt, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        super(connection, request, resultSetType, resultSetConcurrency, resultSetHoldability);

        ProtonConfig config = getConfig();
        defaultCalendar = connection.getDefaultCalendar();
        preferredTimeZone = config.getUseTimeZone();
        timeZoneForTs = preferredTimeZone.toZoneId();
        timeZoneForDate = config.isUseServerTimeZoneForDates() ? timeZoneForTs : null;

        this.parsedStmt = parsedStmt;
        String valuesExpr = null;
        ProtonParameterizedQuery parsedValuesExpr = null;
        String prefix = null;
        if (parsedStmt.hasValues()) { // consolidate multiple inserts into one
            valuesExpr = parsedStmt.getContentBetweenKeywords(ProtonSqlStatement.KEYWORD_VALUES_START,
                    ProtonSqlStatement.KEYWORD_VALUES_END);
            if (ProtonChecker.isNullOrBlank(valuesExpr)) {
                log.warn(
                        "Please consider to use one and only one values expression, for example: use 'values(?)' instead of 'values(?),(?)'.");
            } else {
                valuesExpr += ")";
                prefix = parsedStmt.getSQL().substring(0,
                        parsedStmt.getPositions().get(ProtonSqlStatement.KEYWORD_VALUES_START));
                if (connection.getJdbcConfig().useNamedParameter()) {
                    parsedValuesExpr = ProtonParameterizedQuery.of(config, valuesExpr);
                } else {
                    parsedValuesExpr = JdbcParameterizedQuery.of(config, valuesExpr);
                }
            }
        }

        preparedQuery = parsedValuesExpr == null ? request.getPreparedQuery() : parsedValuesExpr;

        templates = preparedQuery.getParameterTemplates();

        values = new String[templates.length];
        batch = new LinkedList<>();
        builder = new StringBuilder();
        if ((insertValuesQuery = prefix) != null) {
            builder.append(insertValuesQuery);
        }

        counter = 0;
    }

    protected void ensureParams() throws SQLException {
        List<String> columns = new ArrayList<>();
        for (int i = 0, len = values.length; i < len; i++) {
            if (values[i] == null) {
                columns.add(String.valueOf(i + 1));
            }
        }

        if (!columns.isEmpty()) {
            throw SqlExceptionUtils.clientError(ProtonUtils.format("Missing parameter(s): %s", columns));
        }
    }

    @Override
    protected long[] executeAny(boolean asBatch) throws SQLException {
        ensureOpen();
        boolean continueOnError = false;
        if (asBatch) {
            if (counter < 1) {
                throw SqlExceptionUtils.emptyBatchError();
            }
            continueOnError = getConnection().getJdbcConfig().isContinueBatchOnError();
        } else {
            if (counter != 0) {
                throw SqlExceptionUtils.undeterminedExecutionError();
            }
            addBatch();
        }

        long[] results = new long[counter];
        ProtonResponse r = null;
        if (builder.length() > 0) { // insert ... values
            long rows = 0L;
            try {
                r = executeStatement(builder.toString(), null, null, null);
                updateResult(parsedStmt, r);
                if (asBatch && getResultSet() != null) {
                    throw SqlExceptionUtils.queryInBatchError(results);
                }
                rows = r.getSummary().getWrittenRows();
                // no effective rows for update and delete, and the number for insertion is not
                // accurate as well
                // if (rows > 0L && rows != counter) {
                // log.warn("Expect %d rows being inserted but only got %d", counter, rows);
                // }
                // FIXME needs to enhance http client before getting back to this
                Arrays.fill(results, 1);
            } catch (Exception e) {
                // just a wild guess...
                if (rows < 1) {
                    results[0] = EXECUTE_FAILED;
                } else {
                    if (rows >= counter) {
                        rows = counter;
                    }
                    for (int i = 0, len = (int) rows - 1; i < len; i++) {
                        results[i] = 1;
                    }
                    results[(int) rows] = EXECUTE_FAILED;
                }

                if (!continueOnError) {
                    throw SqlExceptionUtils.batchUpdateError(e, results);
                }
                log.error("Failed to execute batch insertion of %d records", counter, e);
            } finally {
                if (asBatch && r != null) {
                    r.close();
                }
                clearBatch();
            }
        } else {
            int index = 0;
            try {
                for (String[] params : batch) {
                    builder.setLength(0);
                    preparedQuery.apply(builder, params);
                    try {
                        r = executeStatement(builder.toString(), null, null, null);
                        updateResult(parsedStmt, r);
                        if (asBatch && getResultSet() != null) {
                            throw SqlExceptionUtils.queryInBatchError(results);
                        }
                        int count = getUpdateCount();
                        results[index] = count > 0 ? count : 0;
                    } catch (Exception e) {
                        results[index] = EXECUTE_FAILED;
                        if (!continueOnError) {
                            throw SqlExceptionUtils.batchUpdateError(e, results);
                        }
                        log.error("Failed to execute batch insert at %d of %d", index + 1, counter, e);
                    } finally {
                        index++;
                        if (asBatch && r != null) {
                            r.close();
                        }
                    }
                }
            } finally {
                clearBatch();
            }
        }

        return results;
    }

    @Override
    protected int getMaxParameterIndex() {
        return templates.length;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        ensureParams();

        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Query failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
        }
        ResultSet rs = getResultSet();
        return rs == null ? newEmptyResultSet() : rs;
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        ensureParams();

        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Update failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
        }
        return getLargeUpdateCount();
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = String.valueOf(x);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = ProtonValues.convertToQuotedString(x);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value == null) {
            templates[idx] = value = ProtonStringValue.ofNull();
        }
        values[idx] = value.update(x).toSqlExpression();
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

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.update(x);
            values[idx] = value.toSqlExpression();
        } else {
            if (x instanceof ProtonValue) {
                value = (ProtonValue) x;
                templates[idx] = value;
                values[idx] = value.toSqlExpression();
            } else {
                values[idx] = ProtonValues.convertToSqlExpression(x);
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        ensureParams();

        if (executeAny(false)[0] == EXECUTE_FAILED) {
            throw new SQLException("Execution failed", SqlExceptionUtils.SQL_STATE_SQL_ERROR);
        }
        return getResultSet() != null;
    }

    @Override
    public void addBatch() throws SQLException {
        ensureOpen();

        if (builder.length() > 0) {
            int index = 1;
            for (String v : values) {
                if (v == null) {
                    throw SqlExceptionUtils
                            .clientError(ProtonUtils.format("Missing value for parameter #%d", index));
                }
                index++;
            }
            preparedQuery.apply(builder, values);
        } else {
            int len = values.length;
            String[] newValues = new String[len];
            for (int i = 0; i < len; i++) {
                String v = values[i];
                if (v == null) {
                    throw SqlExceptionUtils
                            .clientError(ProtonUtils.format("Missing value for parameter #%d", i + 1));
                } else {
                    newValues[i] = v;
                }
            }
            batch.add(newValues);
        }
        counter++;
        clearParameters();
    }

    @Override
    public void clearBatch() throws SQLException {
        ensureOpen();

        this.batch.clear();
        this.builder.setLength(0);
        if (insertValuesQuery != null) {
            this.builder.append(insertValuesQuery);
        }

        this.counter = 0;
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        Object array = x != null ? x.getArray() : x;
        values[idx] = array != null ? ProtonValues.convertToSqlExpression(array)
                : ProtonValues.EMPTY_ARRAY_EXPR;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ProtonValues.NULL_EXPR;
            return;
        }

        LocalDate d;
        if (cal == null) {
            cal = defaultCalendar;
        }
        ZoneId tz = cal.getTimeZone().toZoneId();
        if (timeZoneForDate == null || tz.equals(timeZoneForDate)) {
            d = x.toLocalDate();
        } else {
            Calendar c = (Calendar) cal.clone();
            c.setTime(x);
            d = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForDate).toLocalDate();
        }

        ProtonValue value = templates[idx];
        if (value == null) {
            value = ProtonDateValue.ofNull();
        }
        values[idx] = value.update(d).toSqlExpression();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ProtonValues.NULL_EXPR;
            return;
        }

        LocalTime t;
        if (cal == null) {
            cal = defaultCalendar;
        }
        ZoneId tz = cal.getTimeZone().toZoneId();
        if (tz.equals(timeZoneForTs)) {
            t = x.toLocalTime();
        } else {
            Calendar c = (Calendar) cal.clone();
            c.setTime(x);
            t = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForTs).toLocalTime();
        }

        ProtonValue value = templates[idx];
        if (value == null) {
            value = ProtonDateValue.ofNull();
        }
        values[idx] = value.update(t).toSqlExpression();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        if (x == null) {
            values[idx] = ProtonValues.NULL_EXPR;
            return;
        }

        LocalDateTime dt;
        if (cal == null) {
            cal = defaultCalendar;
        }
        ZoneId tz = cal.getTimeZone().toZoneId();
        if (tz.equals(timeZoneForTs)) {
            dt = x.toLocalDateTime();
        } else {
            Calendar c = (Calendar) cal.clone();
            c.setTime(x);
            dt = c.toInstant().atZone(tz).withZoneSameInstant(timeZoneForTs).toLocalDateTime();
        }

        ProtonValue value = templates[idx];
        if (value == null) {
            value = ProtonDateTimeValue.ofNull(dt.getNano() > 0 ? 9 : 0, preferredTimeZone);
        }
        values[idx] = value.update(dt).toSqlExpression();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value != null) {
            value.resetToNullOrEmpty();
            values[idx] = value.toSqlExpression();
        } else {
            values[idx] = ProtonValues.NULL_EXPR;
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        ensureOpen();

        int idx = toArrayIndex(parameterIndex);
        ProtonValue value = templates[idx];
        if (value == null) {
            value = ProtonValues.newValue(getConfig(), JdbcTypeMapping.fromJdbcType(targetSqlType, scaleOrLength));
            templates[idx] = value;
        }

        value.update(x);
        values[idx] = value.toSqlExpression();
    }
}
