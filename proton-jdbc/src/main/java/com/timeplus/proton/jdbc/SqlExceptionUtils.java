package com.timeplus.proton.jdbc;

import java.net.ConnectException;
import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import com.timeplus.proton.client.ProtonException;

/**
 * Helper class for building {@link SQLException}.
 */
public final class SqlExceptionUtils {
    public static final String SQL_STATE_CLIENT_ERROR = "HY000";
    public static final String SQL_STATE_CONNECTION_EXCEPTION = "08000";
    public static final String SQL_STATE_SQL_ERROR = "07000";
    public static final String SQL_STATE_NO_DATA = "02000";
    public static final String SQL_STATE_INVALID_SCHEMA = "3F000";
    public static final String SQL_STATE_INVALID_TX_STATE = "25000";
    public static final String SQL_STATE_DATA_EXCEPTION = "22000";
    public static final String SQL_STATE_FEATURE_NOT_SUPPORTED = "0A000";

    private SqlExceptionUtils() {
    }

    // https://en.wikipedia.org/wiki/SQLSTATE
    private static String toSqlState(ProtonException e) {
        String sqlState;
        if (e.getErrorCode() == ProtonException.ERROR_NETWORK
                || e.getErrorCode() == ProtonException.ERROR_POCO) {
            sqlState = SQL_STATE_CONNECTION_EXCEPTION;
        } else if (e.getErrorCode() == 0) {
            sqlState = e.getCause() instanceof ConnectException ? SQL_STATE_CONNECTION_EXCEPTION
                    : SQL_STATE_CLIENT_ERROR;
        } else {
            sqlState = e.getCause() instanceof ConnectException ? SQL_STATE_CONNECTION_EXCEPTION : SQL_STATE_SQL_ERROR;
        }

        return sqlState;
    }

    public static SQLException clientError(String message) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, null);
    }

    public static SQLException clientError(Throwable e) {
        return e != null ? new SQLException(e.getMessage(), SQL_STATE_CLIENT_ERROR, e) : unknownError();
    }

    public static SQLException clientError(String message, Throwable e) {
        return new SQLException(message, SQL_STATE_CLIENT_ERROR, e);
    }

    public static SQLException handle(ProtonException e) {
        return e != null ? new SQLException(e.getMessage(), toSqlState(e), e.getErrorCode(), e.getCause())
                : unknownError();
    }

    public static SQLException handle(Throwable e) {
        if (e == null) {
            return unknownError();
        } else if (e instanceof ProtonException) {
            return handle((ProtonException) e);
        } else if (e instanceof SQLException) {
            return (SQLException) e;
        }

        Throwable cause = e.getCause();
        if (cause instanceof ProtonException) {
            return handle((ProtonException) cause);
        } else if (e instanceof SQLException) {
            return (SQLException) cause;
        } else if (cause == null) {
            cause = e;
        }

        return new SQLException(cause);
    }

    public static BatchUpdateException batchUpdateError(Throwable e, long[] updateCounts) {
        if (e == null) {
            return new BatchUpdateException("Something went wrong when performing batch update", SQL_STATE_CLIENT_ERROR,
                    0, updateCounts, null);
        } else if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (e instanceof ProtonException) {
            return batchUpdateError(e, updateCounts);
        } else if (e instanceof SQLException) {
            SQLException sqlExp = (SQLException) e;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        }

        Throwable cause = e.getCause();
        if (e instanceof BatchUpdateException) {
            return (BatchUpdateException) e;
        } else if (cause instanceof ProtonException) {
            return batchUpdateError(cause, updateCounts);
        } else if (e instanceof SQLException) {
            SQLException sqlExp = (SQLException) e;
            return new BatchUpdateException(sqlExp.getMessage(), sqlExp.getSQLState(), sqlExp.getErrorCode(),
                    updateCounts, null);
        } else if (cause == null) {
            cause = e;
        }

        return new BatchUpdateException("Unexpected error", SQL_STATE_SQL_ERROR, 0, updateCounts, cause);
    }

    public static SQLException emptyBatchError() {
        return clientError("Please call addBatch method at least once before batch execution");
    }

    public static BatchUpdateException queryInBatchError(int[] updateCounts) {
        return new BatchUpdateException("Query is not allow in batch update", SQL_STATE_CLIENT_ERROR, updateCounts);
    }

    public static BatchUpdateException queryInBatchError(long[] updateCounts) {
        return new BatchUpdateException("Query is not allow in batch update", SQL_STATE_CLIENT_ERROR, 0, updateCounts,
                null);
    }

    public static SQLException undeterminedExecutionError() {
        return clientError("Please either call clearBatch() to clean up context first, or use executeBatch() instead");
    }

    public static SQLException forCancellation(Exception e) {
        Throwable cause = e.getCause();
        if (cause == null) {
            cause = e;
        }

        // operation canceled
        return new SQLException(e.getMessage(), "HY008", ProtonException.ERROR_ABORTED, cause);
    }

    public static SQLFeatureNotSupportedException unsupportedError(String message) {
        return new SQLFeatureNotSupportedException(message, SQL_STATE_FEATURE_NOT_SUPPORTED);
    }

    public static SQLException unknownError() {
        return new SQLException("Unknown error", SQL_STATE_CLIENT_ERROR);
    }
}
