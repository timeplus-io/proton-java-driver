package com.timeplus.proton.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonColumn;

public class ProtonArray implements Array {
    private final int columnIndex;
    private ProtonResultSet resultSet;

    protected ProtonArray(ProtonResultSet resultSet, int columnIndex) throws SQLException {
        this.resultSet = ProtonChecker.nonNull(resultSet, "ResultSet");
        resultSet.ensureRead(columnIndex);
        this.columnIndex = columnIndex;
    }

    protected void ensureValid() throws SQLException {
        if (resultSet == null) {
            throw SqlExceptionUtils.clientError("Cannot operate on a freed Array object");
        }
    }

    protected ProtonColumn getBaseColumn() {
        return resultSet.columns.get(columnIndex - 1).getArrayBaseColumn();
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        ensureValid();

        return getBaseColumn().getDataType().name();
    }

    @Override
    public int getBaseType() throws SQLException {
        ensureValid();

        // don't really want to pass type mapping to Array object...
        return JdbcTypeMapping.toJdbcType(null, getBaseColumn());
    }

    @Override
    public Object getArray() throws SQLException {
        ensureValid();

        return resultSet.getValue(columnIndex).asObject();
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        ensureValid();

        throw SqlExceptionUtils.unsupportedError("getArray not implemented");
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        ensureValid();

        throw SqlExceptionUtils.unsupportedError("getResultSet not implemented");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public void free() throws SQLException {
        this.resultSet = null;
    }
}
