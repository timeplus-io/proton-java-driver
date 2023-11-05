package com.timeplus.proton.jdbc;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

import com.timeplus.proton.client.ProtonChecker;

public class ProtonStruct implements Struct {
    private final String typeName;
    private final Object[] values;

    protected ProtonStruct(String typeName, Object[] values) {
        this.typeName = ProtonChecker.nonNull(typeName, "SQLTypeName");
        this.values = ProtonChecker.nonNull(values, "values");
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return values;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return getAttributes();
    }
}
