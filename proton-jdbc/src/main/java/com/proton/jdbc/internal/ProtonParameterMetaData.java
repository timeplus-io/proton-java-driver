package com.proton.jdbc.internal;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonColumn;
import com.proton.client.ProtonUtils;
import com.proton.jdbc.JdbcTypeMapping;
import com.proton.jdbc.SqlExceptionUtils;
import com.proton.jdbc.JdbcWrapper;

public class ProtonParameterMetaData extends JdbcWrapper implements ParameterMetaData {
    protected final List<ProtonColumn> params;

    protected ProtonParameterMetaData(List<ProtonColumn> params) {
        this.params = ProtonChecker.nonNull(params, "Parameters");
    }

    protected ProtonColumn getParameter(int param) throws SQLException {
        if (param < 1 || param > params.size()) {
            throw SqlExceptionUtils.clientError(ProtonUtils
                    .format("Parameter index should between 1 and %d but we got %d", params.size(), param));
        }

        return params.get(param - 1);
    }

    @Override
    public int getParameterCount() throws SQLException {
        return params.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        if (p == null) {
            return ParameterMetaData.parameterNullableUnknown;
        }

        return p.isNullable() ? ParameterMetaData.parameterNullable : ParameterMetaData.parameterNoNulls;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return p != null && p.getDataType().isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return p != null ? p.getPrecision() : 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return p != null ? p.getScale() : 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return p != null ? JdbcTypeMapping.toJdbcType(null, p) : Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return p != null ? p.getOriginalTypeName() : "<unknown>";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        ProtonColumn p = getParameter(param);
        return (p != null ? p.getDataType().getObjectClass() : Object.class).getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
