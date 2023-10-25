package com.timeplus.proton;

import com.timeplus.proton.response.ProtonResponse;
import com.timeplus.proton.settings.ProtonQueryParam;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;


public interface ProtonPreparedStatement extends PreparedStatement, ProtonStatement {
    ProtonResponse executeQueryProtonResponse() throws SQLException;

    ProtonResponse executeQueryProtonResponse(Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    void setArray(int parameterIndex, Collection collection) throws SQLException;

    void setArray(int parameterIndex, Object[] array) throws SQLException;

    ResultSet executeQuery(Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(Map<ProtonQueryParam, String> additionalDBParams, List<ProtonExternalData> externalData) throws SQLException;

    int[] executeBatch(Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    String asSql();
}
