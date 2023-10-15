package ru.yandex.proton;

import ru.yandex.proton.response.ProtonResponse;
import ru.yandex.proton.response.ProtonResponseSummary;
import ru.yandex.proton.settings.ProtonQueryParam;
import ru.yandex.proton.util.ProtonRowBinaryInputStream;
import ru.yandex.proton.util.ProtonStreamCallback;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;


public interface ProtonStatement extends Statement {

    ProtonResponse executeQueryProtonResponse(String sql) throws SQLException;

    ProtonResponse executeQueryProtonResponse(String sql, Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    ProtonResponse executeQueryProtonResponse(String sql,
                                                      Map<ProtonQueryParam, String> additionalDBParams,
                                                      Map<String, String> additionalRequestParams) throws SQLException;

    ProtonRowBinaryInputStream executeQueryProtonRowBinaryStream(String sql) throws SQLException;

    ProtonRowBinaryInputStream executeQueryProtonRowBinaryStream(String sql,
                                                                         Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    ProtonRowBinaryInputStream executeQueryProtonRowBinaryStream(String sql,
                                                                         Map<ProtonQueryParam, String> additionalDBParams,
                                                                         Map<String, String> additionalRequestParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    ResultSet executeQuery(String sql, Map<ProtonQueryParam, String> additionalDBParams, List<ProtonExternalData> externalData) throws SQLException;

    ResultSet executeQuery(String sql,
                           Map<ProtonQueryParam, String> additionalDBParams,
                           List<ProtonExternalData> externalData,
                           Map<String, String> additionalRequestParams) throws SQLException;

    @Deprecated
    void sendStream(InputStream content, String table, Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendStream(InputStream content, String table) throws SQLException;

    @Deprecated
    void sendRowBinaryStream(String sql, Map<ProtonQueryParam, String> additionalDBParams, ProtonStreamCallback callback) throws SQLException;

    @Deprecated
    void sendRowBinaryStream(String sql, ProtonStreamCallback callback) throws SQLException;

    @Deprecated
    void sendNativeStream(String sql, Map<ProtonQueryParam, String> additionalDBParams, ProtonStreamCallback callback) throws SQLException;

    @Deprecated
    void sendNativeStream(String sql, ProtonStreamCallback callback) throws SQLException;

    @Deprecated
    void sendCSVStream(InputStream content, String table, Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendCSVStream(InputStream content, String table) throws SQLException;

    @Deprecated
    void sendStreamSQL(InputStream content, String sql, Map<ProtonQueryParam, String> additionalDBParams) throws SQLException;

    @Deprecated
    void sendStreamSQL(InputStream content, String sql) throws SQLException;

    /**
     * Returns extended write-API, which simplifies uploading larger files or
     * data streams.
     *
     * @return a new {@link Writer} builder object which can be used to
     *         construct a request to the server
     */
    Writer write();

    ProtonResponseSummary getResponseSummary();
}
