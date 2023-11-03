package com.timeplus.proton.jdbc;

import javax.sql.DataSource;

import com.timeplus.proton.client.config.ProtonDefaults;
import com.timeplus.proton.jdbc.internal.ProtonConnectionImpl;
import com.timeplus.proton.jdbc.internal.ProtonJdbcUrlParser;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class ProtonDataSource extends JdbcWrapper implements DataSource {
    private final String url;

    protected final ProtonDriver driver;
    protected final ProtonJdbcUrlParser.ConnectionInfo connInfo;

    protected PrintWriter printWriter;
    protected int loginTimeoutSeconds = 0;

    public ProtonDataSource(String url) throws SQLException {
        this(url, new Properties());
    }

    public ProtonDataSource(String url, Properties properties) throws SQLException {
        if (url == null) {
            throw new IllegalArgumentException("Incorrect Proton jdbc url. It must be not null");
        }
        this.url = url;

        this.driver = new ProtonDriver();
        this.connInfo = ProtonJdbcUrlParser.parse(url, properties);
    }

    @Override
    public ProtonConnection getConnection() throws SQLException {
        return new ProtonConnectionImpl(connInfo);
    }

    @Override
    public ProtonConnection getConnection(String username, String password) throws SQLException {
        if (username == null || username.isEmpty()) {
            throw SqlExceptionUtils.clientError("Non-empty user name is required");
        }

        if (password == null) {
            password = "";
        }

        Properties props = new Properties(connInfo.getProperties());
        props.setProperty(ProtonDefaults.USER.getKey(), username);
        props.setProperty(ProtonDefaults.PASSWORD.getKey(), password);
        return driver.connect(url, props);
    }

    public String getHost() {
        return connInfo.getServer().getHost();
    }

    public int getPort() {
        return connInfo.getServer().getPort();
    }

    public String getDatabase() {
        return connInfo.getServer().getDatabase()
                .orElse((String) ProtonDefaults.DATABASE.getEffectiveDefaultValue());
    }

    // public String getUrl() {
    // return url;
    // }

    // public Properties getProperties() {
    // return connInfo.getProperties();
    // }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ProtonDriver.parentLogger;
    }
}
