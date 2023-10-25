package com.timeplus.proton;

import com.timeplus.proton.settings.ProtonConnectionSettings;
import com.timeplus.proton.settings.ProtonProperties;
import com.timeplus.proton.settings.ProtonQueryParam;
import com.timeplus.proton.settings.DriverPropertyCreator;
import com.timeplus.proton.util.LogProxy;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.*;

import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

/**
 *
 * URL Format
 *
 * primitive for now
 *
 * jdbc:proton://host:port
 *
 * for example, jdbc:proton://localhost:8123
 *
 */
public class ProtonDriver implements Driver {

    private static final Logger log = LoggerFactory.getLogger(ProtonDriver.class);

    private static final Map<ProtonConnectionImpl, Boolean> connections = Collections.synchronizedMap(new WeakHashMap<>());

    static {
        ProtonDriver driver = new ProtonDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        log.warn("******************************************************************************************");
        log.warn("* This driver is DEPRECATED. Please use [com.proton.jdbc.ProtonDriver] instead.  *");
        log.warn("* Also everything in package [com.timeplus.proton] will be removed starting from 0.4.0. *");
        log.warn("******************************************************************************************");
    }

    @Override
    public ProtonConnection connect(String url, Properties info) throws SQLException {
        return connect(url, new ProtonProperties(info));
    }

    public ProtonConnection connect(String url, ProtonProperties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        log.debug("Creating connection");
        ProtonConnectionImpl connection = new ProtonConnectionImpl(url, properties);
        registerConnection(connection);
        return LogProxy.wrap(ProtonConnection.class, connection);
    }

    private void registerConnection(ProtonConnectionImpl connection) {
        connections.put(connection, Boolean.TRUE);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(ProtonJdbcUrlParser.JDBC_PROTON_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties copy = new Properties(info);
        Properties properties;
        try {
            properties = ProtonJdbcUrlParser.parse(url, copy).asProperties();
        } catch (Exception ex) {
            properties = copy;
            log.error("could not parse url %s", url, ex);
        }
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(ProtonQueryParam.values().length
                + ProtonConnectionSettings.values().length);
        result.addAll(dumpProperties(ProtonQueryParam.values(), properties));
        result.addAll(dumpProperties(ProtonConnectionSettings.values(), properties));
        return result.toArray(new DriverPropertyInfo[0]);
    }

    private List<DriverPropertyInfo> dumpProperties(DriverPropertyCreator[] creators, Properties info) {
        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>(creators.length);
        for (DriverPropertyCreator creator : creators) {
            result.add(creator.createDriverPropertyInfo(info));
        }
        return result;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Schedules connections cleaning at a rate. Turned off by default.
     * See https://hc.apache.org/httpcomponents-client-4.5.x/tutorial/html/connmgmt.html#d5e418
     *
     * @param rate period when checking would be performed
     * @param timeUnit time unit of rate
     */
    public void scheduleConnectionsCleaning(int rate, TimeUnit timeUnit){
        ScheduledConnectionCleaner.INSTANCE.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    for (ProtonConnectionImpl connection : connections.keySet()) {
                        connection.cleanConnections();
                    }
                } catch (Exception e){
                    log.error("error evicting connections", e);
                }
            }
        }, 0, rate, timeUnit);
    }

    static class ScheduledConnectionCleaner {
        static final ScheduledExecutorService INSTANCE = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());

        static class DaemonThreadFactory implements ThreadFactory {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setDaemon(true);
                return thread;
            }
        }
    }
}
