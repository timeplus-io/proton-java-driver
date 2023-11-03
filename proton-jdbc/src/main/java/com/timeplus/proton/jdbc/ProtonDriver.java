package com.timeplus.proton.jdbc;

import java.io.Serializable;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Map.Entry;

import com.timeplus.proton.client.ProtonClient;
import com.timeplus.proton.client.ProtonVersion;
import com.timeplus.proton.client.config.ProtonClientOption;
import com.timeplus.proton.client.config.ProtonOption;
import com.timeplus.proton.client.logging.Logger;
import com.timeplus.proton.client.logging.LoggerFactory;
import com.timeplus.proton.jdbc.internal.ProtonConnectionImpl;
import com.timeplus.proton.jdbc.internal.ProtonJdbcUrlParser;

/**
 * JDBC driver for Proton. It takes a connection string like below for
 * connecting to Proton server:
 * {@code jdbc:proton://[<user>:<password>@]<server>[:<port>][/<db>][?parameter1=value1&parameter2=value2]}
 *
 * <p>
 * For examples:
 * <ul>
 * <li>{@code jdbc:proton://localhost:8123/system}</li>
 * <li>{@code jdbc:proton://admin:password@localhost/system?socket_time=30}</li>
 * <li>{@code jdbc:proton://localhost/system?protocol=grpc}</li>
 * </ul>
 */
public class ProtonDriver implements Driver {
    private static final Logger log = LoggerFactory.getLogger(ProtonDriver.class);

    private static final Map<Object, ProtonOption> clientSpecificOptions;

    static final String driverVersionString;
    static final ProtonVersion driverVersion;
    static final ProtonVersion specVersion;

    static final java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.timeplus.proton.jdbc");

    static {
        driverVersionString = ProtonDriver.class.getPackage().getImplementationVersion();
        driverVersion = ProtonVersion.of(driverVersionString);
        specVersion = ProtonVersion.of(ProtonDriver.class.getPackage().getSpecificationVersion());

        try {
            DriverManager.registerDriver(new ProtonDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

        log.debug("Proton Driver %s(JDBC: %s) registered", driverVersion, specVersion);

        // client-specific options
        Map<Object, ProtonOption> m = new LinkedHashMap<>();
        try {
            for (ProtonClient c : ServiceLoader.load(ProtonClient.class,
                    ProtonDriver.class.getClassLoader())) {
                Class<? extends ProtonOption> clazz = c.getOptionClass();
                if (clazz == null || clazz == ProtonClientOption.class) {
                    continue;
                }
                for (ProtonOption o : clazz.getEnumConstants()) {
                    m.put(o.getKey(), o);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to load client-specific options", e);
        }

        clientSpecificOptions = Collections.unmodifiableMap(m);
    }

    public static Map<ProtonOption, Serializable> toClientOptions(Properties props) {
        if (props == null || props.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<ProtonOption, Serializable> options = new HashMap<>();
        for (Entry<Object, Object> e : props.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                continue;
            }

            String key = e.getKey().toString();
            ProtonOption o = ProtonClientOption.fromKey(key);
            if (o == null) {
                o = clientSpecificOptions.get(key);
            }

            if (o != null) {
                options.put(o, ProtonOption.fromString(e.getValue().toString(), o.getValueType()));
            }
        }

        return options;
    }

    private DriverPropertyInfo create(ProtonOption option, Properties props) {
        DriverPropertyInfo propInfo = new DriverPropertyInfo(option.getKey(),
                props.getProperty(option.getKey(), String.valueOf(option.getEffectiveDefaultValue())));
        propInfo.required = false;
        propInfo.description = option.getDescription();
        propInfo.choices = null;

        Class<?> clazz = option.getValueType();
        if (Boolean.class == clazz || boolean.class == clazz) {
            propInfo.choices = new String[] { "true", "false" };
        } else if (clazz.isEnum()) {
            Object[] values = clazz.getEnumConstants();
            String[] names = new String[values.length];
            int index = 0;
            for (Object v : values) {
                names[index++] = ((Enum<?>) v).name();
            }
            propInfo.choices = names;
        }
        return propInfo;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && (url.startsWith(ProtonJdbcUrlParser.JDBC_PROTON_PREFIX)
                || url.startsWith(ProtonJdbcUrlParser.JDBC_ABBREVIATION_PREFIX));
    }

    @Override
    public ProtonConnection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        log.debug("Creating connection");
        return new ProtonConnectionImpl(url, info);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        try {
            info = ProtonJdbcUrlParser.parse(url, info).getProperties();
        } catch (Exception e) {
            log.error("Could not parse url %s", url, e);
        }

        List<DriverPropertyInfo> result = new ArrayList<>(ProtonClientOption.values().length * 2);
        for (ProtonClientOption option : ProtonClientOption.values()) {
            result.add(create(option, info));
        }

        // and then client-specific options
        for (ProtonOption option : clientSpecificOptions.values()) {
            result.add(create(option, info));
        }

        result.addAll(JdbcConfig.getDriverProperties());
        return result.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return driverVersion.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driverVersion.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }
}
