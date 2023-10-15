package com.proton.client.http;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonResponseSummary;
import com.proton.client.ProtonUtils;
import com.proton.client.config.ProtonClientOption;
import com.proton.client.config.ProtonOption;

public class ProtonHttpResponse {
    private static long getLongValue(Map<String, String> map, String key) {
        String value = map.get(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // ignore error
            }
        }
        return 0L;
    }

    private final ProtonHttpConnection connection;
    private final ProtonInputStream input;

    protected final String serverDisplayName;
    protected final String queryId;
    protected final ProtonFormat format;
    protected final TimeZone timeZone;

    protected final ProtonResponseSummary summary;

    protected ProtonConfig getConfig(ProtonRequest<?> request) {
        ProtonConfig config = request.getConfig();
        if (format != null && format != config.getFormat()) {
            Map<ProtonOption, Serializable> options = new HashMap<>();
            options.putAll(config.getAllOptions());
            options.put(ProtonClientOption.FORMAT, format);
            config = new ProtonConfig(options, config.getDefaultCredentials(), config.getNodeSelector(),
                    config.getMetricRegistry());
        }
        return config;
    }

    public ProtonHttpResponse(ProtonHttpConnection connection, ProtonInputStream input,
            String serverDisplayName, String queryId, String summary, ProtonFormat format, TimeZone timeZone) {
        if (connection == null || input == null) {
            throw new IllegalArgumentException("Non-null connection and input stream are required");
        }

        this.connection = connection;
        this.input = input;

        this.serverDisplayName = !ProtonChecker.isNullOrEmpty(serverDisplayName) ? serverDisplayName
                : connection.server.getHost();
        this.queryId = !ProtonChecker.isNullOrEmpty(queryId) ? queryId : "";
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        Map<String, String> map = (Map<String, String>) ProtonUtils
                .parseJson(!ProtonChecker.isNullOrEmpty(summary) ? summary : "{}");
        // discard those x-proton-progress headers
        this.summary = new ProtonResponseSummary(
                new ProtonResponseSummary.Progress(getLongValue(map, "read_rows"), getLongValue(map, "read_bytes"),
                        getLongValue(map, "total_rows_to_read"), getLongValue(map, "written_rows"),
                        getLongValue(map, "written_bytes")),
                null);

        this.format = format != null ? format : connection.config.getFormat();
        this.timeZone = timeZone != null ? timeZone : connection.config.getServerTimeZone();
    }

    public ProtonInputStream getInputStream() {
        return input;
    }
}
