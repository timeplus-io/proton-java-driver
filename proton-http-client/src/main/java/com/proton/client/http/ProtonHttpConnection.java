package com.proton.client.http;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonCompression;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonCredentials;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonUtils;
import com.proton.client.config.ProtonClientOption;
import com.proton.client.data.ProtonExternalTable;
import com.proton.client.data.ProtonLZ4InputStream;
import com.proton.client.data.ProtonLZ4OutputStream;
import com.proton.client.http.config.ProtonHttpOption;

public abstract class ProtonHttpConnection implements AutoCloseable {
    protected static final int DEFAULT_BUFFER_SIZE = 8192;

    static String urlEncode(String str, Charset charset) {
        try {
            return URLEncoder.encode(str, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // should not happen
            throw new IllegalArgumentException(e);
        }
    }

    private static StringBuilder appendQueryParameter(StringBuilder builder, String key, String value) {
        return builder.append(urlEncode(key, StandardCharsets.UTF_8)).append('=')
                .append(urlEncode(value, StandardCharsets.UTF_8)).append('&');
    }

    static String buildQueryParams(ProtonRequest<?> request) {
        if (request == null) {
            return "";
        }

        ProtonConfig config = request.getConfig();
        StringBuilder builder = new StringBuilder();

        // start with custom query parameters first
        Map<String, String> customParams = ProtonUtils
                .getKeyValuePairs((String) config.getOption(ProtonHttpOption.CUSTOM_PARAMS));
        for (Entry<String, String> cp : customParams.entrySet()) {
            appendQueryParameter(builder, cp.getKey(), cp.getValue());
        }

        if (config.isCompressServerResponse()) {
            appendQueryParameter(builder, "compress", "1");
        }
        if (config.isDecompressClientRequet()) {
            appendQueryParameter(builder, "decompress", "1");
        }

        Map<String, Object> settings = request.getSettings();
        List<String> stmts = request.getStatements(false);
        String settingKey = "max_execution_time";
        if (config.getMaxExecutionTime() > 0 && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxExecutionTime()));
        }
        settingKey = "max_result_rows";
        if (config.getMaxResultRows() > 0L && !settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, String.valueOf(config.getMaxResultRows()));
            appendQueryParameter(builder, "result_overflow_mode", "break");
        }
        settingKey = "log_comment";
        if (!stmts.isEmpty() && (boolean) config.getOption(ProtonClientOption.LOG_LEADING_COMMENT)
                && !settings.containsKey(settingKey)) {
            String comment = ProtonUtils.getLeadingComment(stmts.get(0));
            if (!comment.isEmpty()) {
                appendQueryParameter(builder, settingKey, comment);
            }
        }
        settingKey = "extremes";
        if (!settings.containsKey(settingKey)) {
            appendQueryParameter(builder, settingKey, "0");
        }

        Optional<String> optionalValue = request.getSessionId();
        if (optionalValue.isPresent()) {
            appendQueryParameter(builder, "session_id", optionalValue.get());

            if (config.isSessionCheck()) {
                appendQueryParameter(builder, "session_check", "1");
            }
            if (config.getSessionTimeout() > 0) {
                // see default_session_timeout
                appendQueryParameter(builder, "session_timeout", String.valueOf(config.getSessionTimeout()));
            }
        }

        optionalValue = request.getQueryId();
        if (optionalValue.isPresent()) {
            appendQueryParameter(builder, "query_id", optionalValue.get());
        }

        for (Map.Entry<String, Object> entry : settings.entrySet()) {
            appendQueryParameter(builder, entry.getKey(), String.valueOf(entry.getValue()));
        }

        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    static String buildUrl(ProtonNode server, ProtonRequest<?> request) {
        ProtonConfig config = request.getConfig();

        StringBuilder builder = new StringBuilder();
        builder.append(config.isSsl() ? "https" : "http").append("://").append(server.getHost()).append(':')
                .append(server.getPort()).append('/');
        String context = (String) config.getOption(ProtonHttpOption.WEB_CONTEXT);
        if (context != null && !context.isEmpty()) {
            char prev = '/';
            for (int i = 0, len = context.length(); i < len; i++) {
                char ch = context.charAt(i);
                if (ch != '/' || ch != prev) {
                    builder.append(ch);
                }
                prev = ch;
            }

            if (prev != '/') {
                builder.append('/');
            }
        }

        String query = buildQueryParams(request);
        if (!query.isEmpty()) {
            builder.append('?').append(query);
        }

        return builder.toString();
    }

    protected final ProtonConfig config;
    protected final ProtonNode server;
    protected final Map<String, String> defaultHeaders;

    protected final String url;

    protected ProtonHttpConnection(ProtonNode server, ProtonRequest<?> request) {
        if (server == null || request == null) {
            throw new IllegalArgumentException("Non-null server and request are required");
        }

        this.config = request.getConfig();
        this.server = server;

        this.url = buildUrl(server, request);

        Map<String, String> map = new LinkedHashMap<>();
        // add customer headers
        map.putAll(ProtonUtils.getKeyValuePairs((String) config.getOption(ProtonHttpOption.CUSTOM_HEADERS)));
        map.put("Accept", "*/*");
        if (!(boolean) config.getOption(ProtonHttpOption.KEEP_ALIVE)) {
            map.put("Connection", "Close");
        }
        map.put("User-Agent", config.getClientName());

        ProtonCredentials credentials = server.getCredentials(config);
        if (credentials.useAccessToken()) {
            // TODO check if auth-scheme is available and supported
            map.put("Authorization", credentials.getAccessToken());
        } else {
            map.put("x-proton-user", credentials.getUserName());
            if (!ProtonChecker.isNullOrEmpty(credentials.getPassword())) {
                map.put("x-proton-key", credentials.getPassword());
            }
        }

        String database = server.getDatabase(config);
        if (!ProtonChecker.isNullOrEmpty(database)) {
            map.put("x-proton-database", database);
        }
        // Also, you can use the ‘default_format’ URL parameter
        map.put("x-proton-format", config.getFormat().name());
        if (config.isCompressServerResponse()) {
            map.put("Accept-Encoding", config.getCompressAlgorithmForServerResponse().encoding());
        }
        if (config.isDecompressClientRequet()
                && config.getDecompressAlgorithmForClientRequest() != ProtonCompression.LZ4) {
            map.put("Content-Encoding", config.getDecompressAlgorithmForClientRequest().encoding());
        }

        this.defaultHeaders = Collections.unmodifiableMap(map);
    }

    protected void closeQuietly() {
        try {
            close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected String getBaseUrl() {
        String baseUrl;
        int index = url.indexOf('?');
        if (index > 0) {
            baseUrl = url.substring(0, index);
        } else {
            baseUrl = url;
        }

        return baseUrl;
    }

    protected OutputStream getRequestOutputStream(OutputStream out) throws IOException {
        if (!config.isDecompressClientRequet()) {
            return out;
        }

        // TODO support more algorithms
        ProtonCompression algorithm = config.getDecompressAlgorithmForClientRequest();
        switch (algorithm) {
            case GZIP:
                out = new GZIPOutputStream(out, (int) config.getOption(ProtonClientOption.MAX_COMPRESS_BLOCK_SIZE));
                break;
            case LZ4:
                out = new ProtonLZ4OutputStream(out,
                        (int) config.getOption(ProtonClientOption.MAX_COMPRESS_BLOCK_SIZE));
                break;
            default:
                throw new UnsupportedOperationException("Unsupported compression algorithm: " + algorithm);
        }
        return out;
    }

    protected ProtonInputStream getResponseInputStream(InputStream in) throws IOException {
        Runnable afterClose = null;
        if (!isReusable()) {
            afterClose = this::closeQuietly;
        }
        ProtonInputStream chInput;
        if (config.isCompressServerResponse()) {
            // TODO support more algorithms
            ProtonCompression algorithm = config.getCompressAlgorithmForServerResponse();
            switch (algorithm) {
                case GZIP:
                    chInput = ProtonInputStream.of(new GZIPInputStream(in), config.getMaxBufferSize(), afterClose);
                    break;
                case LZ4:
                    chInput = new ProtonLZ4InputStream(in, afterClose);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported compression algorithm: " + algorithm);
            }
        } else {
            chInput = ProtonInputStream.of(in, config.getMaxBufferSize(), afterClose);
        }

        return chInput;
    }

    /**
     * Creates a merged map.
     *
     * @param requestHeaders request headers
     * @return
     */
    protected Map<String, String> mergeHeaders(Map<String, String> requestHeaders) {
        if (requestHeaders == null || requestHeaders.isEmpty()) {
            return defaultHeaders;
        }

        Map<String, String> merged = new LinkedHashMap<>();
        merged.putAll(defaultHeaders);
        for (Entry<String, String> header : requestHeaders.entrySet()) {
            if (header.getValue() == null) {
                merged.remove(header.getKey());
            } else {
                merged.put(header.getKey(), header.getValue());
            }
        }
        return merged;
    }

    /**
     * Pipes data from input stream to output stream. Input stream will be closed
     * but output stream will remain open.
     *
     * @param input      non-null input stream, which will be closed
     * @param output     non-null output stream, which will remain open
     * @param bufferSize buffer size, zero or negative number will be treated as
     *                   {@link #DEFAULT_BUFFER_SIZE}
     * @throws IOException when error occured reading from input stream or writing
     *                     data to output stream
     */
    protected void pipe(InputStream input, OutputStream output, int bufferSize) throws IOException {
        if (bufferSize <= 0) {
            bufferSize = DEFAULT_BUFFER_SIZE;
        }

        byte[] bytes = new byte[bufferSize];
        int counter = 0;
        try {
            while ((counter = input.read(bytes, 0, bufferSize)) >= 0) {
                output.write(bytes, 0, counter);
            }
            output.flush();
            input.close();
            input = null;
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * Posts query and data to server.
     *
     * @param query   non-blank query
     * @param data    optionally input stream for batch updating
     * @param tables  optionally external tables for query
     * @param headers optionally request headers
     * @return response
     * @throws IOException when error occured posting request and/or server failed
     *                     to respond
     */
    protected abstract ProtonHttpResponse post(String query, InputStream data, List<ProtonExternalTable> tables,
            Map<String, String> headers) throws IOException;

    /**
     * Checks whether the connection is reusable or not. This method will be called
     * in
     * {@link ProtonHttpClient#checkConnection(ProtonHttpConnection, ProtonNode, ProtonNode, ProtonRequest)}
     * for making a decision of whether to create a new connection. In addition to
     * that, if a connection is NOT reusable, it will be closed right after
     * corresponding ProtonResponse is closed.
     *
     * @return true if it's reusable; false otherwise
     */
    protected boolean isReusable() {
        return true;
    }

    /**
     * Sends a request to {@code <baseUrl>/ping} for liveness detection.
     *
     * @param timeout timeout in millisecond
     * @return true if server responded {@code Ok.}; false otherwise
     */
    public abstract boolean ping(int timeout);

    public ProtonHttpResponse update(String query) throws IOException {
        return post(query, null, null, null);
    }

    public ProtonHttpResponse update(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, headers);
    }

    public ProtonHttpResponse update(String query, InputStream data) throws IOException {
        return post(query, data, null, null);
    }

    public ProtonHttpResponse update(String query, InputStream data, Map<String, String> headers)
            throws IOException {
        return post(query, data, null, headers);
    }

    public ProtonHttpResponse query(String query) throws IOException {
        return post(query, null, null, null);
    }

    public ProtonHttpResponse query(String query, Map<String, String> headers) throws IOException {
        return post(query, null, null, headers);
    }

    public ProtonHttpResponse query(String query, List<ProtonExternalTable> tables) throws IOException {
        return post(query, null, tables, null);
    }

    public ProtonHttpResponse query(String query, List<ProtonExternalTable> tables, Map<String, String> headers)
            throws IOException {
        return post(query, null, tables, headers);
    }
}
