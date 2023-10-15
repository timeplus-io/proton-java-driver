package com.proton.client.http;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonNode;
import com.proton.client.ProtonRequest;
import com.proton.client.ProtonSslContextProvider;
import com.proton.client.config.ProtonSslMode;
import com.proton.client.data.ProtonExternalTable;
import com.proton.client.http.config.ProtonHttpOption;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

public class HttpUrlConnectionImpl extends ProtonHttpConnection {
    private static final Logger log = LoggerFactory.getLogger(HttpUrlConnectionImpl.class);

    private final HttpURLConnection conn;

    private ProtonHttpResponse buildResponse() throws IOException {
        // x-proton-server-display-name: xxx
        // x-proton-query-id: xxx
        // x-proton-format: RowBinaryWithNamesAndTypes
        // x-proton-timezone: UTC
        // x-proton-summary:
        // {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
        String displayName = getResponseHeader("x-proton-server-display-name", server.getHost());
        String queryId = getResponseHeader("x-proton-query-id", "");
        String summary = getResponseHeader("x-proton-summary", "{}");

        ProtonFormat format = config.getFormat();
        TimeZone timeZone = config.getServerTimeZone();
        // queryId, format and timeZone are only available for queries
        if (!ProtonChecker.isNullOrEmpty(queryId)) {
            String value = getResponseHeader("x-proton-format", "");
            format = !ProtonChecker.isNullOrEmpty(value) ? ProtonFormat.valueOf(value)
                    : format;
            value = getResponseHeader("x-proton-timezone", "");
            timeZone = !ProtonChecker.isNullOrEmpty(value) ? TimeZone.getTimeZone(value)
                    : timeZone;
        }

        return new ProtonHttpResponse(this, getResponseInputStream(conn.getInputStream()), displayName, queryId,
                summary, format, timeZone);
    }

    private HttpURLConnection newConnection(String url, boolean post) throws IOException {
        HttpURLConnection newConn = (HttpURLConnection) new URL(url).openConnection();

        if ((newConn instanceof HttpsURLConnection) && config.isSsl()) {
            HttpsURLConnection secureConn = (HttpsURLConnection) newConn;
            SSLContext sslContext = ProtonSslContextProvider.getProvider().getSslContext(SSLContext.class, config)
                    .orElse(null);
            HostnameVerifier verifier = config.getSslMode() == ProtonSslMode.STRICT
                    ? HttpsURLConnection.getDefaultHostnameVerifier()
                    : (hostname, session) -> true;

            secureConn.setHostnameVerifier(verifier);
            secureConn.setSSLSocketFactory(sslContext.getSocketFactory());
        }

        if (post) {
            newConn.setInstanceFollowRedirects(true);
            newConn.setRequestMethod("POST");
        }
        newConn.setUseCaches(false);
        newConn.setAllowUserInteraction(false);
        newConn.setDoInput(true);
        newConn.setDoOutput(true);
        newConn.setConnectTimeout(config.getConnectionTimeout());
        newConn.setReadTimeout(config.getSocketTimeout());

        return newConn;
    }

    private String getResponseHeader(String header, String defaultValue) {
        String value = conn.getHeaderField(header);
        return value != null ? value : defaultValue;
    }

    private void setHeaders(HttpURLConnection conn, Map<String, String> headers) {
        headers = mergeHeaders(headers);

        if (headers == null || headers.isEmpty()) {
            return;
        }

        for (Entry<String, String> header : headers.entrySet()) {
            conn.setRequestProperty(header.getKey(), header.getValue());
        }
    }

    private void checkResponse(HttpURLConnection conn) throws IOException {
        if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
            // TODO get exception from response header, for example:
            // x-proton-exception-code: 47
            StringBuilder builder = new StringBuilder();
            try (Reader reader = new InputStreamReader(getResponseInputStream(conn.getErrorStream()),
                    StandardCharsets.UTF_8)) {
                int c = 0;
                while ((c = reader.read()) != -1) {
                    builder.append((char) c);
                }
            } catch (IOException e) {
                log.warn("Error while reading error message", e);
            }

            throw new IOException(builder.toString());
        }
    }

    protected HttpUrlConnectionImpl(ProtonNode server, ProtonRequest<?> request, ExecutorService executor)
            throws IOException {
        super(server, request);

        conn = newConnection(url, true);
    }

    @Override
    protected boolean isReusable() {
        return false;
    }

    @Override
    protected ProtonHttpResponse post(String sql, InputStream data, List<ProtonExternalTable> tables,
            Map<String, String> headers) throws IOException {
        String boundary = null;
        if (tables != null && !tables.isEmpty()) {
            boundary = UUID.randomUUID().toString();
            conn.setRequestProperty("Content-Type", "multipart/form-data; boundary=" + boundary);
        } else {
            conn.setRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        }
        setHeaders(conn, headers);

        try (OutputStream out = getRequestOutputStream(conn.getOutputStream());
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8))) {
            if (boundary != null) {
                String line = "\r\n--" + boundary + "\r\n";
                writer.write(line);
                writer.write("Content-Disposition: form-data; name=\"query\"\r\n\r\n");
                writer.write(sql);

                StringBuilder builder = new StringBuilder();
                for (ProtonExternalTable t : tables) {
                    String tableName = t.getName();
                    builder.setLength(0);
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("_format\"\r\n\r\n").append(t.getFormat().name());
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("_structure\"\r\n\r\n").append(t.getStructure());
                    builder.append(line).append("Content-Disposition: form-data; name=\"").append(tableName)
                            .append("\"; filename=\"").append(tableName).append("\"\r\n")
                            .append("Content-Type: application/octet-stream\r\n")
                            .append("Content-Transfer-Encoding: binary\r\n\r\n");
                    writer.write(builder.toString());
                    writer.flush();

                    pipe(t.getContent(), out, DEFAULT_BUFFER_SIZE);
                }

                writer.write("\r\n--" + boundary + "--\r\n");
                writer.flush();
            } else {
                writer.write(sql);
                writer.flush();

                if (data != null && data.available() > 0) {
                    // append \n
                    if (sql.charAt(sql.length() - 1) != '\n') {
                        out.write(10);
                    }

                    pipe(data, out, DEFAULT_BUFFER_SIZE);
                }
            }
        }

        checkResponse(conn);

        return buildResponse();
    }

    @Override
    public boolean ping(int timeout) {
        String response = (String) config.getOption(ProtonHttpOption.DEFAULT_RESPONSE);
        HttpURLConnection c = null;
        try {
            c = newConnection(getBaseUrl() + "ping", false);
            c.setConnectTimeout(timeout);
            c.setReadTimeout(timeout);

            checkResponse(c);

            int size = 12;
            try (ByteArrayOutputStream out = new ByteArrayOutputStream(size)) {
                pipe(c.getInputStream(), out, size);

                c.disconnect();
                c = null;
                return response.equals(new String(out.toByteArray(), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            log.debug("Failed to ping server: ", e.getMessage());
        } finally {
            if (c != null) {
                c.disconnect();
            }
        }

        return false;
    }

    @Override
    public void close() {
        conn.disconnect();
    }
}
