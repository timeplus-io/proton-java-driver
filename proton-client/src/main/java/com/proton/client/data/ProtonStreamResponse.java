package com.proton.client.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.proton.client.ProtonColumn;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonDataProcessor;
import com.proton.client.ProtonDataStreamFactory;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonRecord;
import com.proton.client.ProtonResponse;
import com.proton.client.ProtonResponseSummary;
import com.proton.client.logging.Logger;
import com.proton.client.logging.LoggerFactory;

/**
 * A stream response from server.
 */
public class ProtonStreamResponse implements ProtonResponse {
    private static final Logger log = LoggerFactory.getLogger(ProtonStreamResponse.class);

    private static final long serialVersionUID = 2271296998310082447L;

    protected static final List<ProtonColumn> defaultTypes = Collections
            .singletonList(ProtonColumn.of("results", "nullable(string)"));

    public static ProtonResponse of(ProtonConfig config, ProtonInputStream input) throws IOException {
        return of(config, input, null, null, null);
    }

    public static ProtonResponse of(ProtonConfig config, ProtonInputStream input,
            Map<String, Object> settings) throws IOException {
        return of(config, input, settings, null, null);
    }

    public static ProtonResponse of(ProtonConfig config, ProtonInputStream input,
            List<ProtonColumn> columns) throws IOException {
        return of(config, input, null, columns, null);
    }

    public static ProtonResponse of(ProtonConfig config, ProtonInputStream input,
            Map<String, Object> settings, List<ProtonColumn> columns) throws IOException {
        return of(config, input, settings, columns, null);
    }

    public static ProtonResponse of(ProtonConfig config, ProtonInputStream input,
            Map<String, Object> settings, List<ProtonColumn> columns, ProtonResponseSummary summary)
            throws IOException {
        return new ProtonStreamResponse(config, input, settings, columns, summary);
    }

    protected final ProtonConfig config;
    protected final transient InputStream input;
    protected final transient ProtonDataProcessor processor;
    protected final List<ProtonColumn> columns;
    protected final ProtonResponseSummary summary;

    private boolean closed;

    protected ProtonStreamResponse(ProtonConfig config, ProtonInputStream input,
            Map<String, Object> settings, List<ProtonColumn> columns, ProtonResponseSummary summary)
            throws IOException {
        if (config == null || input == null) {
            throw new IllegalArgumentException("Non-null configuration and input stream are required");
        }

        this.config = config;
        this.input = input;

        boolean hasError = true;
        try {
            this.processor = ProtonDataStreamFactory.getInstance().getProcessor(config, input, null, settings,
                    columns);
            this.columns = columns != null ? columns
                    : (processor != null ? processor.getColumns() : Collections.emptyList());
            hasError = false;
        } finally {
            if (hasError) {
                // rude but safe
                log.error("Failed to create stream response, closing input stream");
                try {
                    input.close();
                } catch (Exception e) {
                    // ignore
                }
            }
        }
        this.summary = summary != null ? summary : ProtonResponseSummary.EMPTY;
        this.closed = hasError;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        try {
            log.debug("%d bytes skipped before closing input stream", input.skip(Long.MAX_VALUE));
        } catch (Exception e) {
            // ignore
            log.debug("Failed to skip reading input stream due to: %s", e.getMessage());
        } finally {
            try {
                input.close();
            } catch (Exception e) {
                log.warn("Failed to close input stream", e);
            }
            closed = true;
        }
    }

    @Override
    public List<ProtonColumn> getColumns() {
        return columns;
    }

    public ProtonFormat getFormat() {
        return this.config.getFormat();
    }

    @Override
    public ProtonResponseSummary getSummary() {
        return summary;
    }

    @Override
    public InputStream getInputStream() {
        return input;
    }

    @Override
    public Iterable<ProtonRecord> records() {
        if (processor == null) {
            throw new UnsupportedOperationException(
                    "No data processor available for deserialization, please consider to use getInputStream instead");
        }

        return processor.records();
    }
}
