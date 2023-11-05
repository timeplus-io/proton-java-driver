package com.timeplus.proton.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import com.timeplus.proton.client.data.ProtonPipedStream;
import com.timeplus.proton.client.data.ProtonRowBinaryProcessor;
import com.timeplus.proton.client.data.ProtonTabSeparatedProcessor;

/**
 * Factory class for creating objects to handle data stream.
 */
public class ProtonDataStreamFactory {
    private static final ProtonDataStreamFactory instance = ProtonUtils
            .getService(ProtonDataStreamFactory.class, new ProtonDataStreamFactory());

    /**
     * Gets instance of the factory class.
     *
     * @return instance of the factory class
     */
    public static ProtonDataStreamFactory getInstance() {
        return instance;
    }

    /**
     * Gets data processor according to given {@link ProtonConfig} and settings.
     *
     * @param config   non-null configuration containing information like
     *                 {@link ProtonFormat}
     * @param input    input stream for deserialization, must not be null when
     *                 {@code output} is null
     * @param output   output stream for serialization, must not be null when
     *                 {@code input} is null
     * @param settings nullable settings
     * @param columns  nullable list of columns
     * @return data processor
     * @throws IOException when failed to read columns from input stream
     */
    public ProtonDataProcessor getProcessor(ProtonConfig config, ProtonInputStream input,
            OutputStream output, Map<String, Object> settings, List<ProtonColumn> columns) throws IOException {
        ProtonFormat format = ProtonChecker.nonNull(config, "config").getFormat();
        ProtonDataProcessor processor;
        if (ProtonFormat.RowBinary == format || ProtonFormat.RowBinaryWithNamesAndTypes == format) {
            processor = new ProtonRowBinaryProcessor(config, input, output, columns, settings);
        } else if (ProtonFormat.TSVWithNames == format || ProtonFormat.TSVWithNamesAndTypes == format
                || ProtonFormat.TabSeparatedWithNames == format
                || ProtonFormat.TabSeparatedWithNamesAndTypes == format) {
            processor = new ProtonTabSeparatedProcessor(config, input, output, columns, settings);
        } else if (format != null && format.isText()) {
            processor = new ProtonTabSeparatedProcessor(config, input, output,
                    ProtonDataProcessor.DEFAULT_COLUMNS, settings);
        } else {
            throw new IllegalArgumentException("Unsupported format: " + format);
        }

        return processor;
    }

    /**
     * Creates a piped stream.
     *
     * @param config non-null configuration
     * @return piped stream
     */
    public ProtonPipedStream createPipedStream(ProtonConfig config) {
        ProtonChecker.nonNull(config, "config");

        return new ProtonPipedStream(config.getMaxBufferSize(), config.getMaxQueuedBuffers(),
                config.getSocketTimeout());
    }
}
