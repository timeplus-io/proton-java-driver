package com.clickhouse.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This defines a data processor for dealing with one or multiple
 * {@link ClickHouseFormat}.
 */
public abstract class ClickHouseDataProcessor {
    public static final List<ClickHouseColumn> DEFAULT_COLUMNS = Collections
            .singletonList(ClickHouseColumn.of("results", "nullable(string)"));

    protected static final String ERROR_UNKNOWN_DATA_TYPE = "Unsupported data type: ";

    // not a fan of Java generics :<
    protected static void buildAggMappings(
            Map<ClickHouseAggregateFunction, ClickHouseDeserializer<ClickHouseValue>> deserializers,
            Map<ClickHouseAggregateFunction, ClickHouseSerializer<ClickHouseValue>> serializers,
            ClickHouseDeserializer<ClickHouseValue> d, ClickHouseSerializer<ClickHouseValue> s,
            ClickHouseAggregateFunction... types) {
        for (ClickHouseAggregateFunction t : types) {
            if (deserializers.put(t, d) != null) {
                throw new IllegalArgumentException("Duplicated deserializer of aggregate_function - " + t.name());
            }
            if (serializers.put(t, s) != null) {
                throw new IllegalArgumentException("Duplicated serializer of aggregate_function - " + t.name());
            }
        }
    }

    protected static <E extends Enum<E>, T extends ClickHouseValue> void buildMappings(
            Map<E, ClickHouseDeserializer<? extends ClickHouseValue>> deserializers,
            Map<E, ClickHouseSerializer<? extends ClickHouseValue>> serializers,
            ClickHouseDeserializer<T> d, ClickHouseSerializer<T> s, E... types) {
        for (E t : types) {
            if (deserializers.put(t, d) != null) {
                throw new IllegalArgumentException("Duplicated deserializer of: " + t.name());
            }
            if (serializers.put(t, s) != null) {
                throw new IllegalArgumentException("Duplicated serializer of: " + t.name());
            }
        }
    }

    protected final ClickHouseConfig config;
    protected final ClickHouseInputStream input;
    protected final OutputStream output;
    protected final List<ClickHouseColumn> columns;
    protected final Map<String, Object> settings;

    /**
     * Read columns from input stream. Usually this will be only called once during
     * instantiation.
     *
     * @return list of columns
     * @throws IOException when failed to read columns from input stream
     */
    protected abstract List<ClickHouseColumn> readColumns() throws IOException;

    /**
     * Default constructor.
     *
     * @param config   non-null confinguration contains information like format
     * @param input    input stream for deserialization, can be null when
     *                 {@code output} is not
     * @param output   outut stream for serialization, can be null when
     *                 {@code input} is not
     * @param columns  nullable columns
     * @param settings nullable settings
     * @throws IOException when failed to read columns from input stream
     */
    protected ClickHouseDataProcessor(ClickHouseConfig config, ClickHouseInputStream input, OutputStream output,
            List<ClickHouseColumn> columns, Map<String, Object> settings) throws IOException {
        this.config = ClickHouseChecker.nonNull(config, "config");
        if (input == null && output == null) {
            throw new IllegalArgumentException("One of input and output stream must be non-null");
        }

        this.input = input;
        this.output = output;
        if (settings == null || settings.isEmpty()) {
            this.settings = Collections.emptyMap();
        } else {
            Map<String, Object> map = new HashMap<>();
            map.putAll(settings);
            this.settings = Collections.unmodifiableMap(map);
        }

        if (columns == null && input != null) {
            columns = readColumns();
        }

        if (columns == null || columns.isEmpty()) {
            this.columns = Collections.emptyList();
        } else {
            List<ClickHouseColumn> list = new ArrayList<>(columns.size());
            list.addAll(columns);
            this.columns = Collections.unmodifiableList(list);
        }
    }

    /**
     * Gets list of columns to process.
     *
     * @return list of columns to process
     */
    public final List<ClickHouseColumn> getColumns() {
        return columns;
    }

    /**
     * Returns an iterable collection of records which can be walked through in a
     * foreach loop. Please pay attention that: 1)
     * {@link java.io.UncheckedIOException} might be thrown when iterating through
     * the collection; and 2) it's not supposed to be called for more than once.
     *
     * @return non-null iterable collection
     */
    public abstract Iterable<ClickHouseRecord> records();
}
