package com.timeplus.proton.client.data;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonRecord;
import com.timeplus.proton.client.ProtonResponse;
import com.timeplus.proton.client.ProtonResponseSummary;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * A simple response built on top of two lists: columns and records.
 */
public class ProtonSimpleResponse implements ProtonResponse {
    @Deprecated
    public static final ProtonSimpleResponse EMPTY = new ProtonSimpleResponse(Collections.emptyList(),
            new ProtonValue[0][], ProtonResponseSummary.EMPTY);

    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param config  non-null config
     * @param columns list of columns
     * @param values  raw values, which may or may not be null
     * @return response object
     */
    public static ProtonResponse of(ProtonConfig config, List<ProtonColumn> columns, Object[][] values) {
        return of(config, columns, values, null);
    }

    /**
     * Creates a response object using columns definition and raw values.
     *
     * @param config  non-null config
     * @param columns list of columns
     * @param values  raw values, which may or may not be null
     * @return response object
     */
    public static ProtonResponse of(ProtonConfig config, List<ProtonColumn> columns, Object[][] values,
            ProtonResponseSummary summary) {
        if (columns == null || columns.isEmpty()) {
            return EMPTY;
        }

        int size = columns.size();
        int len = values != null ? values.length : 0;

        ProtonValue[][] wrappedValues = new ProtonValue[len][];
        if (len > 0) {
            ProtonValue[] templates = new ProtonValue[size];
            for (int i = 0; i < size; i++) {
                templates[i] = ProtonValues.newValue(config, columns.get(i));
            }

            for (int i = 0; i < len; i++) {
                Object[] input = values[i];
                int count = input != null ? input.length : 0;
                ProtonValue[] v = new ProtonValue[size];
                for (int j = 0; j < size; j++) {
                    v[j] = templates[j].copy().update(j < count ? input[j] : null);
                }
                wrappedValues[i] = v;
            }
        }

        return new ProtonSimpleResponse(columns, wrappedValues, summary);
    }

    /**
     * Creates a response object by copying columns and values from the given one.
     * Same as {@code of(response, null)}.
     *
     * @param response response to copy
     * @return new response object
     */
    public static ProtonResponse of(ProtonResponse response) {
        return of(response, null);
    }

    /**
     * Creates a response object by copying columns and values from the given one.
     * You should never use this method against a large response, because it will
     * load everything into memory. Worse than that, when {@code func} is not null,
     * it will be applied to every single row, which is going to be slow when
     * original response contains many records.
     *
     * @param response response to copy
     * @param func     optinal function to update value by column index
     * @return new response object
     */
    public static ProtonResponse of(ProtonResponse response, ProtonRecordTransformer func) {
        if (response == null) {
            return EMPTY;
        } else if (response instanceof ProtonSimpleResponse) {
            return response;
        }

        List<ProtonColumn> columns = response.getColumns();
        int size = columns.size();
        List<ProtonRecord> records = new LinkedList<>();
        int rowIndex = 0;
        for (ProtonRecord r : response.records()) {
            ProtonValue[] values = new ProtonValue[size];
            for (int i = 0; i < size; i++) {
                values[i] = r.getValue(i).copy();
            }

            ProtonRecord rec = ProtonSimpleRecord.of(columns, values);
            if (func != null) {
                func.update(rowIndex, rec);
            }
            records.add(rec);
        }

        return new ProtonSimpleResponse(response.getColumns(), records, response.getSummary());
    }

    private final List<ProtonColumn> columns;
    // better to use simple ProtonRecord as template along with raw values
    private final List<ProtonRecord> records;
    private final ProtonResponseSummary summary;

    private boolean isClosed;

    protected ProtonSimpleResponse(List<ProtonColumn> columns, List<ProtonRecord> records,
            ProtonResponseSummary summary) {
        this.columns = columns;
        this.records = Collections.unmodifiableList(records);
        this.summary = summary != null ? summary : ProtonResponseSummary.EMPTY;
    }

    protected ProtonSimpleResponse(List<ProtonColumn> columns, ProtonValue[][] values,
            ProtonResponseSummary summary) {
        this.columns = columns;

        int len = values.length;
        List<ProtonRecord> list = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            list.add(new ProtonSimpleRecord(columns, values[i]));
        }

        this.records = Collections.unmodifiableList(list);

        this.summary = summary != null ? summary : ProtonResponseSummary.EMPTY;
    }

    @Override
    public List<ProtonColumn> getColumns() {
        return columns;
    }

    @Override
    public ProtonResponseSummary getSummary() {
        return summary;
    }

    @Override
    public InputStream getInputStream() {
        throw new UnsupportedOperationException("An in-memory response does not have input stream");
    }

    @Override
    public Iterable<ProtonRecord> records() {
        return records;
    }

    @Override
    public void close() {
        // nothing to close
        isClosed = true;
    }

    @Override
    public boolean isClosed() {
        return isClosed;
    }
}
