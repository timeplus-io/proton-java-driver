package com.proton.client.data;

import java.util.Collections;
import java.util.List;

import com.proton.client.ProtonColumn;
import com.proton.client.ProtonRecord;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValue;

/**
 * Default implementation of {@link com.proton.client.ProtonRecord},
 * which is simply a combination of list of columns and array of values.
 */
public class ProtonSimpleRecord implements ProtonRecord {
    public static final ProtonSimpleRecord EMPTY = new ProtonSimpleRecord(Collections.emptyList(),
            new ProtonValue[0]);

    private final List<ProtonColumn> columns;
    private ProtonValue[] values;

    /**
     * Creates a record object to wrap given values.
     *
     * @param columns non-null list of columns
     * @param values  non-null array of values
     * @return record
     */
    public static ProtonRecord of(List<ProtonColumn> columns, ProtonValue[] values) {
        if (columns == null || values == null) {
            throw new IllegalArgumentException("Non-null columns and values are required");
        } else if (columns.size() != values.length) {
            throw new IllegalArgumentException(ProtonUtils.format(
                    "Mismatched count: we have %d columns but we got %d values", columns.size(), values.length));
        } else if (values.length == 0) {
            return EMPTY;
        }

        return new ProtonSimpleRecord(columns, values);
    }

    protected ProtonSimpleRecord(List<ProtonColumn> columns, ProtonValue[] values) {
        this.columns = columns;
        this.values = values;
    }

    protected List<ProtonColumn> getColumns() {
        return columns;
    }

    protected ProtonValue[] getValues() {
        return values;
    }

    protected void update(ProtonValue[] values) {
        this.values = values;
    }

    protected void update(Object[] values) {
        int len = values != null ? values.length : 0;
        for (int i = 0, size = this.values.length; i < size; i++) {
            if (i < len) {
                this.values[i].update(values[i]);
            } else {
                this.values[i].resetToNullOrEmpty();
            }
        }
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public ProtonValue getValue(int index) {
        return values[index];
    }

    @Override
    public ProtonValue getValue(String name) {
        int index = 0;
        for (ProtonColumn c : columns) {
            if (c.getColumnName().equalsIgnoreCase(name)) {
                return getValue(index);
            }
            index++;
        }

        throw new IllegalArgumentException(ProtonUtils.format("Unable to find column [%s]", name));
    }
}
