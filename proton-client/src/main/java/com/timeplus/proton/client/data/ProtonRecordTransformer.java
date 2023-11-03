package com.timeplus.proton.client.data;

import com.timeplus.proton.client.ProtonRecord;

@FunctionalInterface
public interface ProtonRecordTransformer {
    /**
     * Updates values in the given record.
     *
     * @param rowIndex zero-based index of row
     * @param r        record to update
     */
    void update(int rowIndex, ProtonRecord r);
}
