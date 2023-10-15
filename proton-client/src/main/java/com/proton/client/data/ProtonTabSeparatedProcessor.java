package com.proton.client.data;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.proton.client.ProtonColumn;
import com.proton.client.ProtonConfig;
import com.proton.client.ProtonDataProcessor;
import com.proton.client.ProtonFormat;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonRecord;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValue;
import com.proton.client.data.tsv.ByteFragment;
import com.proton.client.data.tsv.StreamSplitter;

public class ProtonTabSeparatedProcessor extends ProtonDataProcessor {
    private static String[] toStringArray(ByteFragment headerFragment, byte delimitter) {
        if (delimitter == (byte) 0) {
            return new String[] { headerFragment.asString(true) };
        }

        ByteFragment[] split = headerFragment.split(delimitter);
        String[] array = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            array[i] = split[i].asString(true);
        }
        return array;
    }

    private class Records implements Iterator<ProtonRecord> {
        private ByteFragment currentRow;

        Records() {
            if (!columns.isEmpty()) {
                readNextRow();
            }
        }

        void readNextRow() {
            try {
                currentRow = splitter.next();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return currentRow != null;
        }

        @Override
        public ProtonRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException("No more record");
            }

            ByteFragment[] currentCols = colDelimitter != (byte) 0 ? currentRow.split(colDelimitter)
                    : new ByteFragment[] { currentRow };
            readNextRow();

            return new ProtonRecord() {
                @Override
                public int size() {
                    return currentCols.length;
                }

                @Override
                public ProtonValue getValue(int index) {
                    return ProtonStringValue.of(null, currentCols[index].asString(true));
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
            };
        }
    }

    private final byte rowDelimitter = (byte) 0x0A;

    // initialize in readColumns()
    private byte colDelimitter;
    private StreamSplitter splitter;

    @Override
    public List<ProtonColumn> readColumns() throws IOException {
        if (input == null) {
            return Collections.emptyList();
        }

        ProtonFormat format = config.getFormat();
        if (!format.hasHeader()) {
            return DEFAULT_COLUMNS;
        }

        switch (config.getFormat()) {
            case TSVWithNames:
            case TSVWithNamesAndTypes:
            case TabSeparatedWithNames:
            case TabSeparatedWithNamesAndTypes:
                colDelimitter = (byte) 0x09;
                break;
            default:
                colDelimitter = (byte) 0;
                break;
        }

        this.splitter = new StreamSplitter(input, rowDelimitter, config.getMaxBufferSize());

        ByteFragment headerFragment = this.splitter.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("Proton response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            input.close();
            throw new IllegalArgumentException("Proton error: " + header);
        }
        String[] cols = toStringArray(headerFragment, colDelimitter);
        String[] types = null;
        if (ProtonFormat.TSVWithNamesAndTypes == format
                || ProtonFormat.TabSeparatedWithNamesAndTypes == format) {
            ByteFragment typesFragment = splitter.next();
            if (typesFragment == null) {
                throw new IllegalArgumentException("Proton response without column types");
            }

            types = toStringArray(typesFragment, colDelimitter);
        }
        List<ProtonColumn> list = new ArrayList<>(cols.length);

        for (int i = 0; i < cols.length; i++) {
            list.add(ProtonColumn.of(cols[i], types == null ? "nullable(string)" : types[i]));
        }

        return list;
    }

    public ProtonTabSeparatedProcessor(ProtonConfig config, ProtonInputStream input, OutputStream output,
            List<ProtonColumn> columns, Map<String, Object> settings) throws IOException {
        super(config, input, output, columns, settings);

        if (this.splitter == null && input != null) {
            this.splitter = new StreamSplitter(input, rowDelimitter, config.getMaxBufferSize());
        }
    }

    @Override
    public Iterable<ProtonRecord> records() {
        return new Iterable<ProtonRecord>() {
            @Override
            public Iterator<ProtonRecord> iterator() {
                return new Records();
            }
        };
    }
}
