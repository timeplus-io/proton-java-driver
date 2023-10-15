package ru.yandex.proton.response;

import ru.yandex.proton.settings.ProtonProperties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

/**
 * For building ProtonResultSet by hands
 */
public class ProtonResultBuilder {

    private final int columnsNum;
    private List<String> names;
    private List<String> types;
    private List<List<?>> rows = new ArrayList<List<?>>();
    private TimeZone timezone = TimeZone.getTimeZone("UTC");
    private boolean usesWithTotals;
    private ProtonProperties properties = new ProtonProperties();

    public static ProtonResultBuilder builder(int columnsNum) {
        return new ProtonResultBuilder(columnsNum);
    }

    private ProtonResultBuilder(int columnsNum) {
        this.columnsNum = columnsNum;
    }

    public ProtonResultBuilder names(String... names) {
        return names(Arrays.asList(names));
    }

    public ProtonResultBuilder types(String... types) {
        return types(Arrays.asList(types));
    }

    public ProtonResultBuilder addRow(Object... row) {
        return addRow(Arrays.asList(row));
    }

    public ProtonResultBuilder withTotals(boolean usesWithTotals) {
        this.usesWithTotals = usesWithTotals;
        return this;
    }

    public ProtonResultBuilder names(List<String> names) {
        if (names.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + names.size());
        this.names = names;
        return this;
    }

    public ProtonResultBuilder types(List<String> types) {
        if (types.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + types.size());
        this.types = types;
        return this;
    }

    public ProtonResultBuilder addRow(List<?> row) {
        if (row.size() != columnsNum) throw new IllegalArgumentException("size mismatch, req: " + columnsNum + " got: " + row.size());
        rows.add(row);
        return this;
    }

    public ProtonResultBuilder timeZone(TimeZone timezone) {
        this.timezone = timezone;
        return this;
    }

    public ProtonResultBuilder properties(ProtonProperties properties) {
        this.properties = properties;
        return this;
    }

    public ProtonResultSet build() {
        try {
            if (names == null) throw new IllegalStateException("names == null");
            if (types == null) throw new IllegalStateException("types == null");
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            appendRow(names, baos);
            appendRow(types, baos);
            for (List<?> row : rows) {
                appendRow(row, baos);
            }

            byte[] bytes = baos.toByteArray();
            ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);

            return new ProtonResultSet(inputStream, 1024, "system", "unknown", usesWithTotals, null, timezone, properties);
        } catch (IOException e) {
            throw new RuntimeException("Never happens", e);
        }
    }

    private void appendRow(List<?> row, ByteArrayOutputStream baos) throws IOException {
        for (int i = 0; i < row.size(); i++) {
            if (i != 0) baos.write('\t');
            appendObject(row.get(i), baos);
        }
        baos.write('\n');
    }

    private void appendObject(Object o, ByteArrayOutputStream baos) throws IOException {
        if (o == null) {
            baos.write('\\');
            baos.write('N');
        } else {
            String value;
            if (o instanceof Boolean) {
                if ((Boolean) o) {
                    value = "1";
                } else {
                    value = "0";
                }
            } else {
                value = o.toString();
            }
            ByteFragment.escape(value.getBytes(StandardCharsets.UTF_8), baos);
        }
    }

}
