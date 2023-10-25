package com.proton.client.data;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import com.proton.client.ProtonChecker;
import com.proton.client.ProtonColumn;
import com.proton.client.ProtonFormat;

public class ProtonExternalTable {
    public static class Builder {
        private String name;
        private CompletableFuture<InputStream> content;
        private ProtonFormat format;
        private List<ProtonColumn> columns;
        private boolean asTempTable;

        protected Builder() {
            columns = new LinkedList<>();
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder content(InputStream content) {
            this.content = CompletableFuture.completedFuture(ProtonChecker.nonNull(content, "content"));
            return this;
        }

        public Builder content(CompletableFuture<InputStream> content) {
            this.content = ProtonChecker.nonNull(content, "Content");
            return this;
        }

        public Builder format(String format) {
            if (!ProtonChecker.isNullOrBlank(format)) {
                this.format = ProtonFormat.valueOf(format);
            }
            return this;
        }

        public Builder format(ProtonFormat format) {
            this.format = format;
            return this;
        }

        public Builder addColumn(String name, String type) {
            this.columns.add(ProtonColumn.of(name, type));
            return this;
        }

        public Builder removeColumn(String name) {
            Iterator<ProtonColumn> iterator = columns.iterator();
            while (iterator.hasNext()) {
                ProtonColumn c = iterator.next();
                if (c.getColumnName().equals(name)) {
                    iterator.remove();
                }
            }

            return this;
        }

        public Builder removeColumn(ProtonColumn column) {
            this.columns.remove(column);
            return this;
        }

        public Builder columns(String columns) {
            return !ProtonChecker.isNullOrBlank(columns) ? columns(ProtonColumn.parse(columns)) : this;
        }

        public Builder columns(Collection<ProtonColumn> columns) {
            if (columns != null) {
                for (ProtonColumn c : columns) {
                    this.columns.add(c);
                }
            }
            return this;
        }

        public Builder asTempTable() {
            asTempTable = true;
            return this;
        }

        public Builder asExternalTable() {
            asTempTable = false;
            return this;
        }

        public ProtonExternalTable build() {
            return new ProtonExternalTable(name, content, format, columns, asTempTable);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private final String name;
    private final CompletableFuture<InputStream> content;
    private final ProtonFormat format;
    private final List<ProtonColumn> columns;
    private final boolean asTempTable;

    private final String structure;

    protected ProtonExternalTable(String name, CompletableFuture<InputStream> content, ProtonFormat format,
            Collection<ProtonColumn> columns, boolean asTempTable) {
        this.name = name == null ? "" : name.trim();
        this.content = ProtonChecker.nonNull(content, "content");
        this.format = format == null ? ProtonFormat.TabSeparated : format;

        int size = columns == null ? 0 : columns.size();
        if (size == 0) {
            this.columns = Collections.emptyList();
            this.structure = "";
        } else {
            StringBuilder builder = new StringBuilder();
            List<ProtonColumn> list = new ArrayList<>(size);
            for (ProtonColumn c : columns) {
                list.add(c);
                builder.append(c.getColumnName()).append(' ').append(c.getOriginalTypeName()).append(',');
            }
            this.columns = Collections.unmodifiableList(list);
            this.structure = builder.deleteCharAt(builder.length() - 1).toString();
        }

        this.asTempTable = asTempTable;
    }

    public boolean hasName() {
        return !name.isEmpty();
    }

    public String getName() {
        return name;
    }

    public InputStream getContent() {
        try {
            return content.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        } catch (ExecutionException e) {
            throw new CompletionException(e.getCause());
        }
    }

    public ProtonFormat getFormat() {
        return format;
    }

    public List<ProtonColumn> getColumns() {
        return columns;
    }

    public boolean isTempTable() {
        return asTempTable;
    }

    public String getStructure() {
        return structure;
    }
}
