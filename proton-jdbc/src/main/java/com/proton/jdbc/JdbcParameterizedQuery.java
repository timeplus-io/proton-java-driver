package com.proton.jdbc;

import java.util.Collection;
import java.util.Iterator;

import com.proton.client.ProtonConfig;
import com.proton.client.ProtonParameterizedQuery;
import com.proton.client.ProtonUtils;
import com.proton.client.ProtonValues;

/**
 * A parameterized query is a parsed query with parameters being extracted for
 * substitution.
 */
public final class JdbcParameterizedQuery extends ProtonParameterizedQuery {
    /**
     * Creates an instance by parsing the given query.
     *
     * @param config non-null configuration
     * @param query  non-empty SQL query
     * @return parameterized query
     */
    public static JdbcParameterizedQuery of(ProtonConfig config, String query) {
        // cache if query.length() is greater than 1024?
        return new JdbcParameterizedQuery(config, query);
    }

    private JdbcParameterizedQuery(ProtonConfig config, String query) {
        super(config, query);
    }

    @Override
    protected String parse() {
        int paramIndex = 0;
        int partIndex = 0;
        int len = originalQuery.length();
        for (int i = 0; i < len; i++) {
            char ch = originalQuery.charAt(i);
            if (ProtonUtils.isQuote(ch)) {
                i = ProtonUtils.skipQuotedString(originalQuery, i, len, ch) - 1;
            } else if (ch == '?') {
                int idx = ProtonUtils.skipContentsUntil(originalQuery, i + 2, len, '?', ':');
                if (idx < len && originalQuery.charAt(idx - 1) == ':' && originalQuery.charAt(idx) != ':'
                        && originalQuery.charAt(idx - 2) != ':') {
                    i = idx - 1;
                } else {
                    addPart(originalQuery.substring(partIndex, i), paramIndex++, null);
                    partIndex = i + 1;
                }
            } else if (ch == ';') {
                throw new IllegalArgumentException(ProtonUtils.format(
                        "Multi-statement query cannot be used in prepared statement. Please remove semicolon at %d and everything after it.",
                        i));
            } else if (i + 1 < len) {
                char nextCh = originalQuery.charAt(i + 1);
                if (ch == '-' && nextCh == ch) {
                    i = ProtonUtils.skipSingleLineComment(originalQuery, i + 2, len) - 1;
                } else if (ch == '/' && nextCh == '*') {
                    i = ProtonUtils.skipMultiLineComment(originalQuery, i + 2, len) - 1;
                }
            }
        }

        return partIndex < len ? originalQuery.substring(partIndex, len) : null;
    }

    @Override
    public void apply(StringBuilder builder, Collection<String> params) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        Iterator<String> it = params == null ? null : params.iterator();
        boolean hasMore = it != null && it.hasNext();
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(hasMore ? it.next() : ProtonValues.NULL_EXPR);
            hasMore = hasMore && it.hasNext();
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, Object param, Object... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length + 1;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : null;
            }
            builder.append(toSqlExpression(p.paramName, param));
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, Object[] values) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = values == null ? 0 : values.length;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(
                    index < len ? toSqlExpression(p.paramName, values[index]) : ProtonValues.NULL_EXPR);
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, String param, String... more) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = more == null ? 0 : more.length + 1;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            if (index > 0) {
                param = index < len ? more[index - 1] : ProtonValues.NULL_EXPR;
            }
            builder.append(param);
            index++;
        }

        appendLastPartIfExists(builder);
    }

    @Override
    public void apply(StringBuilder builder, String[] values) {
        if (!hasParameter()) {
            builder.append(originalQuery);
            return;
        }

        int len = values == null ? 0 : values.length;
        int index = 0;
        for (QueryPart p : getParts()) {
            builder.append(p.part);
            builder.append(index < len ? values[index] : ProtonValues.NULL_EXPR);
            index++;
        }

        appendLastPartIfExists(builder);
    }
}
