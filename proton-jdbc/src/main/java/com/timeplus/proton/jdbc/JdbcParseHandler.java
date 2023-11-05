package com.timeplus.proton.jdbc;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.jdbc.parser.ProtonSqlStatement;
import com.timeplus.proton.jdbc.parser.ParseHandler;
import com.timeplus.proton.jdbc.parser.StatementType;

public class JdbcParseHandler extends ParseHandler {
    private static final String SETTING_MUTATIONS_SYNC = "mutations_sync";

    public static final ParseHandler INSTANCE = new JdbcParseHandler();

    private void addMutationSetting(String sql, StringBuilder builder, Map<String, Integer> positions,
            Map<String, String> settings, int index) {
        boolean hasSetting = settings != null && !settings.isEmpty();
        String setting = hasSetting ? settings.get(SETTING_MUTATIONS_SYNC) : null;
        if (setting == null) {
            String keyword = "SETTINGS";
            Integer settingsIndex = positions.get(keyword);

            if (settingsIndex == null) {
                builder.append(sql.substring(index)).append(" SETTINGS mutations_sync=1");
                if (hasSetting) {
                    builder.append(',');
                }
            } else {
                builder.append(sql.substring(index, settingsIndex)).append("SETTINGS mutations_sync=1,")
                        .append(sql.substring(settingsIndex + keyword.length()));
            }
        } else {
            builder.append(sql.substring(index));
        }
    }

    private ProtonSqlStatement handleDelete(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String format, String outfile, List<Integer> parameters,
            Map<String, Integer> positions, Map<String, String> settings, Set<String> tempTables) {
        StringBuilder builder = new StringBuilder();
        int index = positions.get("DELETE");
        if (index > 0) {
            builder.append(sql.substring(0, index));
        }
        index = positions.get("FROM");
        Integer whereIdx = positions.get("WHERE");
        if (whereIdx != null) {
            builder.append("ALTER TABLE ");
            if (!ProtonChecker.isNullOrEmpty(database)) {
                builder.append('`').append(database).append('`').append('.');
            }
            builder.append('`').append(table).append('`').append(" DELETE ");
            addMutationSetting(sql, builder, positions, settings, whereIdx);
        } else {
            builder.append("TRUNCATE TABLE").append(sql.substring(index + 4));
        }
        return new ProtonSqlStatement(builder.toString(), stmtType, cluster, database, table, input, format,
                outfile, parameters, null, settings, null);
    }

    private ProtonSqlStatement handleUpdate(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String format, String outfile, List<Integer> parameters,
            Map<String, Integer> positions, Map<String, String> settings, Set<String> tempTables) {
        StringBuilder builder = new StringBuilder();
        int index = positions.get("UPDATE");
        if (index > 0) {
            builder.append(sql.substring(0, index));
        }
        builder.append("ALTER TABLE ");
        index = positions.get("SET");
        if (!ProtonChecker.isNullOrEmpty(database)) {
            builder.append('`').append(database).append('`').append('.');
        }
        builder.append('`').append(table).append('`').append(" UPDATE"); // .append(sql.substring(index + 3));
        addMutationSetting(sql, builder, positions, settings, index + 3);
        return new ProtonSqlStatement(builder.toString(), stmtType, cluster, database, table, input, format,
                outfile, parameters, null, settings, null);
    }

    @Override
    public ProtonSqlStatement handleStatement(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String format, String outfile, List<Integer> parameters,
            Map<String, Integer> positions, Map<String, String> settings, Set<String> tempTables) {
        ProtonSqlStatement s = null;
        if (stmtType == StatementType.DELETE) {
            s = handleDelete(sql, stmtType, cluster, database, table, input, format, outfile, parameters, positions,
                    settings, tempTables);
        } else if (stmtType == StatementType.UPDATE) {
            s = handleUpdate(sql, stmtType, cluster, database, table, input, format, outfile, parameters, positions,
                    settings, tempTables);
        }
        return s;
    }

    private JdbcParseHandler() {
    }
}
