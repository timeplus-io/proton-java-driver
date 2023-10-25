package com.proton.jdbc.parser;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class ParseHandler {
    /**
     * Handle macro like "#include('/tmp/template.sql')".
     * 
     * @param name       name of the macro
     * @param parameters parameters
     * @return output of the macro, could be null or empty string
     */
    public String handleMacro(String name, List<String> parameters) {
        return null;
    }

    /**
     * Handle parameter.
     * 
     * @param cluster     cluster
     * @param database    database
     * @param table       table
     * @param columnIndex columnIndex(starts from 1 not 0)
     * @return parameter value
     */
    public String handleParameter(String cluster, String database, String table, int columnIndex) {
        return null;
    }

    /**
     * Hanlde statemenet.
     * 
     * @param sql        sql statement
     * @param stmtType   statement type
     * @param cluster    cluster
     * @param database   database
     * @param table      table
     * @param format     format
     * @param input      input
     * @param outfile    outfile
     * @param parameters positions of parameters
     * @param positions  keyword positions
     * @param settings   settings
     * @param tempTables temporary tables
     * @return sql statement, or null means no change
     */
    public ProtonSqlStatement handleStatement(String sql, StatementType stmtType, String cluster, String database,
            String table, String input, String format, String outfile, List<Integer> parameters,
            Map<String, Integer> positions, Map<String, String> settings, Set<String> tempTables) {
        return null;
    }
}
