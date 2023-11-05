package com.timeplus.proton.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.Types;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonDataType;
import com.timeplus.proton.client.ProtonParameterizedQuery;
import com.timeplus.proton.client.ProtonUtils;
import com.timeplus.proton.client.ProtonValues;
import com.timeplus.proton.client.data.ProtonRecordTransformer;
import com.timeplus.proton.client.data.ProtonSimpleResponse;
import com.timeplus.proton.client.logging.Logger;
import com.timeplus.proton.client.logging.LoggerFactory;

public class ProtonDatabaseMetaData extends JdbcWrapper implements DatabaseMetaData {
    private static final Logger log = LoggerFactory.getLogger(ProtonDatabaseMetaData.class);

    private static final String DATABASE_NAME = "Proton";
    private static final String DRIVER_NAME = DATABASE_NAME + " JDBC Driver";

    private static final String[] TABLE_TYPES = new String[] { "DICTIONARY", "LOG TABLE", "MEMORY TABLE",
            "REMOTE TABLE", "TABLE", "VIEW", "SYSTEM TABLE", "TEMPORARY TABLE" };

    private final ProtonConnection connection;
    private final Map<String, Class<?>> typeMaps;

    protected ResultSet empty(String columns) throws SQLException {
        return fixed(columns, null);
    }

    protected ResultSet fixed(String columns, Object[][] values) throws SQLException {
        return new ProtonResultSet("", "", connection.createStatement(),
                ProtonSimpleResponse.of(connection.getConfig(), ProtonColumn.parse(columns), values));
    }

    protected ResultSet query(String sql) throws SQLException {
        return query(sql, null, false);
    }

    protected ResultSet query(String sql, boolean ignoreError) throws SQLException {
        return query(sql, null, ignoreError);
    }

    protected ResultSet query(String sql, ProtonRecordTransformer func) throws SQLException {
        return query(sql, func, false);
    }

    protected ResultSet query(String sql, ProtonRecordTransformer func, boolean ignoreError) throws SQLException {
        SQLException error = null;
        try {
            ProtonStatement stmt = connection.createStatement();
            return new ProtonResultSet("", "", stmt,
                    // load everything into memory
                    ProtonSimpleResponse.of(stmt.getRequest().query(sql).execute().get(), func));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw SqlExceptionUtils.forCancellation(e);
        } catch (Exception e) {
            error = SqlExceptionUtils.handle(e);
        }

        if (ignoreError) {
            return null;
        } else {
            throw error;
        }
    }

    public ProtonDatabaseMetaData(ProtonConnection connection) throws SQLException {
        this.connection = ProtonChecker.nonNull(connection, "Connection");
        this.typeMaps = connection.getTypeMap();
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }

    @Override
    public String getURL() throws SQLException {
        return connection.getUri().toString();
    }

    @Override
    public String getUserName() throws SQLException {
        return connection.getCurrentUser();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return true;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return DATABASE_NAME;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connection.getServerVersion().toString();
    }

    @Override
    public String getDriverName() throws SQLException {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return ProtonDriver.driverVersionString;
    }

    @Override
    public int getDriverMajorVersion() {
        return ProtonDriver.driverVersion.getMajorVersion();
    }

    @Override
    public int getDriverMinorVersion() {
        return ProtonDriver.driverVersion.getMinorVersion();
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "`";
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return "APPLY,ASOF,ATTACH,CLUSTER,DATABASE,DATABASES,DETACH,"
                + "DICTIONARY,DICTIONARIES,ILIKE,INF,LIMIT,LIVE,KILL,MATERIALIZED,"
                + "NAN,OFFSET,OPTIMIZE,OUTFILE,POLICY,PREWHERE,PROFILE,QUARTER,QUOTA,"
                + "RENAME,REPLACE,SAMPLE,SETTINGS,SHOW,TABLES,TIES,TOP,TOTALS,TRUNCATE,USE,WATCH,WEEK";
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        // took from below URLs(not from system.functions):
        // https://Clickhouse.com/docs/en/sql-reference/functions/arithmetic-functions/
        // https://Clickhouse.com/docs/en/sql-reference/functions/math-functions/
        return "abs,acos,acosh,asin,asinh,atan,atan2,atanh,cbrt,cos,cosh,divide,e,erf,erfc,exp,exp10,exp2,gcd,hypot,int_div,int_div_or_zero,int_exp10,int_exp2,lcm,lgamma,ln,log,log10,log1p,log2,minus,modulo,modulo_or_zero,multiply,negate,pi,plus,pow,power,sign,sin,sinh,sqrt,tan,tgamma";
    }

    @Override
    public String getStringFunctions() throws SQLException {
        // took from below URLs(not from system.functions):
        // https://Clickhouse.com/docs/en/sql-reference/functions/string-functions/
        // https://Clickhouse.com/docs/en/sql-reference/functions/string-search-functions/
        // https://Clickhouse.com/docs/en/sql-reference/functions/string-replace-functions/
        return "append_trailing_char_if_absent,base64_decode,base64_encode,char_length,CHAR_LENGTH,character_length,CHARACTER_LENGTH,concat,concat_assume_injective,convert_charset,count_matches,count_substrings,count_substrings_case_insensitive,count_substrings_case_insensitive_utf8,crc32,crc32ieee,crc64,decode_xml_component,empty,encode_xml_component,ends_with,extract,extract_all,extract_all_groups_horizontal,extract_all_groups_vertical,extract_text_from_html ,format,ilike,is_valid_utf8,lcase,left_pad,left_pad_utf8,length,length_utf8,like,locate,lower,lower_utf8,match,mid,multi_fuzzy_match_all_indices,multi_fuzzy_match_any,multi_fuzzy_match_any_index,multi_match_all_indices,multi_match_any,multi_match_any_index,multi_search_all_positions,multi_search_all_positions_utf8,multi_search_any,multi_search_first_index,multi_search_first_position,ngram_distance,ngram_search,normalized_query_hash,normalize_query,not_empty,not_like,position,position_case_insensitive,position_case_insensitive_utf8,position_utf8,regexp_quote_meta,repeat,replace,replace_all,replace_one,replace_regexp_all,replace_regexp_one,reverse,reverse_utf8,right_pad,right_pad_utf8,starts_with,substr,substring,substring_utf8,tokens,to_valid_utf8,trim,trim_both,trim_left,trim_right,try_base64_decode,ucase,upper,upper_utf8";
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        // took from below URL(not from system.functions):
        // https://Clickhouse.com/docs/en/sql-reference/functions/other-functions/
        return "bar,basename,block_number,block_serialized_size,block_size,build_id,byte_size,count_digits,current_database,current_profiles,current_roles,current_user,default_profiles,default_roles,default_value_of_argument_type,default_value_of_typeName,dump_column_structure,enabled_profiles,enabled_roles,error_code_to_name,filesystem_available,filesystem_capacity,filesystem_free,finalize_aggregation,format_readable_quantity,format_readable_size,format_readable_time_delta,fqdn,get_macro,get_server_port,get_setting,get_size_of_enum_type,greatest,has_column_in_table,hostname,identity,if_not_finite,ignore,index_hint,initialize_aggregation,initial_query_id,is_constant,is_decimal_overflow,is_finite,is_infinite,is_nan,join_get,least,mac_num_to_string,mac_string_to_num,mac_string_to_oui,materialize,model_evaluate,neighbor,query_id,random_fixed_string,random_printable_ascii,random_string,random_string_utf8,replicate,row_number_in_all_blocks,row_number_in_block,running_accumulate,running_concurrency,running_difference,running_difference_starting_with_first_value,shard_count ,shard_num,sleep,sleep_each_row,tcp_port,throw_if,to_column_type_name,to_type_name,transform,uptime,version,visible_width";
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        // took from below URL(not from system.functions):
        // https://Clickhouse.com/docs/en/sql-reference/functions/date-time-functions/
        return "add_days,add_hours,add_minutes,add_months,add_quarters,add_seconds,add_weeks,add_years,date_add,date_diff,date_sub,date_trunc,date_name,format_date_time,from_unixtime,from_modified_julian_day,from_modified_julian_day_or_null,now,subtract_days,subtract_hours,subtract_minutes,subtract_months,subtract_quarters,subtract_seconds,subtract_weeks,subtract_years,time_slot,time_slots,timestamp_add,timestamp_sub,timeZone,time_zone_of,time_zone_offset,today,to_day_of_month,weekday,to_day_of_year,to_hour,to_iso_week,to_iso_year,to_minute,to_modified_julian_day,to_modified_julian_day_or_null,to_monday,to_month,to_quarter,to_relative_day_num,to_relative_hour_num,to_relative_minute_num,to_relative_month_num,to_relative_quarter_num,to_relative_second_num,to_relative_week_num,to_relative_year_num,to_second,to_start_of_day,to_start_of_fifteen_minutes,to_start_of_five_minute,to_start_of_hour,to_start_of_interval,to_start_of_iso_year,to_start_of_minute,to_start_of_month,to_start_of_quarter,to_start_of_second,to_start_of_ten_minutes,to_start_of_week,to_start_of_year,to_time,to_time_zone,to_unix_timestamp,to_week,to_year,to_year_week,to_YYYYMM,to_YYYYMMDD,to_YYYYMMDDhhmmss,yesterday";
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        // TODO select { fn CONVERT({ts '2021-01-01 12:12:12'}, TIMESTAMP) }
        // select cast('2021-01-01 12:12:12' as DateTime)
        return false;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        // TODO select { fn CONVERT({ts '2021-01-01 12:12:12'}, TIMESTAMP) }
        // select cast('2021-01-01 12:12:12' as DateTime)
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        // TODO let's add this in 0.3.3
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return "database";
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return "procedure";
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return "catalog";
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return connection.getJdbcConfig().isJdbcCompliant() ? Connection.TRANSACTION_READ_COMMITTED
                : Connection.TRANSACTION_NONE;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        if (Connection.TRANSACTION_NONE == level) {
            return true;
        } else if (Connection.TRANSACTION_READ_UNCOMMITTED != level && Connection.TRANSACTION_READ_COMMITTED != level
                && Connection.TRANSACTION_REPEATABLE_READ != level && Connection.TRANSACTION_SERIALIZABLE != level) {
            throw SqlExceptionUtils.clientError("Unknown isolation level: " + level);
        }

        return connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return connection.getJdbcConfig().isJdbcCompliant();
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
            throws SQLException {
        return empty("PROCEDURE_CAT nullable(string), PROCEDURE_SCHEM nullable(string), "
                + "RESERVED1 nullable(string), RESERVED2 nullable(string), RESERVED3 nullable(string), "
                + "PROCEDURE_NAME string, REMARKS string, PROCEDURE_TYPE int16, SPECIFIC_NAME string");
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        return empty("PROCEDURE_CAT nullable(string), PROCEDURE_SCHEM nullable(string), "
                + "PROCEDURE_NAME string, COLUMN_NAME string, COLUMN_TYPE int16, "
                + "DATA_TYPE int32, TYPE_NAME string, PRECISION int32, LENGTH int32, "
                + "SCALE int16, RADIX int16, NULLABLE int16, REMARKS string, "
                + "COLUMN_DEF nullable(string), SQL_DATA_TYPE int32, SQL_DATETIME_SUB int32, "
                + "CHAR_OCTET_LENGTH int32, ORDINAL_POSITION int32, IS_NULLABLE string, SPECIFIC_NAME string");
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
        StringBuilder builder = new StringBuilder();
        if (types == null || types.length == 0) {
            types = TABLE_TYPES;
        }
        for (String type : types) {
            builder.append('\'').append(ProtonUtils.escape(type, '\'')).append('\'').append(',');
        }
        builder.setLength(builder.length() - 1);

        List<String> databases = new LinkedList<>();
        if (ProtonChecker.isNullOrEmpty(schemaPattern)) {
            try (ResultSet rs = query("select name from system.databases order by name")) {
                while (rs.next()) {
                    databases.add(rs.getString(1));
                }
            } catch (Exception e) {
                // ignore
            } finally {
                if (databases.isEmpty()) {
                    databases.add("%");
                }
            }
        } else {
            databases.add(schemaPattern);
        }

        List<ResultSet> results = new ArrayList<>(databases.size());
        for (String database : databases) {
            Map<String, String> params = new HashMap<>();
            params.put("comment", connection.getServerVersion().check("[21.6,)") ? "t.comment" : "''");
            params.put("database", ProtonValues.convertToQuotedString(database));
            params.put("table", ProtonChecker.isNullOrEmpty(tableNamePattern) ? "'%'"
                    : ProtonValues.convertToQuotedString(tableNamePattern));
            params.put("types", builder.toString());
            String sql = ProtonParameterizedQuery
                    .apply("select null as TABLE_CAT, t.database as TABLE_SCHEM, t.name as TABLE_NAME, "
                            + "case when t.engine like '%Log' then 'LOG TABLE' "
                            + "when t.engine in ('Buffer', 'Memory', 'Set') then 'MEMORY TABLE' "
                            + "when t.is_temporary != 0 then 'TEMPORARY TABLE' "
                            + "when t.engine like '%View' then 'VIEW' when t.engine = 'Dictionary' then 'DICTIONARY' "
                            + "when t.engine like 'Async%' or t.engine like 'System%' then 'SYSTEM TABLE' "
                            + "when empty(t.data_paths) then 'REMOTE TABLE' else 'TABLE' end as TABLE_TYPE, "
                            + ":comment as REMARKS, null as TYPE_CAT, d.engine as TYPE_SCHEM, "
                            + "t.engine as TYPE_NAME, null as SELF_REFERENCING_COL_NAME, null as REF_GENERATION\n"
                            + "from system.tables t inner join system.databases d on t.database = d.name\n"
                            + "where t.database like :database and t.name like :table and TABLE_TYPE in (:types) "
                            + "order by t.database, t.name", params);
            results.add(query(sql, true));
        }
        return new CombinedResultSet(results);
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return getSchemas(null, null);
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return empty("TABLE_CAT String");
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        // "TABLE", "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY", "LOCAL TEMPORARY",
        // "ALIAS", "SYNONYM".
        int len = TABLE_TYPES.length;
        Object[][] rows = new Object[len][];
        for (int i = 0; i < len; i++) {
            rows[i] = new Object[] { TABLE_TYPES[i] };
        }
        return fixed("TABLE_TYPE string", rows);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        params.put("comment", connection.getServerVersion().check("[18.16,)") ? "comment" : "''");
        params.put("database", ProtonChecker.isNullOrEmpty(schemaPattern) ? "'%'"
                : ProtonValues.convertToQuotedString(schemaPattern));
        params.put("table", ProtonChecker.isNullOrEmpty(tableNamePattern) ? "'%'"
                : ProtonValues.convertToQuotedString(tableNamePattern));
        params.put("column", ProtonChecker.isNullOrEmpty(columnNamePattern) ? "'%'"
                : ProtonValues.convertToQuotedString(columnNamePattern));
        params.put("defaultNullable", String.valueOf(DatabaseMetaData.typeNullable));
        params.put("defaultNonNull", String.valueOf(DatabaseMetaData.typeNoNulls));
        params.put("defaultType", String.valueOf(Types.OTHER));
        String sql = ProtonParameterizedQuery
                .apply("select null as TABLE_CAT, database as TABLE_SCHEM, table as TABLE_NAME, "
                        + "name as COLUMN_NAME, to_int32(:defaultType) as DATA_TYPE, type as TYPE_NAME, to_int32(0) as COLUMN_SIZE, "
                        + "0 as BUFFER_LENGTH, cast(null as nullable(int32)) as DECIMAL_DIGITS, 10 as NUM_PREC_RADIX, "
                        + "to_int32(position(type, 'nullable(') >= 1 ? :defaultNullable : :defaultNonNull) as NULLABLE, :comment as REMARKS, default_expression as COLUMN_DEF, "
                        + "0 as SQL_DATA_TYPE, 0 as SQL_DATETIME_SUB, cast(null as nullable(int32)) as CHAR_OCTET_LENGTH, position as ORDINAL_POSITION, "
                        + "position(type, 'nullable(') >= 1 ? 'YES' : 'NO' as IS_NULLABLE, null as SCOPE_CATALOG, null as SCOPE_SCHEMA, null as SCOPE_TABLE, "
                        + "null as SOURCE_DATA_TYPE, 'NO' as IS_AUTOINCREMENT, 'NO' as IS_GENERATEDCOLUMN from system.columns\n"
                        + "where database like :database and table like :table and table not like '.%' and name like :column", params);
        return query(sql, (i, r) -> {
            String typeName = r.getValue("TYPE_NAME").asString();
            try {
                ProtonColumn column = ProtonColumn.of("", typeName);
                r.getValue("DATA_TYPE").update(JdbcTypeMapping.toJdbcType(typeMaps, column));
                r.getValue("COLUMN_SIZE").update(
                        column.getPrecision() > 0 ? column.getPrecision() : column.getDataType().getByteLength());
                if (column.isNullable()) {
                    r.getValue("NULLABLE").update(DatabaseMetaData.typeNullable);
                    r.getValue("IS_NULLABLE").update("YES");
                } else {
                    r.getValue("NULLABLE").update(DatabaseMetaData.typeNoNulls);
                    r.getValue("IS_NULLABLE").update("NO");
                }

                if (column.getDataType() == ProtonDataType.fixed_string) {
                    r.getValue("CHAR_OCTET_LENGTH").update(column.getPrecision());
                }

                Class<?> clazz = column.getDataType().getObjectClass();
                if (column.getScale() > 0 || Number.class.isAssignableFrom(clazz) || Date.class.isAssignableFrom(clazz)
                        || Temporal.class.isAssignableFrom(clazz)) {
                    r.getValue("DECIMAL_DIGITS").update(column.getScale());
                } else {
                    r.getValue("DECIMAL_DIGITS").resetToNullOrEmpty();
                }
            } catch (Exception e) {
                log.warn("Failed to read column: %s", typeName, e);
            }
        });
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern)
            throws SQLException {
        return empty("TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, "
                + "COLUMN_NAME string, GRANTOR nullable(string), GRANTEE string, PRIVILEGE string, "
                + "IS_GRANTABLE nullable(string)");
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
            throws SQLException {
        return empty("TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, "
                + "GRANTOR nullable(string), GRANTEE string, PRIVILEGE string, IS_GRANTABLE nullable(string)");
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable)
            throws SQLException {
        return getVersionColumns(catalog, schema, table);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return empty("SCOPE int16, COLUMN_NAME string, DATA_TYPE int32, TYPE_NAME string, "
                + "COLUMN_SIZE int32, BUFFER_LENGTH int32, DECIMAL_DIGITS int16, PSEUDO_COLUMN int16");
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return empty("TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, "
                + "COLUMN_NAME string, KEY_SEQ int16, PK_NAME string");
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return empty("PKTABLE_CAT nullable(string), PKTABLE_SCHEM nullable(string), PKTABLE_NAME string, "
                + "PKCOLUMN_NAME string, FKTABLE_CAT nullable(string), FKTABLE_SCHEM nullable(string), "
                + "FKTABLE_NAME string, FKCOLUMN_NAME string, KEY_SEQ int16, UPDATE_RULE int16, "
                + "DELETE_RULE int16, FK_NAME nullable(string), PK_NAME nullable(string), DEFERRABILITY int16");
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return getImportedKeys(catalog, schema, table);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return empty("PKTABLE_CAT nullable(string), PKTABLE_SCHEM nullable(string), PKTABLE_NAME string, "
                + "PKCOLUMN_NAME string, FKTABLE_CAT nullable(string), FKTABLE_SCHEM nullable(string), "
                + "FKTABLE_NAME string, FKCOLUMN_NAME string, KEY_SEQ int16, UPDATE_RULE int16, "
                + "DELETE_RULE int16, FK_NAME nullable(string), PK_NAME nullable(string), DEFERRABILITY int16");
    }

    private Object[] toTypeRow(String typeName, String aliasTo) {
        ProtonDataType type;
        try {
            type = ProtonDataType.of(typeName);
        } catch (Exception e) {
            if (aliasTo == null || aliasTo.isEmpty()) {
                return new Object[0];
            }
            try {
                type = ProtonDataType.of(aliasTo);
            } catch (Exception ex) {
                return new Object[0];
            }
        }

        String prefix = "";
        String suffix = "";
        String params = "";
        int nullable = DatabaseMetaData.typeNullable;
        int searchable = type == ProtonDataType.fixed_string || type == ProtonDataType.string
                ? DatabaseMetaData.typeSearchable
                : DatabaseMetaData.typePredBasic;
        int money = 0;
        switch (type) {
            case date:
            case date32:
            case datetime:
            case datetime32:
            case datetime64:
            case enum8:
            case enum16:
            case string:
            case fixed_string:
            case uuid:
                prefix = "'";
                suffix = "'";
                break;
            case array:
            case nested:
            case ring:
            case polygon:
            case multi_polygon:
                prefix = "[";
                suffix = "]";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            case aggregate_function:
            case tuple:
            case point:
                prefix = "(";
                suffix = ")";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            case map:
                prefix = "{";
                suffix = "}";
                nullable = DatabaseMetaData.typeNoNulls;
                break;
            default:
                break;
        }
        return new Object[] { typeName,
                JdbcTypeMapping.toJdbcType(typeMaps, ProtonColumn.of("", type, false, false, new String[0])),
                type.getMaxPrecision(), prefix, suffix, params, nullable, type.isCaseSensitive() ? 1 : 0, searchable,
                type.getMaxPrecision() > 0 && !type.isSigned() ? 1 : 0, money, 0,
                aliasTo == null || aliasTo.isEmpty() ? type.name() : aliasTo, type.getMinScale(), type.getMaxScale(), 0,
                0, 10 };
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Object[]> list = new ArrayList<>();
        try (ResultSet rs = query("select name, alias_to from system.data_type_families order by name")) {
            while (rs.next()) {
                Object[] row = toTypeRow(rs.getString(1), rs.getString(2));
                if (row.length > 0) {
                    list.add(row);
                }
            }
        }

        return fixed("TYPE_NAME string, DATA_TYPE int32, PRECISION int32, "
                + "LITERAL_PREFIX nullable(string), LITERAL_SUFFIX nullable(string), CREATE_PARAMS nullable(string), "
                + "NULLABLE int16, CASE_SENSITIVE uint8, SEARCHABLE int16, UNSIGNED_ATTRIBUTE uint8, "
                + "FIXED_PREC_SCALE uint8, AUTO_INCREMENT uint8, LOCAL_TYPE_NAME nullable(string), "
                + "MINIMUM_SCALE int16, MAXIMUM_SCALE int16, SQL_DATA_TYPE int32, SQL_DATETIME_SUB int32, "
                + "NUM_PREC_RADIX int32", list.toArray(new Object[0][]));
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        params.put("database",
                ProtonChecker.isNullOrEmpty(schema) ? "'%'" : ProtonValues.convertToQuotedString(schema));
        params.put("table",
                ProtonChecker.isNullOrEmpty(table) ? "'%'" : ProtonValues.convertToQuotedString(table));
        params.put("stat_index", String.valueOf(DatabaseMetaData.tableIndexStatistic));
        params.put("other_index", String.valueOf(DatabaseMetaData.tableIndexOther));
        return new CombinedResultSet(
                empty("TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, "
                        + "NON_UNIQUE uint8, INDEX_QUALIFIER nullable(string), INDEX_NAME nullable(string), "
                        + "TYPE int16, ORDINAL_POSITION int16, COLUMN_NAME nullable(string), ASC_OR_DESC nullable(string), "
                        + "CARDINALITY int64, PAGES int64, FILTER_CONDITION nullable(string)"),
                query(ProtonParameterizedQuery.apply(
                        "select null as TABLE_CAT, database as TABLE_SCHEM, table as TABLE_NAME, toUInt8(0) as NON_UNIQUE, "
                                + "null as INDEX_QUALIFIER, null as INDEX_NAME, to_int16(:statIndex) as TYPE, "
                                + "to_int16(0) as ORDINAL_POSITION, null as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "sum(rows) as CARDINALITY, uniq_exact(name) as PAGES, null as FILTER_CONDITION from system.parts "
                                + "where active = 1 and database like :database and table like :table group by database, table",
                        params), true),
                query(ProtonParameterizedQuery.apply(
                        "select null as TABLE_CAT, database as TABLE_SCHEM, table as TABLE_NAME, to_uint8(1) as NON_UNIQUE, "
                                + "type as INDEX_QUALIFIER, name as INDEX_NAME, to_int16(:otherIndex) as TYPE, "
                                + "to_int16(1) as ORDINAL_POSITION, expr as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION "
                                + "from system.data_skipping_indices where database like :database and table like :table",
                        params), true),
                query(ProtonParameterizedQuery.apply(
                        "select null as TABLE_CAT, database as TABLE_SCHEM, table as TABLE_NAME, to_uint8(1) as NON_UNIQUE, "
                                + "null as INDEX_QUALIFIER, name as INDEX_NAME, to_int16(:otherIndex) as TYPE, "
                                + "column_position as ORDINAL_POSITION, column as COLUMN_NAME, null as ASC_OR_DESC, "
                                + "sum(rows) as CARDINALITY, uniq_exact(partition) as PAGES, null as FILTER_CONDITION "
                                + "from system.projection_parts_columns where active = 1 and database like :database and table like :table "
                                + "group by database, table, name, column, column_position "
                                + "order by database, table, name, column_position",
                        params), true));
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY == type;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return true;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types)
            throws SQLException {
        return empty("TYPE_CAT nullable(string), TYPE_SCHEM nullable(string), TYPE_NAME string, "
                + "CLASS_NAME string, DATA_TYPE int32, REMARKS string, BASE_TYPE int16");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return empty("TYPE_CAT nullable(string), TYPE_SCHEM nullable(string), TYPE_NAME string, "
                + "SUPERTYPE_CAT nullable(string), SUPERTYPE_SCHEM nullable(string), SUPERTYPE_NAME string");
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return empty(
                "TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, SUPERTABLE_NAME string");
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        return empty("TYPE_CAT nullable(string), TYPE_SCHEM nullable(string), TYPE_NAME string, "
                + "ATTR_NAME string, DATA_TYPE int32, ATTR_TYPE_NAME string, ATTR_SIZE int32, "
                + "DECIMAL_DIGITS int32, NUM_PREC_RADIX int32, NULLABLE int32, REMARKS nullable(string), "
                + "ATTR_DEF nullable(string), SQL_DATA_TYPE int32, SQL_DATETIME_SUB int32, "
                + "CHAR_OCTET_LENGTH int32, ORDINAL_POSITION int32, IS_NULLABLE string, "
                + "SCOPE_CATALOG string, SCOPE_SCHEMA string, SCOPE_TABLE string, SOURCE_DATA_TYPE int16");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return connection.getServerVersion().getMajorVersion();
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return connection.getServerVersion().getMinorVersion();
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return ProtonDriver.specVersion.getMajorVersion();
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return ProtonDriver.specVersion.getMinorVersion();
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        Map<String, String> params = Collections.singletonMap("pattern",
                ProtonChecker.isNullOrEmpty(schemaPattern) ? "'%'"
                        : ProtonValues.convertToQuotedString(schemaPattern));
        return new CombinedResultSet(
                query(ProtonParameterizedQuery.apply("select name as TABLE_SCHEM, null as TABLE_CATALOG "
                        + "from system.databases where name like :pattern order by name", params)),
                query(ProtonParameterizedQuery.apply(
                        "select concat('jdbc(''', name, ''')') as TABLE_SCHEM, null as TABLE_CATALOG "
                                + "from jdbc('', 'SHOW DATASOURCES') where TABLE_SCHEM like :pattern order by name",
                        params), true));
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        ProtonParameterizedQuery q = ProtonParameterizedQuery
                .of(connection.getConfig(),
                        "select :name as NAME, to_int32(0) as MAX_LEN, :default as DEFAULT_VALUE, :desc as DESCRIPTION");
        StringBuilder builder = new StringBuilder();
        q.apply(builder, ProtonConnection.PROP_APPLICATION_NAME,
                connection.getClientInfo(ProtonConnection.PROP_APPLICATION_NAME), "Application name");
        builder.append(" union all ");
        q.apply(builder, ProtonConnection.PROP_CUSTOM_HTTP_HEADERS,
                connection.getClientInfo(ProtonConnection.PROP_CUSTOM_HTTP_HEADERS), "Custom HTTP headers");
        builder.append(" union all ");
        q.apply(builder, ProtonConnection.PROP_CUSTOM_HTTP_PARAMS,
                connection.getClientInfo(ProtonConnection.PROP_CUSTOM_HTTP_PARAMS),
                "Customer HTTP query parameters");
        return query(builder.toString());
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException {
        Map<String, String> params = new HashMap<>();
        params.put("filter", ProtonChecker.isNullOrEmpty(schemaPattern)
                || "system".contains(schemaPattern.toLowerCase(Locale.ROOT)) ? "1" : "0");
        params.put("pattern", ProtonChecker.isNullOrEmpty(functionNamePattern) ? "'%'"
                : ProtonValues.convertToQuotedString(functionNamePattern));

        String sql = ProtonParameterizedQuery.apply(
                "select * from (select null as FUNCTION_CAT, 'system' as FUNCTION_SCHEM, name as FUNCTION_NAME,\n"
                        + "concat('case-', case_insensitive ? 'in' : '', 'sensitive function', is_aggregate ? ' for aggregation' : '') as REMARKS,"
                        + "1 as FUNCTION_TYPE, name as SPECIFIC_NAME from system.functions\n"
                        + "where alias_to = '' and name like :pattern order by name union all\n"
                        + "select null as FUNCTION_CAT, 'system' as FUNCTION_SCHEM, name as FUNCTION_NAME,\n"
                        + "'case-sensistive table function' as REMARKS, 2 as FUNCTION_TYPE, name as SPECIFIC_NAME from system.table_functions\n"
                        + "order by name) where :filter",
                params);
        return query(sql);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        return empty("TABLE_CAT nullable(string), TABLE_SCHEM nullable(string), TABLE_NAME string, "
                + "COLUMN_NAME string, DATA_TYPE int32, COLUMN_SIZE int32, DECIMAL_DIGITS int32, "
                + "NUM_PREC_RADIX int32, COLUMN_USAGE string, REMARKS nullable(string), "
                + "CHAR_OCTET_LENGTH int32, IS_NULLABLE string");
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }
}
