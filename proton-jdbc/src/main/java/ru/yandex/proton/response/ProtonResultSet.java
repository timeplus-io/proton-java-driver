package ru.yandex.proton.response;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import ru.yandex.proton.ProtonArray;
import ru.yandex.proton.ProtonConnection;
import ru.yandex.proton.ProtonStatement;
import ru.yandex.proton.domain.ProtonDataType;
import ru.yandex.proton.except.ProtonExceptionSpecifier;
import ru.yandex.proton.except.ProtonUnknownException;
import ru.yandex.proton.response.parser.ProtonValueParser;
import ru.yandex.proton.settings.ProtonProperties;
import ru.yandex.proton.util.ProtonArrayUtil;
import ru.yandex.proton.util.ProtonBitmap;
import ru.yandex.proton.util.ProtonValueFormatter;
import ru.yandex.proton.util.Utils;


public class ProtonResultSet extends AbstractResultSet {

    private static final long[] EMPTY_LONG_ARRAY = new long[0];

    private final TimeZone dateTimeTimeZone;
    private final TimeZone dateTimeZone;

    private final StreamSplitter bis;

    private final String db;
    private final String table;

    private List<ProtonColumnInfo> columns;

    private int maxRows;

    // current line
    protected ByteFragment[] values;
    // 1-based
    private int lastReadColumn;

    // next line
    protected ByteFragment nextLine;

    // total lines
    private ByteFragment totalLine;

    // row counter
    protected int rowNumber;

    // statement result set belongs to
    private final ProtonStatement statement;

    private final ProtonProperties properties;

    private boolean usesWithTotals;

    // NOTE this can't be used for `isLast` impl because
    // it does not do prefetch. It is effectively a witness
    // to the fact that rs.next() returned false.
    private boolean lastReached = false;

    private boolean isAfterLastReached = false;

    public ProtonResultSet(InputStream is, int bufferSize, String db, String table,
        boolean usesWithTotals, ProtonStatement statement, TimeZone timeZone,
        ProtonProperties properties) throws IOException
    {
        this.db = db;
        this.table = table;
        this.statement = statement;
        this.properties = properties;
        this.usesWithTotals = usesWithTotals;
        this.dateTimeTimeZone = timeZone;
        this.dateTimeZone = properties.isUseServerTimeZoneForDates()
            ? timeZone
            : TimeZone.getDefault(); // FIXME should be the timezone defined in useTimeZone?
        bis = new StreamSplitter(is, (byte) 0x0A, bufferSize);  ///   \n
        ByteFragment headerFragment = bis.next();
        if (headerFragment == null) {
            throw new IllegalArgumentException("Proton response without column names");
        }
        String header = headerFragment.asString(true);
        if (header.startsWith("Code: ") && !header.contains("\t")) {
            is.close();
            throw new IOException("Proton error: " + header);
        }
        String[] cols = toStringArray(headerFragment);
        ByteFragment typesFragment = bis.next();
        if (typesFragment == null) {
            throw new IllegalArgumentException("Proton response without column types");
        }
        String[] types = toStringArray(typesFragment);
        columns = new ArrayList<>(cols.length);
        TimeZone tz = null;
        try {
            if (statement != null && statement.getConnection() instanceof ProtonConnection) {
                tz = ((ProtonConnection)statement.getConnection()).getServerTimeZone();
            }
        } catch (SQLException e) {
            // ignore the error
        }

        if (tz == null) {
            tz = timeZone;
        }
        
        for (int i = 0; i < cols.length; i++) {
            columns.add(ProtonColumnInfo.parse(types[i], cols[i], tz));
        }
    }

    private static String[] toStringArray(ByteFragment headerFragment) {
        ByteFragment[] split = headerFragment.split((byte) 0x09);
        String[] c = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            String name = split[i].asString(true);
            c[i] = name;
        }
        return c;
    }

    /**
     * Check if there is another row.
     *
     * @return {@code true} if this result set has another row after the current
     *         cursor position, {@code false} else
     * @throws SQLException if something goes wrong
     */
    protected boolean hasNext() throws SQLException {
        if (nextLine == null && !lastReached) {
            try {
                nextLine = bis.next();

                if (nextLine == null
                    || (maxRows != 0 && rowNumber >= maxRows)
                    || (usesWithTotals && nextLine.length() == 0)) {
                    if (usesWithTotals) {
                        if (onTheSeparatorRow()) {
                            totalLine = bis.next();
                            endOfStream();
                        } // otherwise do not close the stream, it is single column or invalid result set case
                    } else {
                        endOfStream();
                    }
                }
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
        return nextLine != null;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return rowNumber == 0 && hasNext();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return isAfterLastReached;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return rowNumber == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !hasNext();
     // && !isAfterLastReached should be probably added,
     // but it may brake compatibility with the previous implementation
    }

    private void endOfStream() throws IOException {
        bis.close();
        lastReached = true;
        nextLine = null;
    }

    @Override
    public boolean next() throws SQLException {
        if (hasNext()) {
            values = nextLine.split((byte) 0x09);
            checkValues(columns, values, nextLine);
            nextLine = null;
            rowNumber += 1;
            return true;
        }
        isAfterLastReached = true;
        return false;
    }

    private boolean onTheSeparatorRow() throws IOException {
        // test bis vs "\n???\nEOF" pattern if not then rest to current position
        bis.mark();
        boolean onSeparatorRow = bis.next() !=null && bis.next() == null;
        bis.reset();

        return onSeparatorRow;
    }

    private static void checkValues(List<ProtonColumnInfo> columns, ByteFragment[] values,
        ByteFragment fragment) throws SQLException
    {
        if (columns.size() != values.length) {
            throw ProtonExceptionSpecifier.specify(fragment.asString());
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            bis.close();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        try {
            return bis.isClosed();
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    public void getTotals() throws SQLException {
        if (!usesWithTotals) {
            throw new IllegalStateException("Cannot get totals when totals are not being used.");
        }

        nextLine = totalLine;

        this.next();
    }

    // this method is mocked in a test, do not make it final :-)
    List<ProtonColumnInfo> getColumns() {
        return Collections.unmodifiableList(columns);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new ProtonResultSetMetaData(this);
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) {
            throw new IllegalStateException("You should get something before check nullability");
        }
        return getValue(lastReadColumn).isNull();
    }

    @Override
    public int getInt(String column) throws SQLException {
        return getInt(findColumn(column));
    }

    @Override
    public boolean getBoolean(String column) throws SQLException {
        return getBoolean(findColumn(column));
    }

    @Override
    public long getLong(String column) throws SQLException {
        return getLong(findColumn(column));
    }

    @Override
    public String getString(String column) throws SQLException {
        return getString(findColumn(column));
    }

    @Override
    public byte[] getBytes(String column) throws SQLException {
        return getBytes(findColumn(column));
    }

    @Override
    public Timestamp getTimestamp(String column) throws SQLException {
        return getTimestamp(findColumn(column));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        ProtonColumnInfo columnInfo = getColumnInfo(columnIndex);
        TimeZone tz = getEffectiveTimeZone(columnInfo);
        
        return ProtonValueParser.getParser(Timestamp.class).parse(
            getValue(columnIndex), columnInfo, tz);
    }

    private TimeZone getEffectiveTimeZone(ProtonColumnInfo columnInfo) {
        TimeZone tz = null;

        if (columnInfo.getProtonDataType() == ProtonDataType.Date) {
            tz = dateTimeZone;
        } else {
            tz = properties.isUseServerTimeZone() ? null : dateTimeTimeZone;
        }

        return tz;
    }

    @Override
    public Timestamp getTimestamp(String column, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(column), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public short getShort(String column) throws SQLException {
        return getShort(findColumn(column));
    }

    @Override
    public byte getByte(String column) throws SQLException {
        return getByte(findColumn(column));
    }

    @Override
    public long[] getLongArray(String column) throws SQLException {
        return getLongArray(findColumn(column));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        ProtonColumnInfo colInfo = getColumnInfo(columnIndex);
        if (colInfo.getProtonDataType() != ProtonDataType.Array) {
            throw new SQLException("Column not an array");
        }

        final Object array;
        switch (colInfo.getArrayBaseType()) {
        case Date :
            array = ProtonArrayUtil.parseArray(
                getValue(columnIndex),
                properties.isUseObjectsInArrays(),
                dateTimeZone,
                colInfo
            );
            break;
        default :
            TimeZone timeZone = colInfo.getTimeZone() != null
                ? colInfo.getTimeZone()
                : dateTimeTimeZone;
            array = ProtonArrayUtil.parseArray(
                getValue(columnIndex),
                properties.isUseObjectsInArrays(),
                timeZone,
                colInfo
            );
            break;
        }
        return new ProtonArray(colInfo.getArrayBaseType(), array);
    }

    @Override
    public Array getArray(String column) throws SQLException {
        return getArray(findColumn(column));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public String getString(int colNum) throws SQLException {
        // FIXME this won't help when datetime string is in a nested structure
        ProtonColumnInfo columnInfo = getColumnInfo(colNum);
        ByteFragment value = getValue(colNum);
        ProtonDataType dataType = columnInfo.getProtonDataType();
        
        // Date is time-zone netural so let's skip that.
        // DateTime string returned from Server however is always formatted using server/column
        // timezone. The behaviour may change when
        // https://github.com/Clickhouse/Clickhouse/issues/4548 is addressed
        if (!properties.isUseServerTimeZone() && (
                dataType == ProtonDataType.DateTime
                || dataType == ProtonDataType.DateTime32
                || dataType == ProtonDataType.DateTime64)) {
            TimeZone serverTimeZone = columnInfo.getTimeZone();
            if (serverTimeZone == null) {
                serverTimeZone = ((ProtonConnection) getStatement().getConnection()).getServerTimeZone();
            }
            TimeZone clientTimeZone = Utils.isNullOrEmptyString(properties.getUseTimeZone())
                ? TimeZone.getDefault()
                : TimeZone.getTimeZone(properties.getUseTimeZone());

            if (!clientTimeZone.equals(serverTimeZone)) {
                Timestamp newTs = ProtonValueParser.getParser(Timestamp.class)
                    .parse(value, columnInfo, serverTimeZone);
                value = ByteFragment.fromString(ProtonValueFormatter.formatTimestamp(newTs, clientTimeZone));
            }
        }

        return ProtonValueParser.getParser(String.class).parse(value, columnInfo, null);
    }

    @Override
    public int getInt(int colNum) throws SQLException {
        return ProtonValueParser.parseInt(
            getValue(colNum), getColumnInfo(colNum));
    }

    @Override
    public boolean getBoolean(int colNum) throws SQLException {
        return ProtonValueParser.parseBoolean(
            getValue(colNum), getColumnInfo(colNum));
    }

    @Override
    public long getLong(int colNum) throws SQLException {
        return ProtonValueParser.parseLong(
            getValue(colNum), getColumnInfo(colNum));
    }

    @Override
    public byte[] getBytes(int colNum) {
        return toBytes(getValue(colNum));
    }

    /**
     * Tries to parse the value as a timestamp using the connection time zone if
     * applicable and return its representation as milliseconds since epoch.
     *
     * @param colNum
     *            column number
     * @return timestamp value as milliseconds since epoch
     * @deprecated prefer to use regular JDBC API methods, e.g.
     *             {@link #getTimestamp(int)} or {@link #getObject(int, Class)}
     *             using {@link Instant}
     */
    @Deprecated
    public Long getTimestampAsLong(int colNum) {
        ProtonColumnInfo columnInfo = getColumnInfo(colNum);
        TimeZone tz = getEffectiveTimeZone(columnInfo);
        return getTimestampAsLong(colNum, tz);
    }

    /**
     * Tries to parse the value as a timestamp and return its representation as
     * milliseconds since epoch
     *
     * @param colNum
     *            the column number
     * @param timeZone
     *            time zone to use when parsing date / date time values
     * @return value interpreted as timestamp as milliseconds since epoch
     * @deprecated prefer to use regular JDBC API method
     */
    @Deprecated
    public Long getTimestampAsLong(int colNum, TimeZone timeZone) {
        ByteFragment value = getValue(colNum);
        if (value.isNull() || value.asString().equals("0000-00-00 00:00:00")) {
            return null;
        }
        try {
            Instant instant = ProtonValueParser.getParser(Instant.class)
                .parse(value, getColumnInfo(colNum), timeZone);
            return Long.valueOf(instant.toEpochMilli());
        } catch (SQLException sqle) {
            throw new RuntimeException(sqle);
        }
    }

    @Override
    public short getShort(int colNum) throws SQLException {
        return ProtonValueParser.parseShort(
            getValue(colNum), getColumnInfo(colNum));
    }

    @Override
    public byte getByte(int colNum) {
        return toByte(getValue(colNum));
    }

    /**
     * Parse the value in current row at column index {@code colNum} as an array
     * of long
     *
     * @param colNum
     *            column number
     * @return an array of longs
     * @throws SQLException
     *             if the value cannot be interpreted as {@code long[]}
     * @deprecated prefer to use regular JDBC API
     */
    @Deprecated
    public long[] getLongArray(int colNum) throws SQLException {
        return toLongArray(getValue(colNum), getColumnInfo(colNum));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return ProtonValueParser.parseFloat(
            getValue(columnIndex), getColumnInfo(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return ProtonValueParser.parseDouble(
            getValue(columnIndex), getColumnInfo(columnIndex));
    }

    @Override
    public Statement getStatement() {
        return statement;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        ProtonColumnInfo columnInfo = getColumnInfo(columnIndex);
        TimeZone tz = getEffectiveTimeZone(columnInfo);
        return ProtonValueParser.getParser(Date.class).parse(
            getValue(columnIndex), columnInfo, tz);
    }

    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        ProtonColumnInfo columnInfo = getColumnInfo(columnIndex);
        TimeZone tz = getEffectiveTimeZone(columnInfo);
        return ProtonValueParser.getParser(Time.class).parse(
            getValue(columnIndex), columnInfo, tz);
    }

    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        try {
            if (getValue(columnIndex).isNull()) {
                return null;
            }
            ProtonColumnInfo columnInfo = getColumnInfo(columnIndex);
            ProtonDataType chType = columnInfo.getProtonDataType();
            switch (chType.getSqlType()) {
            case Types.BIGINT:
                if (chType == ProtonDataType.UInt64) {
                    return getObject(columnIndex, BigInteger.class);
                }
                return getObject(columnIndex, Long.class);
            case Types.INTEGER:
                if (!chType.isSigned()){
                    return getObject(columnIndex, Long.class);
                }
                return getObject(columnIndex, Integer.class);
            case Types.TINYINT:
            case Types.SMALLINT:
                return getObject(columnIndex, Integer.class);
            case Types.VARCHAR:     return getString(columnIndex);
            case Types.REAL:        return getObject(columnIndex, Float.class);
            case Types.FLOAT:
            case Types.DOUBLE:      return getObject(columnIndex, Double.class);
            case Types.DATE:        return getDate(columnIndex);
            case Types.TIMESTAMP:   return getTimestamp(columnIndex);
            case Types.BLOB:        return getString(columnIndex);
            case Types.ARRAY:       return getArray(columnIndex);
            case Types.DECIMAL:     return getBigDecimal(columnIndex);
            case Types.NUMERIC:     return getBigInteger(columnIndex);
            default:
                // do not return
            }
            switch (chType) {
            // case Array:
            // case Tuple:
            case AggregateFunction:
                // TODO support more functions
                if ("groupBitmap".equals(columnInfo.getFunctionName())) {
                    ProtonDataType innerType = columnInfo.getArrayBaseType();
                    switch (innerType) {
                    // seems signed integers are not supported in Proton
                    case Int8:
                    case Int16:
                    case Int32:
                    case Int64:
                    case UInt8:
                    case UInt16:
                    case UInt32:
                    case UInt64:
                        return getObject(columnIndex, ProtonBitmap.class);
                    default:
                        break;
                    }
                }
                return getString(columnIndex);
            case Map:
            case UUID :
                return getObject(columnIndex, chType.getJavaClass());
            default :
                return getString(columnIndex);
            }
        } catch (Exception e) {
            throw new ProtonUnknownException(
                "Parse exception: " + values[columnIndex - 1].toString(),
                e);
        }
    }

    /////////////////////////////////////////////////////////

    private static byte toByte(ByteFragment value) {
        if (value.isNull()) {
            return 0;
        }
        return Byte.parseByte(value.asString());
    }

    private static byte[] toBytes(ByteFragment value) {
        if (value.isNull()) {
            return null;
        }
        return value.unescape();
    }

    static long[] toLongArray(ByteFragment value, ProtonColumnInfo columnInfo) throws SQLException {
        if (value.isNull()) {
            return null;
        }
        if (value.charAt(0) != '[' || value.charAt(value.length() - 1) != ']') {
            throw new IllegalArgumentException("not an array: " + value);
        }
        if (value.length() == 2) {
            return EMPTY_LONG_ARRAY;
        }
        ByteFragment trim = value.subseq(1, value.length() - 2);
        ByteFragment[] values = trim.split((byte) ',');
        long[] result = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = ProtonValueParser.parseLong(values[i], columnInfo);
        }
        return result;
    }

    //////

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getRow() throws SQLException {
        return rowNumber;
    }

    public String getDb() {
        return db;
    }

    public String getTable() {
        return table;
    }

    public void setMaxRows(int maxRows) {
        this.maxRows = maxRows;
    }

    /////

    // 1-based index in column list
    @Override
    public int findColumn(String column) throws SQLException {
        if (column == null || column.isEmpty()) {
            throw new ProtonUnknownException(
                "column name required", null);
        }
        for (int i = 0; i < columns.size(); i++) {
            if (column.equalsIgnoreCase(columns.get(i).getColumnName())) {
                return i+1;
            }
        }
        throw new SQLException("no column " + column + " in columns list " + getColumnNamesString());
    }

    private ByteFragment getValue(int colNum) {
        lastReadColumn = colNum;
        return values[colNum - 1];
    }

    private ProtonColumnInfo getColumnInfo(int colNum) {
        return columns.get(colNum - 1);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (String.class.equals(type)) {
            return (T) getString(columnIndex);
        }

        ProtonColumnInfo columnInfo = getColumnInfo(columnIndex);
        TimeZone tz = getEffectiveTimeZone(columnInfo);
        return columnInfo.isArray()
            ? (Array.class.isAssignableFrom(type) ? (T) getArray(columnIndex) : (T) getArray(columnIndex).getArray())
            : ProtonValueParser.getParser(type).parse(getValue(columnIndex), columnInfo, tz);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    /**
     * Retrieve the results in &quot;raw&quot; form.
     *
     * @return the results as an array of {@link ByteFragment}s
     * @deprecated prefer to use regular JDBC API to retrieve the results
     */
    @Deprecated
    public ByteFragment[] getValues() {
        return values;
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)  throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return ProtonValueParser.getParser(BigDecimal.class)
            .parse(getValue(columnIndex), getColumnInfo(columnIndex), null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal result = ProtonValueParser.getParser(BigDecimal.class)
            .parse(getValue(columnIndex), getColumnInfo(columnIndex), null);
        return result != null
            ? result.setScale(scale, RoundingMode.HALF_UP)
            : null;
    }

    public BigInteger getBigInteger(String columnLabel)  throws SQLException {
        return getBigInteger(findColumn(columnLabel));
    }

    public BigInteger getBigInteger(int columnIndex) throws SQLException {
        BigDecimal dec = getBigDecimal(columnIndex);
        return dec == null ? null : dec.toBigInteger();
    }

    public String[] getColumnNames() {
        String[] columnNames = new String[columns.size()];
        for (int i = 0; i < columns.size(); ++i) {
            columnNames[i] = columns.get(i).getColumnName();
        }
        return columnNames;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // ignore perfomance hint
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        // ignore perfomance hint
    }


    @Override
    public String toString() {
        return "ProtonResultSet{" +
            "dateTimeTimeZone=" + dateTimeTimeZone.toString() +
            ", dateTimeZone=" + dateTimeZone.toString() +
            ", bis=" + bis +
            ", db='" + db + '\'' +
            ", table='" + table + '\'' +
            ", columns=" + getColumnNamesString() +
            ", maxRows=" + maxRows +
            ", values=" + Arrays.toString(values) +
            ", lastReadColumn=" + lastReadColumn +
            ", nextLine=" + nextLine +
            ", rowNumber=" + rowNumber +
            ", statement=" + statement +
            '}';
    }

    private String getColumnNamesString() {
        StringBuilder sb = new StringBuilder();
        for (ProtonColumnInfo info : columns) {
            sb.append(info.getColumnName()).append(' ');
        }
        return sb.substring(0, sb.length() - 1);
    }

}
