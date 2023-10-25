package com.timeplus.proton;

import static com.timeplus.proton.domain.ProtonFormat.Native;
import static com.timeplus.proton.domain.ProtonFormat.RowBinary;
import static com.timeplus.proton.domain.ProtonFormat.TabSeparated;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Objects;

import org.apache.http.HttpEntity;
import org.apache.http.entity.InputStreamEntity;
import com.timeplus.proton.domain.ProtonCompression;
import com.timeplus.proton.domain.ProtonFormat;
import com.timeplus.proton.settings.ProtonQueryParam;
import com.timeplus.proton.util.ProtonStreamCallback;
import com.timeplus.proton.util.ProtonStreamHttpEntity;

public class Writer extends ConfigurableApi<Writer> {

    private ProtonFormat format = TabSeparated;
    private ProtonCompression compression = null;
    private String table = null;
    private String sql = null;
    private InputStreamProvider streamProvider = null;

    Writer(ProtonStatementImpl statement) {
        super(statement);

        dataCompression(ProtonCompression.none);
    }

    /**
     * Specifies format for further insert of data via send().
     *
     * @param format
     *            the format of the data to upload
     * @return this writer instance
     */
    public Writer format(ProtonFormat format) {
        if (null == format) {
            throw new NullPointerException("Format can not be null");
        }
        this.format = format;
        return this;
    }

    /**
     * Set table name for data insertion.
     *
     * @param table
     *            name of the table to upload the data to
     * @return this writer instance
     */
    public Writer table(String table) {
        this.sql = null;
        this.table = table;
        return this;
    }

    /**
     * Set SQL for data insertion.
     *
     * @param sql
     *            in a form "INSERT INTO table_name [(X,Y,Z)] VALUES "
     * @return this writer instance
     */
    public Writer sql(String sql) {
        this.sql = sql;
        this.table = null;
        return this;
    }

    /**
     * Specifies data input stream.
     *
     * @param stream
     *            a stream providing the data to upload
     * @return this writer instance
     */
    public Writer data(InputStream stream) {
        streamProvider = new HoldingInputProvider(stream);
        return this;
    }

    /**
     * Specifies data input stream, and the format to use.
     *
     * @param stream
     *            a stream providing the data to upload
     * @param format
     *            the format of the data to upload
     * @return this writer instance
     */
    public Writer data(InputStream stream, ProtonFormat format) {
        return format(format).data(stream);
    }

    /**
     * Shortcut method for specifying a file as an input.
     *
     * @param input
     *            the file to upload
     * @return this writer instance
     */
    public Writer data(File input) {
        streamProvider = new FileInputProvider(input);
        return this;
    }

    public Writer data(InputStream stream, ProtonFormat format, ProtonCompression compression) {
        return dataCompression(compression).format(format).data(stream);
    }

    public Writer data(File input, ProtonFormat format, ProtonCompression compression) {
        return dataCompression(compression).format(format).data(input);
    }

    public Writer dataCompression(ProtonCompression compression) {
        this.compression = Objects.requireNonNull(compression, "Compression can not be null");
        this.addDbParam(ProtonQueryParam.COMPRESS, String.valueOf(compression != ProtonCompression.none));
        
        return this;
    }

    public Writer data(File input, ProtonFormat format) {
        return format(format).data(input);
    }

    /**
     * Method to call, when Writer is fully configured.
     */
    public void send() throws SQLException {
        HttpEntity entity;
        try {
            InputStream stream;
            if (null == streamProvider || null == (stream = streamProvider.get())) {
                throw new IOException("No input data specified");
            }
            entity = new InputStreamEntity(stream);
        } catch (IOException err) {
            throw new SQLException(err);
        }
        send(entity);
    }

    private void send(HttpEntity entity) throws SQLException {
        statement.sendStream(this, entity);
    }

    /**
     * Allows to send stream of data to Proton.
     *
     * @param sql
     *            in a form of "INSERT INTO table_name (X,Y,Z) VALUES "
     * @param data
     *            where to read data from
     * @param format
     *            format of data in InputStream
     * @throws SQLException
     *             if the upload fails
     */
    public void send(String sql, InputStream data, ProtonFormat format) throws SQLException {
        sql(sql).data(data).format(format).send();
    }

    /**
     * Convenient method for importing the data into table.
     *
     * @param table
     *            table name
     * @param data
     *            source data
     * @param format
     *            format of data in InputStream
     * @throws SQLException
     *             if the upload fails
     */
    public void sendToTable(String table, InputStream data, ProtonFormat format) throws SQLException {
        table(table).data(data).format(format).send();
    }

    /**
     * Sends the data in {@link ProtonFormat#RowBinary RowBinary} or in
     * {@link ProtonFormat#Native Native} format.
     *
     * @param sql
     *            the SQL statement to execute
     * @param callback
     *            data source for the upload
     * @param format
     *            the format to use, either {@link ProtonFormat#RowBinary
     *            RowBinary} or {@link ProtonFormat#Native Native}
     * @throws SQLException
     *             if the upload fails
     */
    public void send(String sql, ProtonStreamCallback callback, ProtonFormat format) throws SQLException {
        if (!(RowBinary.equals(format) || Native.equals(format))) {
            throw new SQLException("Wrong binary format - only RowBinary and Native are supported");
        }

        format(format).sql(sql).send(new ProtonStreamHttpEntity(callback, statement.getConnection().getTimeZone(), statement.properties));
    }

    String getSql() {
        if (null != table) {
            return "INSERT INTO " + table + " FORMAT " + format;
        } else if (null != sql) {
            String result = sql;
            if (!ProtonFormat.containsFormat(result)) {
                result += " FORMAT " + format;
            }
            return result;
        } else {
            throw new IllegalArgumentException("Neither table nor SQL clause are specified");
        }
    }

    private interface InputStreamProvider {
        InputStream get() throws IOException;
    }

    private static final class FileInputProvider implements InputStreamProvider {
        private final File file;

        private FileInputProvider(File file) {
            this.file = file;
        }

        @Override
        public InputStream get() throws IOException {
            return new FileInputStream(file);
        }
    }

    private static final class HoldingInputProvider implements InputStreamProvider {
        private final InputStream stream;

        private HoldingInputProvider(InputStream stream) {
            this.stream = stream;
        }

        @Override
        public InputStream get() throws IOException {
            return stream;
        }
    }

    public ProtonCompression getCompression() {
        return compression;
    }
}
