package com.timeplus.proton.response.parser;

import java.io.IOException;
import java.sql.SQLException;
import java.util.TimeZone;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;
import com.timeplus.proton.util.ProtonBitmap;

final class ProtonBitmapParser extends ProtonValueParser<ProtonBitmap> {
    private static ProtonBitmapParser instance;

    static ProtonBitmapParser getInstance() {
        if (instance == null) {
            instance = new ProtonBitmapParser();
        }
        return instance;
    }

    private ProtonBitmapParser() {
        // prevent instantiation
    }

    @Override
    public ProtonBitmap parse(ByteFragment value, ProtonColumnInfo columnInfo, TimeZone resultTimeZone)
            throws SQLException {
        if (value.isNull()) {
            return null;
        }

        // https://github.com/Clickhouse/Clickhouse/blob/master/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L100
        ProtonBitmap rb = ProtonBitmap.wrap();

        // FIXME use DataInput/DataOutput for stream after switching to RowBinary
        byte[] bytes = value.unescape();
        if (bytes.length == 0) {
            return rb;
        }

        try {
            rb = ProtonBitmap.deserialize(bytes, columnInfo.getArrayBaseType());
        } catch (IOException e) {
            throw new SQLException("Failed to deserialize ProtonBitmap", e);
        }

        return rb;
    }

    @Override
    protected ProtonBitmap getDefaultValue() {
        return ProtonBitmap.wrap();
    }
}
