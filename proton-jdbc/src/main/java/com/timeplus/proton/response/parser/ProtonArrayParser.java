package com.timeplus.proton.response.parser;

import java.sql.Array;
import java.sql.SQLException;
import java.util.TimeZone;

import com.timeplus.proton.ProtonArray;
import com.timeplus.proton.domain.ProtonDataType;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;
import com.timeplus.proton.util.ProtonArrayUtil;

final class ProtonArrayParser extends ProtonValueParser<Array> {

    private static ProtonArrayParser instance;

    static ProtonArrayParser getInstance() {
        if (instance == null) {
            instance = new ProtonArrayParser();
        }
        return instance;
    }

    private ProtonArrayParser() {
        // prevent instantiation
    }

    @Override
    public Array parse(ByteFragment value, ProtonColumnInfo columnInfo, TimeZone resultTimeZone)
            throws SQLException {
        if (columnInfo.getProtonDataType() != ProtonDataType.Array) {
            throw new SQLException("Column not an array");
        }

        if (value.isNull()) {
            return null;
        }

        final Object array;
        switch (columnInfo.getArrayBaseType()) {
            case Date:
                // FIXME: properties.isUseObjectsInArrays()
                array = ProtonArrayUtil.parseArray(value, false, resultTimeZone, columnInfo);
                break;
            default:
                // properties.isUseObjectsInArrays()
                TimeZone timeZone = columnInfo.getTimeZone() != null ? columnInfo.getTimeZone() : resultTimeZone;
                array = ProtonArrayUtil.parseArray(value, false, timeZone, columnInfo);
                break;
        }

        return new ProtonArray(columnInfo.getArrayBaseType(), array);
    }

    @Override
    protected Array getDefaultValue() {
        return null;
    }
}
