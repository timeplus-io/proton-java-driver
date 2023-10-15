package ru.yandex.proton.response.parser;

import java.sql.Array;
import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.proton.ProtonArray;
import ru.yandex.proton.domain.ProtonDataType;
import ru.yandex.proton.response.ByteFragment;
import ru.yandex.proton.response.ProtonColumnInfo;
import ru.yandex.proton.util.ProtonArrayUtil;

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
