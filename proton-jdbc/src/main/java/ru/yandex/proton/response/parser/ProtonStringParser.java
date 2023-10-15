package ru.yandex.proton.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import ru.yandex.proton.response.ByteFragment;
import ru.yandex.proton.response.ProtonColumnInfo;

final class ProtonStringParser extends ProtonValueParser<String> {

    private static ProtonStringParser instance;

    static ProtonStringParser getInstance() {
        if (instance == null) {
            instance = new ProtonStringParser();
        }
        return instance;
    }

    private ProtonStringParser() {
        // prevent regular instantiation
    }

    @Override
    public String parse(ByteFragment value, ProtonColumnInfo columnInfo, TimeZone resultTimeZone)
            throws SQLException {
        return value.asString(true);
    }
}
