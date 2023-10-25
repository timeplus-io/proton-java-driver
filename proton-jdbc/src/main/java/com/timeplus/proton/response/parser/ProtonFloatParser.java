package com.timeplus.proton.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import com.timeplus.proton.except.ProtonUnknownException;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonFloatParser extends ProtonValueParser<Float> {

    private static ProtonFloatParser instance;

    static ProtonFloatParser getInstance() {
        if (instance == null) {
            instance = new ProtonFloatParser();
        }
        return instance;
    }

    private ProtonFloatParser() {
        // prevent instantiation
    }

    @Override
    public Float parse(ByteFragment value, ProtonColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        if (value.isNull()) {
            return null;
        }
        if (value.isNaN()) {
            return Float.valueOf(Float.NaN);
        }
        String s = value.asString();
        switch (s) {
            case "+inf":
            case "inf":
                return Float.valueOf(Float.POSITIVE_INFINITY);
            case "-inf":
                return Float.valueOf(Float.NEGATIVE_INFINITY);
            default:
                try {
                    return Float.valueOf(s);
                } catch (NumberFormatException nfe) {
                    throw new ProtonUnknownException(
                        "Error parsing '" + s + "' as Float",
                        nfe);
                }
        }
    }

    @Override
    protected Float getDefaultValue() {
        return Float.valueOf(0);
    }

}
