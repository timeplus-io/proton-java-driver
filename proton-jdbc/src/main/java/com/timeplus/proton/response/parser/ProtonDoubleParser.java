package com.timeplus.proton.response.parser;

import java.sql.SQLException;
import java.util.TimeZone;

import com.timeplus.proton.except.ProtonUnknownException;
import com.timeplus.proton.response.ByteFragment;
import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonDoubleParser extends ProtonValueParser<Double> {

    private static ProtonDoubleParser instance;

    static ProtonDoubleParser getInstance() {
        if (instance == null) {
            instance = new ProtonDoubleParser();
        }
        return instance;
    }

    private ProtonDoubleParser() {
        // prevent instantiation
    }

    @Override
    public Double parse(ByteFragment value, ProtonColumnInfo columnInfo,
        TimeZone resultTimeZone) throws SQLException
    {
        if (value.isNull()) {
            return null;
        }
        if (value.isNaN()) {
            return Double.valueOf(Double.NaN);
        }
        String s = value.asString();
        switch (s) {
            case "+inf":
            case "inf":
                return Double.valueOf(Double.POSITIVE_INFINITY);
            case "-inf":
                return Double.valueOf(Double.NEGATIVE_INFINITY);
            default:
                try {
                    return Double.valueOf(s);
                } catch (NumberFormatException nfe) {
                    throw new ProtonUnknownException(
                        "Error parsing '" + s + "' as Double",
                        nfe);
                }
        }
    }

    @Override
    protected Double getDefaultValue() {
        return Double.valueOf(0);
    }

}
