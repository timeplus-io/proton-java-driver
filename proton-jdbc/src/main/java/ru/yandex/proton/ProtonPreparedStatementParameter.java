package ru.yandex.proton;

import ru.yandex.proton.util.ProtonValueFormatter;

import java.util.TimeZone;

public final class ProtonPreparedStatementParameter {

    private static final ProtonPreparedStatementParameter NULL_PARAM =
        new ProtonPreparedStatementParameter(null, false);

    private static final ProtonPreparedStatementParameter TRUE_PARAM =
            new ProtonPreparedStatementParameter("1", false);

    private static final ProtonPreparedStatementParameter FALSE_PARAM =
            new ProtonPreparedStatementParameter("0", false);

    private final String stringValue;
    private final boolean quoteNeeded;

    public static ProtonPreparedStatementParameter fromObject(Object x,
        TimeZone dateTimeZone, TimeZone dateTimeTimeZone)
    {
        if (x == null) {
            return NULL_PARAM;
        }
        return new ProtonPreparedStatementParameter(
            ProtonValueFormatter.formatObject(x, dateTimeZone, dateTimeTimeZone),
            ProtonValueFormatter.needsQuoting(x));
    }

    public static ProtonPreparedStatementParameter nullParameter() {
        return NULL_PARAM;
    }

    public static ProtonPreparedStatementParameter boolParameter(boolean value) {
        return value ? TRUE_PARAM : FALSE_PARAM;
    }

    public ProtonPreparedStatementParameter(String stringValue,
        boolean quoteNeeded)
    {
        this.stringValue = stringValue == null
            ? ProtonValueFormatter.NULL_MARKER
            : stringValue;
        this.quoteNeeded = quoteNeeded;
    }

    String getRegularValue() {
        return !ProtonValueFormatter.NULL_MARKER.equals(stringValue)
            ? quoteNeeded
                ? "'" + stringValue + "'"
                : stringValue
            : "null";
    }

    String getBatchValue() {
        return stringValue;
    }


    @Override
    public String toString() {
        return stringValue;
    }

}