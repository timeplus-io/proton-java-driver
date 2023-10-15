package ru.yandex.proton.response.parser;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.proton.response.ProtonColumnInfo;

final class ProtonLocalTimeParser extends ProtonDateValueParser<LocalTime> {

    private static ProtonLocalTimeParser instance;

    static ProtonLocalTimeParser getInstance() {
        if (instance == null) {
            instance = new ProtonLocalTimeParser();
        }
        return instance;
    }

    private ProtonLocalTimeParser() {
        super(LocalTime.class);
    }

    @Override
    LocalTime parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return LocalTime.MIDNIGHT;
    }

    @Override
    LocalTime parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToLocalDateTime(value, columnInfo, timeZone).toLocalTime();
    }

    @Override
    LocalTime parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalTime(value);
    }

    @Override
    LocalTime parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME);
        } catch (DateTimeParseException dtpe) {
            // try different pattern
        }
        try {
            return LocalTime.parse(value, DateTimeFormatter.ISO_OFFSET_TIME);
        } catch (DateTimeParseException dtpe) {
            // try different pattern
        }
        return parseAsLocalTime(value);
    }

}
