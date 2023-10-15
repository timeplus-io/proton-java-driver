package ru.yandex.proton.response.parser;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.proton.response.ProtonColumnInfo;

final class ProtonLocalDateParser extends ProtonDateValueParser<LocalDate> {

    private static ProtonLocalDateParser instance;

    static ProtonLocalDateParser getInstance() {
        if (instance == null) {
            instance = new ProtonLocalDateParser();
        }
        return instance;
    }

    private ProtonLocalDateParser() {
        super(LocalDate.class);
    }

    @Override
    LocalDate parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateToLocalDate(value, columnInfo, timeZone).toLocalDate();
    }

    @Override
    LocalDate parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToLocalDateTime(value, columnInfo, timeZone).toLocalDate();
    }

    @Override
    LocalDate parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value).atZone(timeZone.toZoneId()).toLocalDate();
    }

    @Override
    LocalDate parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value);
        } catch (DateTimeParseException dtpe) {
            // not parseable as date
        }
        try {
            return parseAsLocalDateTime(value).toLocalDate();
        } catch (DateTimeParseException dtpe) {
            // not parseable as datetime
        }
        Instant i = parseAsInstant(value);
        return i.atZone(timeZone.toZoneId()).toLocalDate();
    }

}
