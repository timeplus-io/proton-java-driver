package com.timeplus.proton.response.parser;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonLocalDateTimeParser extends ProtonDateValueParser<LocalDateTime> {

    private static ProtonLocalDateTimeParser instance;

    static ProtonLocalDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ProtonLocalDateTimeParser();
        }
        return instance;
    }

    private ProtonLocalDateTimeParser() {
        super(LocalDateTime.class);
    }

    @Override
    LocalDateTime parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value).atStartOfDay();
    }

    @Override
    LocalDateTime parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToLocalDateTime(value, columnInfo, timeZone);
    }

    @Override
    LocalDateTime parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(timeZone.toZoneId())
            .toLocalDateTime();
    }

    @Override
    LocalDateTime parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value).atStartOfDay();
        } catch (DateTimeParseException dtpe) {
            // not parseable as date
        }
        try {
            return parseAsLocalDateTime(value);
        } catch (DateTimeParseException dtpe) {
            // not parseable as datetime
        }
        Instant i = parseAsInstant(value);
        return i.atZone(timeZone.toZoneId()).toLocalDateTime();
    }

}
