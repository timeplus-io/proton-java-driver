package com.timeplus.proton.response.parser;

import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonZonedDateTimeParser extends ProtonDateValueParser<ZonedDateTime> {

    private static ProtonZonedDateTimeParser instance;

    static ProtonZonedDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ProtonZonedDateTimeParser();
        }
        return instance;
    }

    private ProtonZonedDateTimeParser() {
        super(ZonedDateTime.class);
    }

    @Override
    ZonedDateTime parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateToZonedDateTime(value, columnInfo, timeZone);
    }

    @Override
    ZonedDateTime parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToZonedDateTime(value, columnInfo, timeZone);
    }

    @Override
    ZonedDateTime parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone));
    }

    @Override
    ZonedDateTime parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone));
        } catch(DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .toZonedDateTime();
        } catch (DateTimeParseException dtpe) {
            // try another way
        }

        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone));
    }

}
