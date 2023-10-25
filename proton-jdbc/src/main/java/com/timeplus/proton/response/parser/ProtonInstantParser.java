package com.timeplus.proton.response.parser;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonInstantParser extends ProtonDateValueParser<Instant> {

    private static ProtonInstantParser instance;

    static ProtonInstantParser getInstance() {
        if (instance == null) {
            instance = new ProtonInstantParser();
        }
        return instance;
    }

    private ProtonInstantParser() {
        super(Instant.class);
    }

    @Override
    Instant parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsLocalDate(value).atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    @Override
    Instant parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToZonedDateTime(value, columnInfo, timeZone).toInstant();
    }

    @Override
    Instant parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return value > Integer.MAX_VALUE
            ? Instant.ofEpochMilli(value)
            : Instant.ofEpochSecond(value);
    }

    @Override
    Instant parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDate(value)
                .atStartOfDay(effectiveTimeZone(columnInfo, timeZone))
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        try {
            return parseAsOffsetDateTime(value)
                .toInstant();
        } catch (DateTimeParseException dtpe) {
            // better luck next time
        }
        return parseAsInstant(value);
    }
}
