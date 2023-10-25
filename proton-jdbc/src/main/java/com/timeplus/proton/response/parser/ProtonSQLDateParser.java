package com.timeplus.proton.response.parser;

import java.sql.Date;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.TimeZone;

import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonSQLDateParser extends ProtonDateValueParser<Date> {

    private static ProtonSQLDateParser instance;

    static ProtonSQLDateParser getInstance() {
        if (instance == null) {
            instance = new ProtonSQLDateParser();
        }
        return instance;
    }

    private ProtonSQLDateParser() {
        super(Date.class);
    }

    @Override
    Date parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Date(dateToZonedDateTime(value, columnInfo, timeZone).truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
    }

    @Override
    Date parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        if (timeZone == null) {
            timeZone = TimeZone.getDefault();
        }
        return new Date(dateTimeToZonedDateTime(value, columnInfo, timeZone).truncatedTo(ChronoUnit.DAYS).toInstant().toEpochMilli());
    }

    @Override
    Date parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Date(parseAsInstant(value)
            .atZone(getResultTimeZone(timeZone))
            .truncatedTo(ChronoUnit.DAYS)
            .toInstant()
            .toEpochMilli());
    }

    @Override
    Date parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return new Date(parseAsInstant(value)
                .atZone(getResultTimeZone(timeZone))
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return new Date(parseAsOffsetDateTime(value)
                .toInstant()
                .atZone(getResultTimeZone(timeZone))
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        try {
            return new Date(parseAsLocalDateTime(value)
                  .atZone(getParsingTimeZone(columnInfo, timeZone))
                  .withZoneSameInstant(getResultTimeZone(timeZone))
                  .truncatedTo(ChronoUnit.DAYS)
                  .toInstant()
                  .toEpochMilli());
        } catch (DateTimeParseException dtpe) {
            // try next candidate
        }

        return new Date(LocalDateTime
            .of(
                parseAsLocalDate(value),
                LocalTime.MIDNIGHT)
            .atZone(getResultTimeZone(timeZone))
            .toInstant()
            .toEpochMilli());
    }

    private static ZoneId getParsingTimeZone(ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return columnInfo.getTimeZone() != null
            ? columnInfo.getTimeZone().toZoneId()
            : timeZone != null
                ? timeZone.toZoneId()
                : ZoneId.systemDefault();
    }

    private static ZoneId getResultTimeZone(TimeZone timeZone) {
        return timeZone != null
            ? timeZone.toZoneId()
            : ZoneId.systemDefault();
    }

}
