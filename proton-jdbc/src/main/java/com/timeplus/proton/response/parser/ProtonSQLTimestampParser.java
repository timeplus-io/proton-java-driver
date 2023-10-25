package com.timeplus.proton.response.parser;

import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.timeplus.proton.response.ProtonColumnInfo;

final class ProtonSQLTimestampParser extends ProtonDateValueParser<Timestamp> {

    private static final DateTimeFormatter DATE_TIME_FORMATTER_TZ =
        DateTimeFormatter.ofPattern("yyyy-MM-dd['T'][ ]HH:mm:ss[.SSS][XXX]");

    private static ProtonSQLTimestampParser instance;

    static ProtonSQLTimestampParser getInstance() {
        if (instance == null) {
            instance = new ProtonSQLTimestampParser();
        }
        return instance;
    }

    private ProtonSQLTimestampParser() {
        super(Timestamp.class);
    }

    @Override
    Timestamp parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return Timestamp.from(dateToZonedDateTime(value, columnInfo, timeZone).toInstant());
    }

    @Override
    Timestamp parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return Timestamp.from(dateTimeToZonedDateTime(value, columnInfo, timeZone).toInstant());
    }

    @Override
    Timestamp parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return Timestamp.from(parseAsInstant(value));
    }

    @Override
    Timestamp parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return Timestamp.from(parseAsInstant(value));
        } catch (DateTimeParseException dtpe) {
            // try next pattern
        }

        try {
            return Timestamp.from(parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant());
        } catch (DateTimeParseException dtpe) {
            // not parseable as datetime
        }

        try {
            return Timestamp.from(
                OffsetDateTime.parse(
                    value, DATE_TIME_FORMATTER_TZ)
                .toInstant());
        } catch (DateTimeParseException dtpe) {
            // too bad, let's try another pattern
        }

        return Timestamp.from(
            OffsetDateTime.parse(
                value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            .toInstant());
    }

}
