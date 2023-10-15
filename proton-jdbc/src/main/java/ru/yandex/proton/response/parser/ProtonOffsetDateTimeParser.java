package ru.yandex.proton.response.parser;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.proton.response.ProtonColumnInfo;

final class ProtonOffsetDateTimeParser extends ProtonDateValueParser<OffsetDateTime> {

    private static ProtonOffsetDateTimeParser instance;

    static ProtonOffsetDateTimeParser getInstance() {
        if (instance == null) {
            instance = new ProtonOffsetDateTimeParser();
        }
        return instance;
    }

    private ProtonOffsetDateTimeParser() {
        super(OffsetDateTime.class);
    }

    @Override
    OffsetDateTime parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateToZonedDateTime(value, columnInfo, timeZone).toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return dateTimeToZonedDateTime(value, columnInfo, timeZone).toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

    @Override
    OffsetDateTime parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return parseAsLocalDateTime(value)
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toOffsetDateTime();
        } catch (DateTimeParseException dtpe) {
            // try another way
        }
        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        } catch (DateTimeParseException dtpe) {
            // try another way
        }
        return parseAsInstant(value)
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toOffsetDateTime();
    }

}
