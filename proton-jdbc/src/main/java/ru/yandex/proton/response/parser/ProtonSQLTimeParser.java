package ru.yandex.proton.response.parser;

import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import ru.yandex.proton.response.ProtonColumnInfo;

final class ProtonSQLTimeParser extends ProtonDateValueParser<Time> {
    private static ProtonSQLTimeParser instance;

    static ProtonSQLTimeParser getInstance() {
        if (instance == null) {
            instance = new ProtonSQLTimeParser();
        }
        return instance;
    }

    private ProtonSQLTimeParser() {
        super(Time.class);
    }

    @Override
    Time parseDate(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalizeTime(columnInfo, dateToZonedDateTime(value, columnInfo, timeZone).toInstant().toEpochMilli()));
    }

    @Override
    Time parseDateTime(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalizeTime(columnInfo, dateTimeToZonedDateTime(value, columnInfo, timeZone).toInstant().toEpochMilli()));
    }

    @Override
    Time parseNumber(long value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        return new Time(normalizeTime(columnInfo,
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

    @Override
    Time parseOther(String value, ProtonColumnInfo columnInfo,
        TimeZone timeZone)
    {
        try {
            return new Time(normalizeTime(columnInfo,
                LocalDateTime.of(
                    LocalDate.ofEpochDay(0),
                    LocalTime.parse(value, DateTimeFormatter.ISO_LOCAL_TIME))
                .atZone(effectiveTimeZone(columnInfo, timeZone))
                .toInstant()
                .toEpochMilli()));
        } catch (DateTimeParseException dtpe) {
            // try next pattern candidate
        }

        return new Time(normalizeTime(columnInfo,
            LocalDateTime.of(
                LocalDate.ofEpochDay(0),
                parseAsLocalTime(value))
            .atZone(effectiveTimeZone(columnInfo, timeZone))
            .toInstant()
            .toEpochMilli()));
    }

}
