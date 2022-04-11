package ru.yandex.clickhouse.domain;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.DataProvider;

public class ClickHouseDataTypeTestDataProvider {

    private static ClickHouseDataTypeTestData[] testData;

    @DataProvider(name = "clickHouseDataTypeStringsProvider")
    public static Object[][] provideSimpleDataTypes() {
        Object[][] myData = new Object[getTestData().length][2];
        for (int i = 0; i < getTestData().length; i++) {
            myData[i][0] = getTestData()[i].simpleTypeName;
            myData[i][1] = getTestData()[i].clickHouseDataType;
        }
        return myData;
    }

    public static List<ClickHouseDataTypeTestData> provideDataTypes() {
        List<ClickHouseDataTypeTestData> filtered = new ArrayList<>();
        for (int i = 0; i < getTestData().length; i++) {
            ClickHouseDataTypeTestData d = getTestData()[i];
            if (d.isCheckValue()) {
                filtered.add(d);
            }
        }
        return filtered;
    }

    private static ClickHouseDataTypeTestData[] getTestData() {
        if (testData == null) {
            testData = initTestData();
        }
        return testData;
    }

    private static ClickHouseDataTypeTestData[] initTestData() {
        return new ClickHouseDataTypeTestData[] {
            create("interval_year", ClickHouseDataType.interval_year, "interval_year", "42", true, true),
            create("interval_quarter", ClickHouseDataType.interval_quarter, "interval_quarter", "42", true, true),
            create("interval_day", ClickHouseDataType.interval_day, "interval_day", "42", true, true),
            create("interval_week", ClickHouseDataType.interval_week, "interval_week", "42", true, true),
            create("interval_hour", ClickHouseDataType.interval_hour, "interval_hour", "42", true, true),
            create("interval_minute", ClickHouseDataType.interval_minute, "interval_minute", "42", true, true),
            create("nested", ClickHouseDataType.nested),
            create("interval_month", ClickHouseDataType.interval_month, "interval_month", "42", true, true),
            create("tuple", ClickHouseDataType.tuple, "tuple(String, UInt32)", "('foo', 42)", true, true),
            create("aggregate_function", ClickHouseDataType.aggregate_function),
            create("fixed_string", ClickHouseDataType.fixed_string, "fixed_string(6)", "FOOBAR", true, true),
            create("interval_second", ClickHouseDataType.interval_second, "interval_second", "42", true, true),
            create("uint64", ClickHouseDataType.uint64, "uint64", "42", true, true),
            create("enum8", ClickHouseDataType.enum8, "enum8(1 = 'foo', 2 = 'bar')", "foo", true, true),
            create("int32", ClickHouseDataType.int32, "int32", "-23", true, true),
            create("int16", ClickHouseDataType.int16, "int16", "-23", true, true),
            create("int8", ClickHouseDataType.int8, "int8", "-42", true, true),
            create("date", ClickHouseDataType.date, "date", "2019-05-02", true, true),
            create("uint32", ClickHouseDataType.uint32, "uint32", "42", true, true),
            create("uint8", ClickHouseDataType.uint8, "uint8", "23", true, true),
            create("enum16", ClickHouseDataType.enum16, "enum16(1 = 'foo', 2 = 'bar')", "foo", true, false),
            create("datetime", ClickHouseDataType.datetime, "datetime", "2019-05-02 13:37:00", true, true),
            create("uint16", ClickHouseDataType.uint16, "uint16", "42", true, true),
            create("nothing", ClickHouseDataType.nothing),
            create("array", ClickHouseDataType.array),
            create("int64", ClickHouseDataType.int64, "int64", "-42", true, true),
            create("float32", ClickHouseDataType.float32, "float32", "0.42", true, false),
            create("float64", ClickHouseDataType.float64, "float64", "-0.23", true, false),
            create("decimal32", ClickHouseDataType.decimal32, "decimal32(4)", "0.4242", true, false),
            create("decimal64", ClickHouseDataType.decimal64, "decimal64(4)", "1337.23", true, false),
            create("decimal128", ClickHouseDataType.decimal128, "decimal128(4)", "1337.23", true, false),
            create("uuid", ClickHouseDataType.uuid, "uuid", "61f0c404-5cb3-11e7-907b-a6006ad3dba0", true, false),
            create("string", ClickHouseDataType.string, "string", "foo", true, true),
            create("decimal", ClickHouseDataType.decimal, "decimal(12,3)", "23.420", true, true),
            create("LONGBLOB", ClickHouseDataType.string),
            create("MEDIUMBLOB", ClickHouseDataType.string),
            create("TINYBLOB", ClickHouseDataType.string),
            create("BIGINT", ClickHouseDataType.int64),
            create("SMALLINT", ClickHouseDataType.int16),
            create("TIMESTAMP", ClickHouseDataType.datetime),
            create("INTEGER", ClickHouseDataType.int32),
            create("INT", ClickHouseDataType.int32),
            create("DOUBLE", ClickHouseDataType.float64),
            create("MEDIUMTEXT", ClickHouseDataType.string),
            create("TINYINT", ClickHouseDataType.int8),
            create("DEC", ClickHouseDataType.decimal),
            create("BINARY", ClickHouseDataType.fixed_string),
            create("REAL", ClickHouseDataType.float32),
            create("CHAR", ClickHouseDataType.string),
            create("VARCHAR", ClickHouseDataType.string),
            create("TEXT", ClickHouseDataType.string),
            create("TINYTEXT", ClickHouseDataType.string),
            create("LONGTEXT", ClickHouseDataType.string),
            create("BLOB", ClickHouseDataType.string),
            create("FANTASY", ClickHouseDataType.unknown, "Fantasy", "[42, 23]", true, true)
        };
    }

    private static ClickHouseDataTypeTestData create(String simpleTypeName,
        ClickHouseDataType clickHouseDataType, String typeName,
        String testValue, boolean nullableCandidate,
        boolean lowCardinalityCandidate)
    {
        return new ClickHouseDataTypeTestData(simpleTypeName, clickHouseDataType,
            typeName, testValue, nullableCandidate, lowCardinalityCandidate);
    }

    private static ClickHouseDataTypeTestData create(String simpleTypeName,
        ClickHouseDataType clickHouseDataType)
    {
        return new ClickHouseDataTypeTestData(simpleTypeName, clickHouseDataType,
            null, null, false, false);
    }

    public static final class ClickHouseDataTypeTestData {

        private final String simpleTypeName;
        private final ClickHouseDataType clickHouseDataType;
        private final String typeName;
        private final String testValue;
        private final boolean nullableCandidate;
        private final boolean lowCardinalityCandidate;

        ClickHouseDataTypeTestData(String simpleTypeName,
            ClickHouseDataType clickHouseDataType, String typeName,
            String testValue, boolean nullableCandidate,
            boolean lowCardinalityCandidate)
        {
            this.simpleTypeName = simpleTypeName;
            this.clickHouseDataType = clickHouseDataType;
            this.typeName = typeName;
            this.testValue = testValue;
            this.nullableCandidate = nullableCandidate;
            this.lowCardinalityCandidate = lowCardinalityCandidate;
        }

        private boolean isCheckValue() {
            return typeName != null;
        }

        public String getTypeName() {
            return typeName;
        }

        public boolean isNullableCandidate() {
            return nullableCandidate;
        }

        public boolean isLowCardinalityCandidate() {
            return lowCardinalityCandidate;
        }

        public String getTestValue() {
            return testValue;
        }

    }

}
