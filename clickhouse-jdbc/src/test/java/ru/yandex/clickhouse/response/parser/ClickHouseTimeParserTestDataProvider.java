package ru.yandex.clickhouse.response.parser;

import org.testng.annotations.DataProvider;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

final class ClickHouseTimeParserTestDataProvider {

    static final String OTHER_DATA_TYPES = "otherDataTypes";

    @DataProvider(name = OTHER_DATA_TYPES)
    static Object[][] provideNumberAndSimilarClickHouseDataTypes() {
        return new ClickHouseDataType[][] {
            {ClickHouseDataType.int32},
            {ClickHouseDataType.int64},
            {ClickHouseDataType.uint32},
            {ClickHouseDataType.uint64},
            {ClickHouseDataType.string},
            {ClickHouseDataType.unknown}
        };
    }

}
