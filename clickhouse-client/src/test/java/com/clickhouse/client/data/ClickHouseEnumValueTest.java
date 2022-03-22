package com.clickhouse.client.data;

import org.testng.annotations.Test;
import com.clickhouse.client.BaseClickHouseValueTest;
import com.clickhouse.client.ClickHouseDataType;

public class ClickHouseEnumValueTest extends BaseClickHouseValueTest {
    @Test(groups = { "unit" })
    public void testCopy() {
        sameValue(ClickHouseEnumValue.ofNull(ClickHouseDataType.class),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.string),
                ClickHouseEnumValue.of(ClickHouseDataType.string),
                3, 9, Object.class, ClickHouseDataType.class, Object.class, Object.class);
        ClickHouseEnumValue v = ClickHouseEnumValue.of(ClickHouseDataType.array);
        sameValue(v, v.copy(), 3, 9, Object.class, ClickHouseDataType.class, Object.class, Object.class);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        sameValue(ClickHouseEnumValue.ofNull(ClickHouseDataType.class),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class).update(ClickHouseDataType.date32).set(true, 0), 3,
                9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.date32),
                ClickHouseEnumValue.ofNull(ClickHouseDataType.class).update(ClickHouseDataType.date32), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.date32),
                ClickHouseEnumValue.of(ClickHouseDataType.array).update(ClickHouseDataType.date32), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);

        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update(false), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update(new boolean[] { false }), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update('\0'), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update((byte) 0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update((short) 0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update(0), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update(0L), 3, 9, Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
        sameValue(ClickHouseEnumValue.of(ClickHouseDataType.interval_year),
                ClickHouseEnumValue.of(ClickHouseDataType.string).update("interval_year"), 3, 9,
                Object.class,
                ClickHouseDataType.class, Object.class, Object.class);
    }
}
