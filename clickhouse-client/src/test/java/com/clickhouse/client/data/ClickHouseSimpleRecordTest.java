package com.clickhouse.client.data;

import java.util.Arrays;
import java.util.Collections;

import com.clickhouse.client.ClickHouseColumn;
import com.clickhouse.client.ClickHouseValue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseSimpleRecordTest {
    @Test(groups = { "unit" })
    public void testNullInput() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseSimpleRecord.of(null, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseSimpleRecord.of(null, new ClickHouseValue[0]));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseSimpleRecord.of(Collections.emptyList(), null));

        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(null, null);
        Assert.assertNull(record.getColumns());
        Assert.assertNull(record.getValues());
    }

    @Test(groups = { "unit" })
    public void testMismatchedColumnsAndValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseSimpleRecord
                .of(Arrays.asList(ClickHouseColumn.of("a", "string")), new ClickHouseValue[0]));

        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(Arrays.asList(ClickHouseColumn.of("a", "string")),
                new ClickHouseValue[0]);
        Assert.assertEquals(record.getColumns(), Arrays.asList(ClickHouseColumn.of("a", "string")));
        Assert.assertEquals(record.getValues(), new ClickHouseValue[0]);
    }

    @Test(groups = { "unit" })
    public void testGetValueByIndex() {
        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(ClickHouseColumn.parse("a string, b uint32"),
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true) });
        Assert.assertEquals(record.getColumns(), ClickHouseColumn.parse("a string, b uint32"));
        Assert.assertEquals(record.getValues(),
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true) });

        Assert.assertEquals(record.getValue(0), ClickHouseStringValue.of("123"));
        Assert.assertEquals(record.getValue(1), ClickHouseLongValue.of(1L, true));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(-1));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(2));

        int index = 0;
        for (ClickHouseValue v : record) {
            if (index == 0) {
                Assert.assertEquals(v, ClickHouseStringValue.of("123"));
            } else {
                Assert.assertEquals(v, ClickHouseLongValue.of(1L, true));
            }
            index++;
        }
    }

    @Test(groups = { "unit" })
    public void testGetValueByName() {
        ClickHouseSimpleRecord record = new ClickHouseSimpleRecord(
               ClickHouseColumn.parse("`a One` string, `x木哈哈x` uint32, test nullable(string)"),
                new ClickHouseValue[] { ClickHouseStringValue.of("123"), ClickHouseLongValue.of(1L, true),
                        ClickHouseStringValue.ofNull() });
        Assert.assertEquals(record.getColumns(),
                ClickHouseColumn.parse("`a One` string, `x木哈哈x` uint32, test nullable(string)"));
        Assert.assertEquals(record.getValues(), new ClickHouseValue[] { ClickHouseStringValue.of("123"),
                ClickHouseLongValue.of(1L, true), ClickHouseStringValue.ofNull() });

        Assert.assertEquals(record.getValue("A one"), ClickHouseStringValue.of("123"));
        Assert.assertEquals(record.getValue("x木哈哈x"), ClickHouseLongValue.of(1L, true));
        Assert.assertEquals(record.getValue("TEST"), ClickHouseStringValue.ofNull());

        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue("non-exist"));
    }
}
