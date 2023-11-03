package com.timeplus.proton.client.data;

import java.util.Arrays;
import java.util.Collections;

import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonValue;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonSimpleRecordTest {
    @Test(groups = { "unit" })
    public void testNullInput() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonSimpleRecord.of(null, null));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonSimpleRecord.of(null, new ProtonValue[0]));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonSimpleRecord.of(Collections.emptyList(), null));

        ProtonSimpleRecord record = new ProtonSimpleRecord(null, null);
        Assert.assertNull(record.getColumns());
        Assert.assertNull(record.getValues());
    }

    @Test(groups = { "unit" })
    public void testMismatchedColumnsAndValues() {
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonSimpleRecord
                .of(Arrays.asList(ProtonColumn.of("a", "string")), new ProtonValue[0]));

        ProtonSimpleRecord record = new ProtonSimpleRecord(Arrays.asList(ProtonColumn.of("a", "string")),
                new ProtonValue[0]);
        Assert.assertEquals(record.getColumns(), Arrays.asList(ProtonColumn.of("a", "string")));
        Assert.assertEquals(record.getValues(), new ProtonValue[0]);
    }

    @Test(groups = { "unit" })
    public void testGetValueByIndex() {
        ProtonSimpleRecord record = new ProtonSimpleRecord(ProtonColumn.parse("a string, b uint32"),
                new ProtonValue[] { ProtonStringValue.of("123"), ProtonLongValue.of(1L, true) });
        Assert.assertEquals(record.getColumns(), ProtonColumn.parse("a string, b uint32"));
        Assert.assertEquals(record.getValues(),
                new ProtonValue[] { ProtonStringValue.of("123"), ProtonLongValue.of(1L, true) });

        Assert.assertEquals(record.getValue(0), ProtonStringValue.of("123"));
        Assert.assertEquals(record.getValue(1), ProtonLongValue.of(1L, true));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(-1));
        Assert.assertThrows(ArrayIndexOutOfBoundsException.class, () -> record.getValue(2));

        int index = 0;
        for (ProtonValue v : record) {
            if (index == 0) {
                Assert.assertEquals(v, ProtonStringValue.of("123"));
            } else {
                Assert.assertEquals(v, ProtonLongValue.of(1L, true));
            }
            index++;
        }
    }

    @Test(groups = { "unit" })
    public void testGetValueByName() {
        ProtonSimpleRecord record = new ProtonSimpleRecord(
               ProtonColumn.parse("`a One` string, `x木哈哈x` uint32, test nullable(string)"),
                new ProtonValue[] { ProtonStringValue.of("123"), ProtonLongValue.of(1L, true),
                        ProtonStringValue.ofNull() });
        Assert.assertEquals(record.getColumns(),
                ProtonColumn.parse("`a One` string, `x木哈哈x` uint32, test nullable(string)"));
        Assert.assertEquals(record.getValues(), new ProtonValue[] { ProtonStringValue.of("123"),
                ProtonLongValue.of(1L, true), ProtonStringValue.ofNull() });

        Assert.assertEquals(record.getValue("A one"), ProtonStringValue.of("123"));
        Assert.assertEquals(record.getValue("x木哈哈x"), ProtonLongValue.of(1L, true));
        Assert.assertEquals(record.getValue("TEST"), ProtonStringValue.ofNull());

        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue(null));
        Assert.assertThrows(IllegalArgumentException.class, () -> record.getValue("non-exist"));
    }
}
