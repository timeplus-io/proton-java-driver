package ru.yandex.clickhouse.util;

import static org.testng.Assert.assertEquals;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.testng.annotations.Test;

import ru.yandex.clickhouse.domain.ClickHouseDataType;

public class ClickHouseBitmapTest {
    @Test(groups = "unit")
    public void testEmptyRoaringBitmap() {
        byte[] expectedBytes = new byte[] { 0, 0 };

        ClickHouseDataType[] types = new ClickHouseDataType[] { ClickHouseDataType.int8, ClickHouseDataType.uint8,
                ClickHouseDataType.int16, ClickHouseDataType.uint16, ClickHouseDataType.int32,
                ClickHouseDataType.uint32 };
        Object[] bitmaps = new Object[] { RoaringBitmap.bitmapOf(), MutableRoaringBitmap.bitmapOf(),
                ImmutableRoaringBitmap.bitmapOf() };

        for (Object bm : bitmaps) {
            for (ClickHouseDataType t : types) {
                assertEquals(ClickHouseBitmap.wrap(bm, t).toBytes(), expectedBytes);
            }
        }
    }
}
