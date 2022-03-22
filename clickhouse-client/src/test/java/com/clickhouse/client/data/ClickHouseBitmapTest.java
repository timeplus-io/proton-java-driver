package com.clickhouse.client.data;

import com.clickhouse.client.ClickHouseDataType;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClickHouseBitmapTest {
    @Test(groups = "unit")
    public void testEmptyBitmap32() {
        byte[] expectedBytes = new byte[] { 0, 0 };
        ClickHouseDataType[] types = new ClickHouseDataType[] { ClickHouseDataType.int8, ClickHouseDataType.uint8,
                ClickHouseDataType.int16, ClickHouseDataType.uint16, ClickHouseDataType.int32,
                ClickHouseDataType.uint32};
        for (ClickHouseDataType t : types) {
            Assert.assertTrue(ClickHouseBitmap.empty(t).isEmpty(), "Bitmap should be empty");
            Assert.assertEquals(ClickHouseBitmap.empty(t).toBytes(), expectedBytes);
        }

        Object[] bitmaps = new Object[] { RoaringBitmap.bitmapOf(), MutableRoaringBitmap.bitmapOf(),
                ImmutableRoaringBitmap.bitmapOf() };

        for (Object bm : bitmaps) {
            for (ClickHouseDataType t : types) {
                ClickHouseBitmap v = ClickHouseBitmap.wrap(bm, t);
                Assert.assertTrue(v.isEmpty(), "Bitmap should be empty");
                Assert.assertEquals(v.toBytes(), expectedBytes);
            }
        }
    }

    @Test(groups = "unit")
    public void testEmptyBitmap64() {
        byte[] expectedBytes = new byte[] { 0, 0 };

        Assert.assertTrue(ClickHouseBitmap.empty(ClickHouseDataType.int64).isEmpty(), "Bitmap should be empty");
        Assert.assertEquals(ClickHouseBitmap.empty(ClickHouseDataType.int64).toBytes(), expectedBytes);
        Assert.assertTrue(ClickHouseBitmap.empty(ClickHouseDataType.uint64).isEmpty(), "Bitmap should be empty");
        Assert.assertEquals(ClickHouseBitmap.empty(ClickHouseDataType.uint64).toBytes(), expectedBytes);

        ClickHouseDataType[] types = new ClickHouseDataType[] { ClickHouseDataType.int64, ClickHouseDataType.uint64};
        Object[] bitmaps = new Object[] { Roaring64Bitmap.bitmapOf(), Roaring64NavigableMap.bitmapOf() };

        for (Object bm : bitmaps) {
            for (ClickHouseDataType t : types) {
                ClickHouseBitmap v = ClickHouseBitmap.wrap(bm, t);
                Assert.assertTrue(v.isEmpty(), "Bitmap should be empty");
                Assert.assertEquals(v.toBytes(), expectedBytes);
            }
        }
    }
}
