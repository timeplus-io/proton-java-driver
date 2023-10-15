package com.proton.client.data;

import com.proton.client.ProtonDataType;

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonBitmapTest {
    @Test(groups = "unit")
    public void testEmptyBitmap32() {
        byte[] expectedBytes = new byte[] { 0, 0 };
        ProtonDataType[] types = new ProtonDataType[] { ProtonDataType.int8, ProtonDataType.uint8,
                ProtonDataType.int16, ProtonDataType.uint16, ProtonDataType.int32,
                ProtonDataType.uint32};
        for (ProtonDataType t : types) {
            Assert.assertTrue(ProtonBitmap.empty(t).isEmpty(), "Bitmap should be empty");
            Assert.assertEquals(ProtonBitmap.empty(t).toBytes(), expectedBytes);
        }

        Object[] bitmaps = new Object[] { RoaringBitmap.bitmapOf(), MutableRoaringBitmap.bitmapOf(),
                ImmutableRoaringBitmap.bitmapOf() };

        for (Object bm : bitmaps) {
            for (ProtonDataType t : types) {
                ProtonBitmap v = ProtonBitmap.wrap(bm, t);
                Assert.assertTrue(v.isEmpty(), "Bitmap should be empty");
                Assert.assertEquals(v.toBytes(), expectedBytes);
            }
        }
    }

    @Test(groups = "unit")
    public void testEmptyBitmap64() {
        byte[] expectedBytes = new byte[] { 0, 0 };

        Assert.assertTrue(ProtonBitmap.empty(ProtonDataType.int64).isEmpty(), "Bitmap should be empty");
        Assert.assertEquals(ProtonBitmap.empty(ProtonDataType.int64).toBytes(), expectedBytes);
        Assert.assertTrue(ProtonBitmap.empty(ProtonDataType.uint64).isEmpty(), "Bitmap should be empty");
        Assert.assertEquals(ProtonBitmap.empty(ProtonDataType.uint64).toBytes(), expectedBytes);

        ProtonDataType[] types = new ProtonDataType[] { ProtonDataType.int64, ProtonDataType.uint64};
        Object[] bitmaps = new Object[] { Roaring64Bitmap.bitmapOf(), Roaring64NavigableMap.bitmapOf() };

        for (Object bm : bitmaps) {
            for (ProtonDataType t : types) {
                ProtonBitmap v = ProtonBitmap.wrap(bm, t);
                Assert.assertTrue(v.isEmpty(), "Bitmap should be empty");
                Assert.assertEquals(v.toBytes(), expectedBytes);
            }
        }
    }
}
