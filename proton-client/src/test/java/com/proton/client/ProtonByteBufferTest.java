package com.proton.client;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonByteBufferTest {
    @Test(groups = { "unit" })
    public void testEmptyArray() {
        Assert.assertEquals(ProtonByteBuffer.of(null), ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(null, -1, -1), ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(new byte[0]), ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(new byte[0], -1, -1), ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, 0, 0), ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, -1, 0), ProtonByteBuffer.newInstance());

        Assert.assertEquals(ProtonByteBuffer.of(new byte[] { 1, 2, 3 }).update(null),
                ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.of(new byte[] { 1, 2, 3 }).update(null, -1, -1),
                ProtonByteBuffer.newInstance());
    }

    @Test(groups = { "unit" })
    public void testInvalidValue() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, -1, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, 0, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, 3, 1));
    }

    @Test(groups = { "unit" })
    public void testNewInstance() {
        ProtonByteBuffer buf1 = ProtonByteBuffer.newInstance();
        Assert.assertEquals(buf1.array(), ProtonByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(buf1.position(), 0);
        Assert.assertEquals(buf1.length(), 0);
        Assert.assertEquals(buf1.limit(), 0);

        ProtonByteBuffer buf2 = ProtonByteBuffer.newInstance();
        Assert.assertEquals(buf1.array(), ProtonByteBuffer.EMPTY_BYTES);
        Assert.assertEquals(buf1.position(), 0);
        Assert.assertEquals(buf1.length(), 0);
        Assert.assertEquals(buf1.limit(), 0);

        Assert.assertFalse(buf1 == buf2, "Should be different instances");
        Assert.assertEquals(buf1, buf2);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        Assert.assertEquals(ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2).reset(),
                ProtonByteBuffer.newInstance());
        Assert.assertEquals(ProtonByteBuffer.newInstance().update(new byte[] { 1, 2, 3 }, 1, 2),
                ProtonByteBuffer.of(new byte[] { 1, 2, 3 }, 1, 2));
    }
}
