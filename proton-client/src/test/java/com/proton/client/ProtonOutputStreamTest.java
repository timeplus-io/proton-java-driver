package com.proton.client;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonOutputStreamTest {
    @Test(groups = { "unit" })
    public void testWriteString() throws IOException {
        ByteArrayOutputStream inner = new ByteArrayOutputStream();
        ProtonOutputStream out = ProtonOutputStream.of(inner);
        out.writeAsciiString(null);
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[1]);
        out.writeAsciiString("");
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[2]);
        out.writeUnicodeString(null);
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[3]);
        out.writeUnicodeString("");
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[4]);

        inner = new ByteArrayOutputStream();
        out = ProtonOutputStream.of(inner);
        out.writeAsciiString("12");
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[] { 2, 0x31, 0x32 });

        inner = new ByteArrayOutputStream();
        out = ProtonOutputStream.of(inner);
        out.writeUnicodeString("壹贰");
        out.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[] { 6, -27, -93, -71, -24, -76, -80 });
    }

    @Test(groups = { "unit" })
    public void testNullOrClosedOutput() throws IOException {
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonOutputStream.of(null));
        ByteArrayOutputStream inner = new ByteArrayOutputStream();
        OutputStream out = new BufferedOutputStream(inner);
        ProtonOutputStream empty = ProtonOutputStream.of(out);
        Assert.assertEquals(inner.toByteArray(), new byte[0]);
        Assert.assertEquals(empty.isClosed(), false);
        empty.writeByte((byte) 1);
        empty.writeBytes(new byte[] { 1, 2, 3 }, 1, 2);
        empty.writeBytes(ByteBuffer.wrap(new byte[] { 4, 5, 6 }).asReadOnlyBuffer(), 0, 3);
        empty.flush();
        Assert.assertEquals(inner.toByteArray(), new byte[] { 1, 2, 3, 4, 5, 6 });
        out.close();
        Assert.assertEquals(empty.isClosed(), false);
        empty.close();
        Assert.assertEquals(empty.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty.flush());
        empty.close();
        Assert.assertEquals(empty.isClosed(), true);
        Assert.assertThrows(IOException.class, () -> empty.write(1));
    }
}
