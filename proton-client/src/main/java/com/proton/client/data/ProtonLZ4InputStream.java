package com.proton.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.proton.client.ProtonByteBuffer;
import com.proton.client.ProtonChecker;
import com.proton.client.ProtonInputStream;
import com.proton.client.ProtonOutputStream;
import com.proton.client.ProtonUtils;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * Reader from proton in lz4.
 */
public class ProtonLZ4InputStream extends ProtonInputStream {
    private static final LZ4Factory factory = LZ4Factory.fastestInstance();

    static final byte MAGIC = (byte) 0x82;
    static final int HEADER_LENGTH = 25;

    private final LZ4FastDecompressor decompressor;
    private final InputStream stream;
    private final byte[] header;

    private byte[] currentBlock;
    private int position;

    private boolean checkNext() throws IOException {
        if (!closed && position >= currentBlock.length) {
            currentBlock = readNextBlock();
        }
        return currentBlock.length > 0;
    }

    // every block is:
    private byte[] readNextBlock() throws IOException {
        position = 0;

        // checksum(16 bytes) + 1 magic byte + header(8 bytes)
        if (!readFully(header, 0, HEADER_LENGTH)) {
            return ProtonByteBuffer.EMPTY_BYTES;
        } else if (header[16] != MAGIC) {
            // 1 byte - 0x82 (shows this is LZ4)
            throw new IOException(
                    ProtonUtils.format("Magic is not correct - expect [%d] but got [%d]", MAGIC, header[16]));
        }

        // 4 bytes - size of the compressed data including 9 bytes of the header
        int compressedSizeWithHeader = BinaryStreamUtils.toInt32(header, 17);
        // 4 bytes - size of uncompressed data
        int uncompressedSize = BinaryStreamUtils.toInt32(header, 21);
        int offset = 9;
        byte[] block = new byte[compressedSizeWithHeader];
        block[0] = header[16];
        BinaryStreamUtils.setInt32(block, 1, compressedSizeWithHeader);
        BinaryStreamUtils.setInt32(block, 5, uncompressedSize);
        // compressed data: compressed_size - 9 bytes
        if (!readFully(block, offset, compressedSizeWithHeader - offset)) {
            throw new EOFException();
        }

        long[] real = ProtonCityHash.cityHash128(block, 0, block.length);
        if (real[0] != BinaryStreamUtils.toInt64(header, 0) || real[1] != BinaryStreamUtils.toInt64(header, 8)) {
            throw new IllegalArgumentException("Checksum doesn't match: corrupted data.");
        }

        byte[] decompressed = new byte[uncompressedSize];
        decompressor.decompress(block, offset, decompressed, 0, uncompressedSize);
        return decompressed;
    }

    private boolean readFully(byte[] b, int off, int len) throws IOException {
        int n = 0;
        while (n < len) {
            int count = stream.read(b, off + n, len - n);
            if (count < 0) {
                if (n == 0) {
                    return false;
                }
                throw new EOFException();
            }
            n += count;
        }

        return true;
    }

    public ProtonLZ4InputStream(InputStream stream) {
        this(stream, null);
    }

    public ProtonLZ4InputStream(InputStream stream, Runnable afterClose) {
        super(afterClose);

        this.decompressor = factory.fastDecompressor();
        this.stream = ProtonChecker.nonNull(stream, "InputStream");
        this.header = new byte[HEADER_LENGTH];

        this.currentBlock = ProtonByteBuffer.EMPTY_BYTES;
        this.position = 0;
        this.closed = false;
    }

    @Override
    public int peek() throws IOException {
        return checkNext() ? 0xFF & currentBlock[position] : -1;
    }

    @Override
    public long pipe(ProtonOutputStream output) throws IOException {
        long count = 0L;
        if (output == null || output.isClosed()) {
            return count;
        }

        int remain = currentBlock.length - position;
        if (remain > 0) {
            output.write(currentBlock, position, remain);
            position = currentBlock.length;
            count += remain;
        }

        while (checkNext()) {
            output.write(currentBlock);
            count += currentBlock.length;
        }

        return count;
    }

    @Override
    public byte readByte() throws IOException {
        if (!checkNext()) {
            closeQuietly();
            throw new EOFException();
        }

        return currentBlock[position++];
    }

    @Override
    public int available() throws IOException {
        if (closed) {
            return 0;
        }

        int estimated = currentBlock.length - position;
        if (estimated == 0 && checkNext()) {
            estimated = currentBlock.length - position;
        }
        return estimated;
    }

    @Override
    public int read() throws IOException {
        return checkNext() ? 0xFF & currentBlock[position++] : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (!checkNext()) {
            return -1;
        }

        int copied = 0;
        while (copied != len) {
            int toCopy = Math.min(currentBlock.length - position, len - copied);
            System.arraycopy(currentBlock, position, b, off, toCopy);
            position += toCopy;
            off += toCopy;
            copied += toCopy;

            if (!checkNext()) {
                break;
            }
        }

        return copied;
    }

    @Override
    public ProtonByteBuffer read(int len) throws IOException {
        if (len <= 0) {
            byteBuffer.reset();
        } else {
            if (!checkNext()) {
                throw new EOFException();
            }

            int newLimit = position + len;
            if (currentBlock.length >= newLimit) {
                byteBuffer.update(currentBlock, position, len);
                position = newLimit;
            } else {
                byteBuffer.update(readBytes(len));
            }
        }
        return byteBuffer;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            try {
                stream.close();
            } finally {
                super.close();
            }
        }
    }
}
