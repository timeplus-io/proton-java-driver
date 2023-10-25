package com.timeplus.proton.util;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ProtonBlockChecksum {
    private final long first;
    private final long second;

    public ProtonBlockChecksum(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public static ProtonBlockChecksum fromBytes(byte[] checksum) {
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).put(checksum);
        ((Buffer) buffer).flip();
        return new ProtonBlockChecksum(buffer.getLong(), buffer.getLong());
    }

    public static ProtonBlockChecksum calculateForBlock(byte magic, int compressedSizeWithHeader, int uncompressedSize, byte[] data, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(compressedSizeWithHeader).order(ByteOrder.LITTLE_ENDIAN).put((byte)magic).putInt(compressedSizeWithHeader)
                .putInt(uncompressedSize).put(data, 0, length);
        ((Buffer) buffer).flip();
        return calculate(buffer.array());
    }

    public byte[] asBytes(){
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN).putLong(first).putLong(second);
        ((Buffer) buffer).flip();
        return buffer.array();
    }

    private static ProtonBlockChecksum calculate(byte[] data) {
        long[] sum = ProtonCityHash.cityHash128(data, 0, data.length);
        return new ProtonBlockChecksum(sum[0], sum[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProtonBlockChecksum that = (ProtonBlockChecksum) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = (int) (first ^ (first >>> 32));
        result = 31 * result + (int) (second ^ (second >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "{" + first + ", " + second + '}';
    }
}
