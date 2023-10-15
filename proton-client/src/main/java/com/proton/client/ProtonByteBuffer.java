package com.proton.client;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Lite version of {@link java.nio.ByteBuffer}.
 */
public class ProtonByteBuffer implements Serializable {
    private static final long serialVersionUID = -8178041799873465082L;

    /**
     * Empty byte array.
     */
    public static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * Empty and read-only byte buffer.
     */
    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(EMPTY_BYTES).asReadOnlyBuffer();

    /**
     * Creates an empty byte buffer.
     *
     * @return empty byte buffer
     */
    public static ProtonByteBuffer newInstance() {
        return new ProtonByteBuffer(EMPTY_BYTES, 0, 0);
    }

    /**
     * Wraps given byte array as byte buffer.
     *
     * @param bytes byte array
     * @return byte buffer
     */
    public static ProtonByteBuffer of(byte[] bytes) {
        return bytes == null || bytes.length == 0 ? newInstance() : new ProtonByteBuffer(bytes, 0, bytes.length);
    }

    /**
     * Wraps given byte array as byte buffer.
     *
     * @param bytes  byte array
     * @param offset offset
     * @param length length
     * @return byte buffer
     */
    public static ProtonByteBuffer of(byte[] bytes, int offset, int length) {
        if (bytes == null || bytes.length == 0 || length == 0) {
            return newInstance();
        } else {
            validate(bytes, offset, length);
        }

        return new ProtonByteBuffer(bytes, offset, length);
    }

    static void validate(byte[] bytes, int offset, int length) {
        int len = ProtonChecker.nonNull(bytes, "Byte array").length;
        if (ProtonChecker.between(offset, "Offset", 0, len)
                + ProtonChecker.between(length, "Length", 0, len) > len) {
            throw new IllegalArgumentException(
                    ProtonUtils.format("Offset(%d) plus length(%d) should not greater than %d", offset, length,
                            len));
        }
    }

    protected byte[] array;
    protected int position;
    protected int length;

    protected ProtonByteBuffer(byte[] bytes, int offset, int length) {
        this.array = bytes;
        this.position = offset;
        this.length = length;
    }

    public boolean isEmpty() {
        return length < 1;
    }

    public ProtonByteBuffer reset() {
        array = EMPTY_BYTES;
        position = 0;
        length = 0;
        return this;
    }

    public ProtonByteBuffer update(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            reset();
        } else {
            array = bytes;
            position = 0;
            length = bytes.length;
        }

        return this;
    }

    public ProtonByteBuffer update(byte[] bytes, int offset, int length) {
        if (bytes == null || bytes.length == 0 || length == 0) {
            return reset();
        } else {
            validate(bytes, offset, length);
        }

        this.array = bytes;
        this.position = offset;
        this.length = length;
        return this;
    }

    public byte[] array() {
        return array;
    }

    public int position() {
        return position;
    }

    public int length() {
        return length;
    }

    public int limit() {
        return position + length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + Arrays.hashCode(array);
        result = prime * result + position;
        result = prime * result + length;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ProtonByteBuffer other = (ProtonByteBuffer) obj;
        return Arrays.equals(array, other.array) && position == other.position && length == other.length;
    }

    @Override
    public String toString() {
        return new StringBuilder().append(getClass().getSimpleName()).append("array=").append(array)
                .append(", position=").append(position).append(", length=").append(length).append(')').toString();
    }
}
