package com.proton.client.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import com.proton.client.ProtonDataType;

public abstract class ProtonBitmap {
    private static final int[] EMPTY_INT_ARRAY = new int[0];
    private static final long[] EMPTY_LONG_ARRAY = new long[0];
    private static final ProtonBitmap EMPTY_INT8_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.int8);
    private static final ProtonBitmap EMPTY_UINT8_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.uint8);
    private static final ProtonBitmap EMPTY_INT16_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.int16);
    private static final ProtonBitmap EMPTY_UINT16_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.uint16);
    private static final ProtonBitmap EMPTY_INT32_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.int32);
    private static final ProtonBitmap EMPTY_UINT32_BITMAP = wrap(ImmutableRoaringBitmap.bitmapOf(EMPTY_INT_ARRAY),
            ProtonDataType.uint32);

    static class ProtonRoaringBitmap extends ProtonBitmap {
        private final RoaringBitmap rb;

        protected ProtonRoaringBitmap(RoaringBitmap bitmap, ProtonDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public boolean isEmpty() {
            return rb.isEmpty();
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ProtonImmutableRoaringBitmap extends ProtonBitmap {
        private final ImmutableRoaringBitmap rb;

        protected ProtonImmutableRoaringBitmap(ImmutableRoaringBitmap rb, ProtonDataType innerType) {
            super(rb, innerType);

            this.rb = Objects.requireNonNull(rb);
        }

        @Override
        public boolean isEmpty() {
            return rb.isEmpty();
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ProtonMutableRoaringBitmap extends ProtonBitmap {
        private final MutableRoaringBitmap rb;

        protected ProtonMutableRoaringBitmap(MutableRoaringBitmap bitmap, ProtonDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public boolean isEmpty() {
            return rb.isEmpty();
        }

        @Override
        public int getCardinality() {
            return rb.getCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            rb.serialize(buffer);
        }

        @Override
        public int serializedSizeInBytes() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            return rb.toArray();
        }
    }

    static class ProtonRoaring64NavigableMap extends ProtonBitmap {
        private final Roaring64NavigableMap rb;

        protected ProtonRoaring64NavigableMap(Roaring64NavigableMap bitmap, ProtonDataType innerType) {
            super(bitmap, innerType);

            this.rb = Objects.requireNonNull(bitmap);
        }

        @Override
        public boolean isEmpty() {
            return rb.isEmpty();
        }

        @Override
        public int getCardinality() {
            return rb.getIntCardinality();
        }

        @Override
        public long getLongCardinality() {
            return rb.getLongCardinality();
        }

        @Override
        public void serialize(ByteBuffer buffer) {
            int size = serializedSizeInBytes();
            // TODO use custom data output so that we can handle large byte array
            try (ByteArrayOutputStream bas = new ByteArrayOutputStream(size)) {
                DataOutput out = new DataOutputStream(bas);
                try {
                    // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1105
                    rb.serialize(out);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to serialize given bitmap", e);
                }

                byte[] bytes = bas.toByteArray();
                for (int i = 4; i > 0; i--) {
                    buffer.put(bytes[i]);
                }
                buffer.putInt(0);
                buffer.put(bytes, 5, size - 5);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to serialize given bitmap", e);
            }
        }

        @Override
        public int serializedSizeInBytes() {
            return (int) rb.serializedSizeInBytes();
        }

        @Override
        public long serializedSizeInBytesAsLong() {
            return rb.serializedSizeInBytes();
        }

        @Override
        public int[] toIntArray() {
            long[] longs = toLongArray();
            int len = longs.length;
            int[] ints = new int[len];
            for (int i = 0; i < len; i++) {
                ints[i] = (int) longs[i];
            }
            return ints;
        }

        @Override
        public long[] toLongArray() {
            return rb.toArray();
        }
    }

    public static ProtonBitmap empty() {
        return empty(null);
    }

    public static ProtonBitmap empty(ProtonDataType type) {
        if (type == null) {
            type = ProtonDataType.uint32;
        }

        ProtonBitmap v;
        switch (type) {
        case int8:
            v = ProtonBitmap.EMPTY_INT8_BITMAP;
            break;
        case uint8:
            v = ProtonBitmap.EMPTY_UINT8_BITMAP;
            break;
        case int16:
            v = ProtonBitmap.EMPTY_INT16_BITMAP;
            break;
        case uint16:
            v = ProtonBitmap.EMPTY_UINT16_BITMAP;
            break;
        case int32:
            v = ProtonBitmap.EMPTY_INT32_BITMAP;
            break;
        case uint32:
            v = ProtonBitmap.EMPTY_UINT32_BITMAP;
            break;
        case int64:
        case uint64:
            v = wrap(Roaring64NavigableMap.bitmapOf(EMPTY_LONG_ARRAY), type);
            break;
        default:
            throw new IllegalArgumentException("Only native integer types are supported but we got: " + type.name());
        }
        return v;
    }

    public static ProtonBitmap wrap(byte... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            byte v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ProtonDataType.uint8 : ProtonDataType.int8);
    }

    public static ProtonBitmap wrap(short... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            short v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ProtonDataType.uint16 : ProtonDataType.int16);
    }

    public static ProtonBitmap wrap(int... values) {
        boolean isUnsigned = true;
        int len = values.length;
        int[] ints = new int[len];
        for (int i = 0; i < len; i++) {
            int v = values[i];
            ints[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(RoaringBitmap.bitmapOf(ints), isUnsigned ? ProtonDataType.uint32 : ProtonDataType.int32);
    }

    public static ProtonBitmap wrap(long... values) {
        boolean isUnsigned = true;
        int len = values.length;
        long[] longs = new long[len];
        for (int i = 0; i < len; i++) {
            long v = values[i];
            longs[i] = v;
            if (isUnsigned && v < 0) {
                isUnsigned = false;
            }
        }

        return wrap(Roaring64NavigableMap.bitmapOf(longs),
                isUnsigned ? ProtonDataType.uint64 : ProtonDataType.int64);
    }

    public static ProtonBitmap wrap(Object bitmap, ProtonDataType innerType) {
        final ProtonBitmap b;
        if (bitmap instanceof RoaringBitmap) {
            b = new ProtonRoaringBitmap((RoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof MutableRoaringBitmap) {
            b = new ProtonMutableRoaringBitmap((MutableRoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof ImmutableRoaringBitmap) {
            b = new ProtonImmutableRoaringBitmap((ImmutableRoaringBitmap) bitmap, innerType);
        } else if (bitmap instanceof Roaring64Bitmap) {
            b = new ProtonRoaring64NavigableMap(
                    Roaring64NavigableMap.bitmapOf(((Roaring64Bitmap) bitmap).toArray()), innerType);
        } else if (bitmap instanceof Roaring64NavigableMap) {
            b = new ProtonRoaring64NavigableMap((Roaring64NavigableMap) bitmap, innerType);
        } else {
            throw new IllegalArgumentException("Only RoaringBitmap is supported but got: " + bitmap);
        }

        return b;
    }

    public static ProtonBitmap deserialize(InputStream in, ProtonDataType innerType) throws IOException {
        return deserialize(in instanceof DataInputStream ? (DataInputStream) in : new DataInputStream(in), innerType);
    }

    public static ProtonBitmap deserialize(DataInputStream in, ProtonDataType innerType) throws IOException {
        final ProtonBitmap rb;

        int byteLen = byteLength(innerType);
        int flag = in.readUnsignedByte();
        if (flag == 0) {
            byte cardinality = (byte) in.readUnsignedByte();
            byte[] bytes = new byte[2 + byteLen * cardinality];
            bytes[0] = (byte) flag;
            bytes[1] = cardinality;
            in.read(bytes, 2, bytes.length - 2);

            rb = ProtonBitmap.deserialize(bytes, innerType);
        } else {
            int len = BinaryStreamUtils.readVarInt(in);
            byte[] bytes = new byte[len];

            if (byteLen <= 4) {
                in.readFully(bytes);
                RoaringBitmap b = new RoaringBitmap();
                b.deserialize(flip(newBuffer(len).put(bytes)));
                rb = ProtonBitmap.wrap(b, innerType);
            } else {
                // TODO implement a wrapper of DataInput to get rid of byte array here
                bytes[0] = (byte) 0; // always unsigned
                // read map size in big-endian byte order
                for (int i = 4; i > 0; i--) {
                    bytes[i] = in.readByte();
                }
                if (in.readByte() != 0 || in.readByte() != 0 || in.readByte() != 0 || in.readByte() != 0) {
                    throw new IllegalStateException(
                            "Not able to deserialize ProtonBitmap for too many bitmaps(>" + 0xFFFFFFFFL + ")!");
                }
                // read the rest
                in.readFully(bytes, 5, len - 5);
                Roaring64NavigableMap b = new Roaring64NavigableMap();
                b.deserialize(new DataInputStream(new ByteArrayInputStream(bytes)));
                rb = ProtonBitmap.wrap(b, innerType);
            }
        }

        return rb;
    }

    public static ProtonBitmap deserialize(byte[] bytes, ProtonDataType innerType) throws IOException {
        // https://github.com/Proton/Proton/blob/master/src/AggregateFunctions/AggregateFunctionGroupBitmapData.h#L100
        ProtonBitmap rb = ProtonBitmap.wrap();

        if (bytes == null || bytes.length == 0) {
            return rb;
        }

        int byteLen = byteLength(innerType);
        ByteBuffer buffer = newBuffer(bytes.length);
        buffer = (ByteBuffer) ((Buffer) buffer.put(bytes)).flip();

        if (buffer.get() == (byte) 0) { // small set
            int cardinality = buffer.get();
            if (byteLen == 1) {
                byte[] values = new byte[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.get();
                }
                rb = ProtonBitmap.wrap(values);
            } else if (byteLen == 2) {
                short[] values = new short[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getShort();
                }
                rb = ProtonBitmap.wrap(values);
            } else if (byteLen == 4) {
                int[] values = new int[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getInt();
                }
                rb = ProtonBitmap.wrap(values);
            } else {
                long[] values = new long[cardinality];
                for (int i = 0; i < cardinality; i++) {
                    values[i] = buffer.getLong();
                }
                rb = ProtonBitmap.wrap(values);
            }
        } else { // serialized bitmap
            int len = BinaryStreamUtils.readVarInt(buffer);
            if (buffer.remaining() < len) {
                throw new IllegalStateException(
                        "Need " + len + " bytes to deserialize ProtonBitmap but only got " + buffer.remaining());
            }
            if (byteLen <= 4) {
                RoaringBitmap b = new RoaringBitmap();
                b.deserialize(buffer);
                rb = ProtonBitmap.wrap(b, innerType);
            } else {
                // consume map size(long in little-endian byte order)
                byte[] bitmaps = new byte[4];
                buffer.get(bitmaps);
                if (buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0 || buffer.get() != 0) {
                    throw new IllegalStateException(
                            "Not able to deserialize ProtonBitmap for too many bitmaps(>" + 0xFFFFFFFFL + ")!");
                }
                // replace the last 5 bytes to flag(boolean for signed/unsigned) and map
                // size(integer)
                ((Buffer) buffer).position(buffer.position() - 5);
                // always unsigned due to limit of CRoaring
                buffer.put((byte) 0);
                // big-endian -> little-endian
                for (int i = 3; i >= 0; i--) {
                    buffer.put(bitmaps[i]);
                }

                ((Buffer) buffer).position(buffer.position() - 5);
                bitmaps = new byte[buffer.remaining()];
                buffer.get(bitmaps);
                Roaring64NavigableMap b = new Roaring64NavigableMap();
                b.deserialize(new DataInputStream(new ByteArrayInputStream(bitmaps)));
                rb = ProtonBitmap.wrap(b, innerType);
            }
        }

        return rb;
    }

    private static ByteBuffer newBuffer(int capacity) {
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        if (buffer.order() != ByteOrder.LITTLE_ENDIAN) {
            buffer = buffer.slice().order(ByteOrder.LITTLE_ENDIAN);
        }

        return buffer;
    }

    private static ByteBuffer flip(ByteBuffer buffer) {
        return (ByteBuffer) ((Buffer) buffer).flip();
    }

    private static int byteLength(ProtonDataType type) {
        int byteLen;
        switch (Objects.requireNonNull(type)) {
        case int8:
        case uint8:
        case int16:
        case uint16:
        case int32:
        case uint32:
        case int64:
        case uint64:
            byteLen = type.getByteLength();
            break;
        default:
            throw new IllegalArgumentException("Only native integer types are supported but we got: " + type.name());
        }

        return byteLen;
    }

    protected final ProtonDataType innerType;
    protected final int byteLen;
    protected final Object reference;

    protected ProtonBitmap(Object bitmap, ProtonDataType innerType) {
        this.innerType = innerType;
        this.byteLen = byteLength(innerType);
        this.reference = Objects.requireNonNull(bitmap);
    }

    public abstract boolean isEmpty();

    public abstract int getCardinality();

    public long getLongCardinality() {
        return getCardinality();
    }

    public abstract void serialize(ByteBuffer buffer);

    public abstract int serializedSizeInBytes();

    public long serializedSizeInBytesAsLong() {
        return serializedSizeInBytes();
    }

    public abstract int[] toIntArray();

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ProtonBitmap b = (ProtonBitmap) obj;
        return Objects.equals(innerType, b.innerType) && Objects.equals(byteLen, b.byteLen)
                && Objects.equals(reference, b.reference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(innerType, byteLen, reference);
    }

    public long[] toLongArray() {
        int[] ints = toIntArray();
        int len = ints.length;
        long[] longs = new long[len];
        for (int i = 0; i < len; i++) {
            longs[i] = ints[i];
        }
        return longs;
    }

    /**
     * Serialize the bitmap into a flipped ByteBuffer.
     *
     * @return flipped byte buffer
     */
    public ByteBuffer toByteBuffer() {
        ByteBuffer buf;

        int cardinality = getCardinality();
        if (cardinality <= 32) {
            buf = newBuffer(2 + byteLen * cardinality);
            buf.put((byte) 0);
            buf.put((byte) cardinality);
            if (byteLen == 1) {
                for (int v : toIntArray()) {
                    buf.put((byte) v);
                }
            } else if (byteLen == 2) {
                for (int v : toIntArray()) {
                    buf.putShort((short) v);
                }
            } else if (byteLen == 4) {
                for (int v : toIntArray()) {
                    buf.putInt(v);
                }
            } else { // 64
                for (long v : toLongArray()) {
                    buf.putLong(v);
                }
            }
        } else if (byteLen <= 4) {
            int size = serializedSizeInBytes();
            int varIntSize = BinaryStreamUtils.getVarIntSize(size);

            buf = newBuffer(1 + varIntSize + size);
            buf.put((byte) 1);
            BinaryStreamUtils.writeVarInt(buf, size);
            serialize(buf);
        } else { // 64
            // 1) deduct one to exclude the leading byte - boolean flag, see below:
            // https://github.com/RoaringBitmap/RoaringBitmap/blob/0.9.9/RoaringBitmap/src/main/java/org/roaringbitmap/longlong/Roaring64NavigableMap.java#L1107
            // 2) add 4 bytes because CRoaring uses long to store count of 32-bit bitmaps,
            // while Java uses int - see
            // https://github.com/RoaringBitmap/CRoaring/blob/v0.2.66/cpp/roaring64map.hh#L597
            long size = serializedSizeInBytesAsLong() - 1 + 4;
            int varIntSize = BinaryStreamUtils.getVarLongSize(size);
            // TODO add serialize(DataOutput) to handle more
            int intSize = (int) size;
            buf = newBuffer(1 + varIntSize + intSize);
            buf.put((byte) 1);
            BinaryStreamUtils.writeVarInt(buf, intSize);
            serialize(buf);
        }

        return (ByteBuffer) ((Buffer) buf).flip();
    }

    public byte[] toBytes() {
        ByteBuffer buffer = toByteBuffer();
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
    }

    public String toBitmapBuildExpression() {
        StringBuilder sb = new StringBuilder();

        if (byteLen <= 4) {
            for (int v : toIntArray()) {
                sb.append(',').append("to").append(innerType.name()).append('(').append(v).append(')');
            }
        } else {
            for (long v : toLongArray()) {
                sb.append(',').append("to").append(innerType.name()).append('(').append(v).append(')');
            }
        }

        if (sb.length() > 0) {
            sb.deleteCharAt(0).insert(0, '[').append(']');
        } else {
            sb.append("cast([] as Array(").append(innerType.name()).append(')').append(')');
        }

        return sb.insert(0, "bitmapBuild(").append(')').toString();
    }

    public Object unwrap() {
        return this.reference;
    }
}
