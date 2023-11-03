package com.timeplus.proton.client.data;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;
import java.util.function.Supplier;

import com.timeplus.proton.client.ProtonAggregateFunction;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonDataProcessor;
import com.timeplus.proton.client.ProtonDataType;
import com.timeplus.proton.client.ProtonDeserializer;
import com.timeplus.proton.client.ProtonFormat;
import com.timeplus.proton.client.ProtonInputStream;
import com.timeplus.proton.client.ProtonRecord;
import com.timeplus.proton.client.ProtonSerializer;
import com.timeplus.proton.client.ProtonUtils;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.ProtonValues;

/**
 * Data processor for handling {@link ProtonFormat#RowBinary} and
 * {@link ProtonFormat#RowBinaryWithNamesAndTypes} two formats.
 */
public class ProtonRowBinaryProcessor extends ProtonDataProcessor {
    public static class MappedFunctions {
        private static final MappedFunctions instance = new MappedFunctions();

        private void writeArray(ProtonValue value, ProtonConfig config, ProtonColumn column,
                                OutputStream output) throws IOException {
            ProtonColumn nestedColumn = column.getNestedColumns().get(0);
            ProtonColumn baseColumn = column.getArrayBaseColumn();
            int level = column.getArrayNestedLevel();
            Class<?> javaClass = baseColumn.getDataType().getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = value.asArray();
                ProtonValue v = ProtonValues.newValue(config, nestedColumn);
                int length = array.length;
                BinaryStreamUtils.writeVarInt(output, length);
                for (int i = 0; i < length; i++) {
                    serialize(v.update(array[i]), config, nestedColumn, output);
                }
            } else {
                ProtonValue v = ProtonValues.newValue(config, baseColumn);
                if (byte.class == javaClass) {
                    byte[] array = (byte[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (short.class == javaClass) {
                    short[] array = (short[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (int.class == javaClass) {
                    int[] array = (int[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (long.class == javaClass) {
                    long[] array = (long[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (float.class == javaClass) {
                    float[] array = (float[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else if (double.class == javaClass) {
                    double[] array = (double[]) value.asObject();
                    int length = array.length;
                    BinaryStreamUtils.writeVarInt(output, length);
                    for (int i = 0; i < length; i++) {
                        serialize(v.update(array[i]), config, baseColumn, output);
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                }
            }
        }

        private ProtonValue readArray(ProtonValue ref, ProtonConfig config, ProtonColumn nestedColumn,
                ProtonColumn baseColumn, ProtonInputStream input, int length, int level) throws IOException {
            Class<?> javaClass = baseColumn.getDataType().getPrimitiveClass();
            if (level > 1 || !javaClass.isPrimitive()) {
                Object[] array = (Object[]) ProtonValues.createPrimitiveArray(javaClass, length, level);
                for (int i = 0; i < length; i++) {
                    array[i] = deserialize(null, config, nestedColumn, input).asObject();
                }
                ref.update(array);
            } else {
                if (byte.class == javaClass) {
                    byte[] array = new byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asByte();
                    }
                    ref.update(array);
                } else if (short.class == javaClass) {
                    short[] array = new short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asShort();
                    }
                    ref.update(array);
                } else if (int.class == javaClass) {
                    int[] array = new int[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asInteger();
                    }
                    ref.update(array);
                } else if (long.class == javaClass) {
                    long[] array = new long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asLong();
                    }
                    ref.update(array);
                } else if (float.class == javaClass) {
                    float[] array = new float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asFloat();
                    }
                    ref.update(array);
                } else if (double.class == javaClass) {
                    double[] array = new double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = deserialize(null, config, baseColumn, input).asDouble();
                    }
                    ref.update(array);
                } else {
                    throw new IllegalArgumentException("Unsupported primitive type: " + javaClass);
                }
            }

            return ref;
        }

        private final Map<ProtonAggregateFunction, ProtonDeserializer<ProtonValue>> aggDeserializers;
        private final Map<ProtonAggregateFunction, ProtonSerializer<ProtonValue>> aggSerializers;

        private final Map<ProtonDataType, ProtonDeserializer<? extends ProtonValue>> deserializers;
        private final Map<ProtonDataType, ProtonSerializer<? extends ProtonValue>> serializers;

        private void buildMappingsForAggregateFunctions() {
            // aggregate functions
            // buildAggMappings(aggDeserializers, aggSerializers,
            // (r, f, c, i) -> {
            // BinaryStreamUtils.readInt8(i); // always 1?
            // return deserialize(r, f, c.getNestedColumns().get(0), i);
            // },
            // (v, f, c, o) -> {
            // // no that simple:
            // // * select anyState(n) from (select '5' where 0) => FFFF
            // // * select anyState(n) from (select '5') => 0200 0000 3500
            // BinaryStreamUtils.writeInt8(o, (byte) 1);
            // serialize(v, f, c.getNestedColumns().get(0), o);
            // }, ProtonAggregateFunction.any);
            buildAggMappings(aggDeserializers, aggSerializers,
                    (r, f, c, i) -> ProtonBitmapValue
                            .of(BinaryStreamUtils.readBitmap(i, c.getNestedColumns().get(0).getDataType())),
                    (v, f, c, o) -> BinaryStreamUtils.writeBitmap(o, v.asObject(ProtonBitmap.class)),
                    ProtonAggregateFunction.group_bitmap);

            // now the data type
            buildMappings(deserializers, serializers, (r, f, c, i) -> aggDeserializers
                    .getOrDefault(c.getAggregateFunction(), ProtonDeserializer.NOT_SUPPORTED)
                    .deserialize(r, f, c, i),
                    (v, f, c, o) -> aggSerializers
                            .getOrDefault(c.getAggregateFunction(), ProtonSerializer.NOT_SUPPORTED)
                            .serialize(v, f, c, o),
                    ProtonDataType.aggregate_function);
        }

        private void buildMappingsForDataTypes() {
            // enums
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonEnumValue.of(r, c.getEnumConstants(), BinaryStreamUtils.readInt8(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ProtonDataType.enum8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonEnumValue.of(r, c.getEnumConstants(), BinaryStreamUtils.readInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ProtonDataType.enum16);
            // bool and numbers
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBoolValue.of(r, BinaryStreamUtils.readBoolean(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeBoolean(o, v.asBoolean()), ProtonDataType.bool);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonByteValue.of(r, BinaryStreamUtils.readInt8(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt8(o, v.asByte()), ProtonDataType.int8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonShortValue.of(r, BinaryStreamUtils.readUnsignedInt8(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt8(o, v.asInteger()), ProtonDataType.uint8);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonShortValue.of(r, BinaryStreamUtils.readInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt16(o, v.asShort()), ProtonDataType.int16);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt16(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt16(o, v.asInteger()), ProtonDataType.uint16);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonIntegerValue.of(r, BinaryStreamUtils.readInt32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt32(o, v.asInteger()), ProtonDataType.int32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonLongValue.of(r, false, BinaryStreamUtils.readUnsignedInt32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt32(o, v.asLong()), ProtonDataType.uint32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonLongValue.of(r, false, BinaryStreamUtils.readInt64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ProtonDataType.interval_year,
                    ProtonDataType.interval_quarter, ProtonDataType.interval_month,
                    ProtonDataType.interval_week, ProtonDataType.interval_day, ProtonDataType.interval_hour,
                    ProtonDataType.interval_minute, ProtonDataType.interval_second, ProtonDataType.int64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonLongValue.of(r, true, BinaryStreamUtils.readInt64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt64(o, v.asLong()), ProtonDataType.uint64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigIntegerValue.of(r, BinaryStreamUtils.readInt128(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt128(o, v.asBigInteger()), ProtonDataType.int128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt128(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt128(o, v.asBigInteger()),
                    ProtonDataType.uint128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigIntegerValue.of(r, BinaryStreamUtils.readInt256(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInt256(o, v.asBigInteger()), ProtonDataType.int256);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigIntegerValue.of(r, BinaryStreamUtils.readUnsignedInt256(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUnsignedInt256(o, v.asBigInteger()),
                    ProtonDataType.uint256);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonFloatValue.of(r, BinaryStreamUtils.readFloat32(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeFloat32(o, v.asFloat()), ProtonDataType.float32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonDoubleValue.of(r, BinaryStreamUtils.readFloat64(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeFloat64(o, v.asDouble()), ProtonDataType.float64);

            // decimals
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigDecimalValue.of(r,
                            BinaryStreamUtils.readDecimal(i, c.getPrecision(), c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal(o, v.asBigDecimal(c.getScale()), c.getPrecision(),
                            c.getScale()),
                    ProtonDataType.decimal);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigDecimalValue.of(r, BinaryStreamUtils.readDecimal32(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal32(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ProtonDataType.decimal32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigDecimalValue.of(r, BinaryStreamUtils.readDecimal64(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal64(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ProtonDataType.decimal64);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigDecimalValue.of(r, BinaryStreamUtils.readDecimal128(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal128(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ProtonDataType.decimal128);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonBigDecimalValue.of(r, BinaryStreamUtils.readDecimal256(i, c.getScale())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDecimal256(o, v.asBigDecimal(c.getScale()), c.getScale()),
                    ProtonDataType.decimal256);

            // date, time, datetime and IPs
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonDateValue.of(r,
                            BinaryStreamUtils.readDate(i, f.getTimeZoneForDate())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate(o, v.asDate(), f.getTimeZoneForDate()),
                    ProtonDataType.date);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonDateValue.of(r,
                            BinaryStreamUtils.readDate32(i, f.getTimeZoneForDate())),
                    (v, f, c, o) -> BinaryStreamUtils.writeDate32(o, v.asDate(), f.getTimeZoneForDate()),
                    ProtonDataType.date32);
            buildMappings(deserializers, serializers, (r, f, c, i) -> c.getTimeZone() == null
                    ? ProtonDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getUseTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, f.getUseTimeZone())),
                            c.getScale(), f.getUseTimeZone())
                    : ProtonOffsetDateTimeValue.of(r,
                            (c.getScale() > 0 ? BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone())
                                    : BinaryStreamUtils.readDateTime(i, c.getTimeZone())),
                            c.getScale(), c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
                    ProtonDataType.datetime);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null
                            ? ProtonDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, f.getUseTimeZone()), 0,
                                    f.getUseTimeZone())
                            : ProtonOffsetDateTimeValue.of(r, BinaryStreamUtils.readDateTime(i, c.getTimeZone()), 0,
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime32(o, v.asDateTime(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
                    ProtonDataType.datetime32);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> c.getTimeZone() == null ? ProtonDateTimeValue.of(r,
                            BinaryStreamUtils.readDateTime64(i, c.getScale(), f.getUseTimeZone()), c.getScale(),
                            f.getUseTimeZone())
                            : ProtonOffsetDateTimeValue.of(r,
                                    BinaryStreamUtils.readDateTime64(i, c.getScale(), c.getTimeZone()), c.getScale(),
                                    c.getTimeZone()),
                    (v, f, c, o) -> BinaryStreamUtils.writeDateTime64(o, v.asDateTime(), c.getScale(),
                            c.getTimeZoneOrDefault(f.getUseTimeZone())),
                    ProtonDataType.datetime64);

            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonIpv4Value.of(r, BinaryStreamUtils.readInet4Address(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInet4Address(o, v.asInet4Address()),
                    ProtonDataType.ipv4);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonIpv6Value.of(r, BinaryStreamUtils.readInet6Address(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeInet6Address(o, v.asInet6Address()),
                    ProtonDataType.ipv6);

            // string and uuid
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonStringValue.of(r, i.readBytes(c.getPrecision())),
                    (v, f, c, o) -> o.write(v.asBinary(c.getPrecision())), ProtonDataType.fixed_string);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonStringValue.of(r, i.readBytes(i.readVarInt())),
                    (v, f, c, o) -> BinaryStreamUtils.writeString(o, v.asBinary()), ProtonDataType.string);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonUuidValue.of(r, BinaryStreamUtils.readUuid(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeUuid(o, v.asUuid()), ProtonDataType.uuid);

            // geo types
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonGeoPointValue.of(r, BinaryStreamUtils.readGeoPoint(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoPoint(o, v.asObject(double[].class)),
                    ProtonDataType.point);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonGeoRingValue.of(r, BinaryStreamUtils.readGeoRing(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoRing(o, v.asObject(double[][].class)),
                    ProtonDataType.ring);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonGeoPolygonValue.of(r, BinaryStreamUtils.readGeoPolygon(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoPolygon(o, v.asObject(double[][][].class)),
                    ProtonDataType.polygon);
            buildMappings(deserializers, serializers,
                    (r, f, c, i) -> ProtonGeoMultiPolygonValue.of(r, BinaryStreamUtils.readGeoMultiPolygon(i)),
                    (v, f, c, o) -> BinaryStreamUtils.writeGeoMultiPolygon(o, v.asObject(double[][][][].class)),
                    ProtonDataType.multi_polygon);

            // advanced types
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                int length = BinaryStreamUtils.readVarInt(i);
                if (r == null) {
                    r = ProtonValues.newValue(f, c);
                }
                return readArray(r, f, c.getNestedColumns().get(0), c.getArrayBaseColumn(), i, length,
                        c.getArrayNestedLevel());
            }, this::writeArray, ProtonDataType.array);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                Map<Object, Object> map = new LinkedHashMap<>();
                ProtonColumn keyCol = c.getKeyInfo();
                ProtonColumn valCol = c.getValueInfo();
                for (int k = 0, len = BinaryStreamUtils.readVarInt(i); k < len; k++) {
                    map.put(deserialize(null, f, keyCol, i).asObject(), deserialize(null, f, valCol, i).asObject());
                }
                return ProtonMapValue.of(map, valCol.getDataType().getObjectClass(),
                        valCol.getDataType().getObjectClass());
            }, (v, f, c, o) -> {
                Map<Object, Object> map = v.asMap();
                BinaryStreamUtils.writeVarInt(o, map.size());
                if (!map.isEmpty()) {
                    ProtonColumn keyCol = c.getKeyInfo();
                    ProtonColumn valCol = c.getValueInfo();
                    ProtonValue kVal = ProtonValues.newValue(f, keyCol);
                    ProtonValue vVal = ProtonValues.newValue(f, valCol);
                    for (Entry<Object, Object> e : map.entrySet()) {
                        serialize(kVal.update(e.getKey()), f, keyCol, o);
                        serialize(vVal.update(e.getValue()), f, valCol, o);
                    }
                }
            }, ProtonDataType.map);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                int count = c.getNestedColumns().size();
                String[] names = new String[count];
                Object[][] values = new Object[count][];
                int l = 0;
                for (ProtonColumn col : c.getNestedColumns()) {
                    names[l] = col.getColumnName();
                    int k = BinaryStreamUtils.readVarInt(i);
                    Object[] nvalues = new Object[k];
                    for (int j = 0; j < k; j++) {
                        nvalues[j] = deserialize(null, f, col, i).asObject();
                    }
                    values[l++] = nvalues;
                }
                return ProtonNestedValue.of(r, c.getNestedColumns(), values);
            }, (v, f, c, o) -> {
                Object[][] values = (Object[][]) v.asObject();
                int l = 0;
                for (ProtonColumn col : c.getNestedColumns()) {
                    Object[] nvalues = values[l++];
                    int k = nvalues.length;
                    ProtonValue nv = ProtonValues.newValue(f, col);
                    BinaryStreamUtils.writeVarInt(o, k);
                    for (int j = 0; j < k; j++) {
                        serialize(nv.update(nvalues[j]), f, col, o);
                    }
                }
            }, ProtonDataType.nested);
            buildMappings(deserializers, serializers, (r, f, c, i) -> {
                List<Object> tupleValues = new ArrayList<>(c.getNestedColumns().size());
                for (ProtonColumn col : c.getNestedColumns()) {
                    tupleValues.add(deserialize(null, f, col, i).asObject());
                }
                return ProtonTupleValue.of(r, tupleValues);
            }, (v, f, c, o) -> {
                List<Object> tupleValues = v.asTuple();
                Iterator<Object> tupleIterator = tupleValues.iterator();
                for (ProtonColumn col : c.getNestedColumns()) {
                    // FIXME tooooo slow
                    ProtonValue tv = ProtonValues.newValue(f, col);
                    if (tupleIterator.hasNext()) {
                        serialize(tv.update(tupleIterator.next()), f, col, o);
                    } else {
                        serialize(tv, f, col, o);
                    }
                }
            }, ProtonDataType.tuple);
        }

        private MappedFunctions() {
            aggDeserializers = new EnumMap<>(ProtonAggregateFunction.class);
            aggSerializers = new EnumMap<>(ProtonAggregateFunction.class);

            deserializers = new EnumMap<>(ProtonDataType.class);
            serializers = new EnumMap<>(ProtonDataType.class);

            buildMappingsForAggregateFunctions();
            buildMappingsForDataTypes();
        }

        @SuppressWarnings("unchecked")
        public ProtonValue deserialize(ProtonValue ref, ProtonConfig config, ProtonColumn column,
                ProtonInputStream input) throws IOException {
            if (column.isNullable() && BinaryStreamUtils.readNull(input)) {
                return ref == null ? ProtonValues.newValue(config, column) : ref.resetToNullOrEmpty();
            }

            ProtonDeserializer<ProtonValue> func = (ProtonDeserializer<ProtonValue>) deserializers
                    .get(column.getDataType());
            if (func == null) {
                throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE + column.getDataType().name());
            }
            return func.deserialize(ref, config, column, input);
        }

        @SuppressWarnings("unchecked")
        public void serialize(ProtonValue value, ProtonConfig config, ProtonColumn column,
                OutputStream output) throws IOException {
            if (column.isNullable()) { // always false for geo types, and Array, Nested, Map and Tuple etc.
                if (value.isNullOrEmpty()) {
                    BinaryStreamUtils.writeNull(output);
                    return;
                } else {
                    BinaryStreamUtils.writeNonNull(output);
                }
            }

            ProtonSerializer<ProtonValue> func = (ProtonSerializer<ProtonValue>) serializers
                    .get(column.getDataType());
            if (func == null) {
                throw new IllegalArgumentException(ERROR_UNKNOWN_DATA_TYPE + column.getDataType().name());
            }
            func.serialize(value, config, column, output);
        }
    }

    public static MappedFunctions getMappedFunctions() {
        return MappedFunctions.instance;
    }

    // TODO this is where ASM should come into play...
    private class Records implements Iterator<ProtonRecord> {
        private final Supplier<ProtonSimpleRecord> factory;
        private ProtonSimpleRecord record;

        Records() {
            int size = columns.size();
            if (config.isReuseValueWrapper()) {
                ProtonValue[] values = new ProtonValue[size];
                record = new ProtonSimpleRecord(columns, values);
                factory = () -> record;
            } else {
                factory = () -> new ProtonSimpleRecord(columns, new ProtonValue[size]);
            }
        }

        ProtonRecord readNextRow() {
            int index = 0;
            int size = columns.size();
            ProtonSimpleRecord currentRow = factory.get();
            ProtonValue[] values = currentRow.getValues();
            ProtonColumn column = null;
            try {
                MappedFunctions m = getMappedFunctions();
                for (; index < size; index++) {
                    column = columns.get(index);
                    values[index] = m.deserialize(values[index], config, column, input);
                }
            } catch (EOFException e) {
                if (index == 0) { // end of the stream, which is fine
                    values = null;
                } else {
                    throw new UncheckedIOException(
                            ProtonUtils.format("Reached end of the stream when reading column #%d(total %d): %s",
                                    index + 1, size, column),
                            e);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(
                        ProtonUtils.format("Failed to read column #%d(total %d): %s", index + 1, size, column), e);
            }

            return currentRow;
        }

        @Override
        public boolean hasNext() {
            try {
                return input.available() > 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ProtonRecord next() {
            ProtonRecord r = readNextRow();
            if (r == null) {
                throw new NoSuchElementException("No more record");
            }
            return r;
        }
    }

    @Override
    protected List<ProtonColumn> readColumns() throws IOException {
        if (!config.getFormat().hasHeader()) {
            return Collections.emptyList();
        }

        int size = 0;
        try {
            size = input.readVarInt();
        } catch (EOFException e) {
            // no result returned
            return Collections.emptyList();
        }

        String[] names = new String[ProtonChecker.between(size, "size", 0, Integer.MAX_VALUE)];
        for (int i = 0; i < size; i++) {
            names[i] = input.readUnicodeString();
        }

        List<ProtonColumn> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            // a bit risky here - what if Proton support user type?
            columns.add(ProtonColumn.of(names[i], input.readAsciiString()));
        }

        return columns;
    }

    public ProtonRowBinaryProcessor(ProtonConfig config, ProtonInputStream input, OutputStream output,
            List<ProtonColumn> columns, Map<String, Object> settings) throws IOException {
        super(config, input, output, columns, settings);
    }

    @Override
    public Iterable<ProtonRecord> records() {
        return columns.isEmpty() ? Collections.emptyList() : new Iterable<ProtonRecord>() {
            @Override
            public Iterator<ProtonRecord> iterator() {
                return new Records();
            }
        };
    }
}
