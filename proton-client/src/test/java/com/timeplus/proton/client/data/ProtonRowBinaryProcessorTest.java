package com.timeplus.proton.client.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonValue;
import com.timeplus.proton.client.data.array.ProtonByteArrayValue;
import com.timeplus.proton.client.data.array.ProtonShortArrayValue;

public class ProtonRowBinaryProcessorTest {
    private ProtonRowBinaryProcessor newProcessor(int... bytes) throws IOException {
        return new ProtonRowBinaryProcessor(new ProtonConfig(), BinaryStreamUtilsTest.generateInput(bytes),
                null, Collections.emptyList(), null);
    }

    // @Test(groups = { "unit" })
    // public void testDeserializeAggregateFunction() throws IOException {
    // ProtonConfig config = new ProtonConfig();
    // ProtonValue value =
    // ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
    // ProtonColumn.of("a", "AggregateFunction(any, String)"),
    // BinaryStreamUtilsTest.generateInput(0xFF, 0xFF));
    // Assert.assertTrue(value instanceof ProtonStringValue);
    // Assert.assertEquals(value.asObject(), null);
    // }

    @Test(groups = { "unit" })
    public void testDeserializeArray() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("a", "array(uint8)"), BinaryStreamUtilsTest.generateInput(2, 1, 2));
        Assert.assertTrue(value instanceof ProtonShortArrayValue);
        Assert.assertEquals(value.asObject(), new short[] { 1, 2 });
        Object[] shortArray = value.asArray();
        Assert.assertEquals(shortArray.length, 2);
        Assert.assertEquals(shortArray[0], Short.valueOf("1"));
        Assert.assertEquals(shortArray[1], Short.valueOf("2"));

        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("a", "array(nullable(int8))"), BinaryStreamUtilsTest.generateInput(2, 0, 1, 0, 2));
        Assert.assertTrue(value instanceof ProtonByteArrayValue);
        Assert.assertEquals(value.asObject(), new byte[] { 1, 2 });
        Object[] byteArray = value.asArray();
        Assert.assertEquals(byteArray.length, 2);
        Assert.assertEquals(byteArray[0], Byte.valueOf("1"));
        Assert.assertEquals(byteArray[1], Byte.valueOf("2"));

        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("a", "array(array(uint8))"), BinaryStreamUtilsTest.generateInput(1, 2, 1, 2));
        Assert.assertTrue(value instanceof ProtonArrayValue);
        Assert.assertEquals(value.asObject(), new short[][] { new short[] { 1, 2 } });
        Object[] array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        Assert.assertEquals(array[0], new short[] { 1, 2 });

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("a", "array(tuple(string, uint8))"),
                BinaryStreamUtilsTest.generateInput(3, 1, 0x61, 3, 1, 0x62, 2, 1, 0x63, 1));
        Assert.assertTrue(value instanceof ProtonArrayValue);
        array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 3);
        Assert.assertEquals(((List<Object>) array[0]).size(), 2);
        Assert.assertEquals(((List<Object>) array[0]).get(0), "a");
        Assert.assertEquals(((List<Object>) array[0]).get(1), Short.valueOf("3"));
        Assert.assertEquals(((List<Object>) array[1]).size(), 2);
        Assert.assertEquals(((List<Object>) array[1]).get(0), "b");
        Assert.assertEquals(((List<Object>) array[1]).get(1), Short.valueOf("2"));
        Assert.assertEquals(((List<Object>) array[2]).size(), 2);
        Assert.assertEquals(((List<Object>) array[2]).get(0), "c");
        Assert.assertEquals(((List<Object>) array[2]).get(1), Short.valueOf("1"));

        // insert into x values([{ 'a' : (null, 3), 'b' : (1, 2), 'c' : (2, 1)}])
        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("a", "array(map(string, tuple(nullable(uint8), uint16)))"),
                BinaryStreamUtilsTest.generateInput(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1, 0x63, 0, 2, 1, 0));
        Assert.assertTrue(value instanceof ProtonArrayValue);
        array = (Object[]) value.asObject();
        Assert.assertEquals(array.length, 1);
        Map<String, List<Object>> map = (Map<String, List<Object>>) array[0];
        Assert.assertEquals(map.size(), 3);
        List<Object> l = map.get("a");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), null);
        Assert.assertEquals(l.get(1), 3);
        l = map.get("b");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), Short.valueOf("1"));
        Assert.assertEquals(l.get(1), 2);
        l = map.get("c");
        Assert.assertEquals(l.size(), 2);
        Assert.assertEquals(l.get(0), Short.valueOf("2"));
        Assert.assertEquals(l.get(1), 1);
    }

    @Test(groups = { "unit" })
    public void testSerializeArray() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonShortArrayValue.of(new short[] { 1, 2 });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("a", "array(uint8)"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 1, 2));

        value = ProtonByteArrayValue.of(new byte[] { 1, 2 });
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("a", "array(nullable(int8))"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 0, 1, 0, 2));

        value = ProtonArrayValue.of(new short[][] { new short[] { 1, 2 } });
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("a", "array(array(uint8))"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 2, 1, 2));

        // SELECT arrayZip(['a', 'b', 'c'], [3, 2, 1])
        value = ProtonArrayValue.of(new Object[] { Arrays.asList("a", (short) 3), Arrays.asList("b", (short) 2),
                Arrays.asList("c", (short) 1) });
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("a", "array(tuple(string, uint8))"), bas);
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(3, 1, 0x61, 3, 1, 0x62, 2, 1, 0x63, 1));

        // insert into x values([{ 'a' : (null, 3), 'b' : (1, 2), 'c' : (2, 1)}])
        value = ProtonArrayValue.of(new Object[] { new LinkedHashMap<String, List<Object>>() {
            {
                put("a", Arrays.asList((Short) null, 3));
                put("b", Arrays.asList(Short.valueOf("1"), 2));
                put("c", Arrays.asList(Short.valueOf("2"), 1));
            }
        } });
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("a", "array(map(string, tuple(nullable(uint8), uint16)))"), bas);
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(1, 3, 1, 0x61, 1, 3, 0, 1, 0x62, 0, 1, 2, 0, 1, 0x63, 0, 2, 1, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeMap() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("m", "map(uint8, uint8)"), BinaryStreamUtilsTest.generateInput(2, 2, 2, 1, 1));
        Assert.assertTrue(value instanceof ProtonMapValue);
        Map<?, ?> map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get((short) 2), (short) 2);
        Assert.assertEquals(map.get((short) 1), (short) 1);

        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("m", "map(string, uint32)"),
                BinaryStreamUtilsTest.generateInput(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
        Assert.assertTrue(value instanceof ProtonMapValue);
        map = (Map<?, ?>) value.asObject();
        Assert.assertEquals(map.size(), 2);
        Assert.assertEquals(map.get("2"), 2L);
        Assert.assertEquals(map.get("1"), 1L);
    }

    @Test(groups = { "unit" })
    public void testSerializeMap() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonMapValue.of(new LinkedHashMap<Short, Short>() {
            {
                put((short) 2, (short) 2);
                put((short) 1, (short) 1);
            }
        }, Short.class, Short.class);
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("m", "map(uint8, uint8)"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(2, 2, 2, 1, 1));

        value = ProtonMapValue.of(new LinkedHashMap<String, Long>() {
            {
                put("2", 2L);
                put("1", 1L);
            }
        }, String.class, Long.class);
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("m", "map(string, uint32)"), bas);
        Assert.assertEquals(bas.toByteArray(),
                BinaryStreamUtilsTest.generateBytes(2, 1, 0x32, 2, 0, 0, 0, 1, 0x31, 1, 0, 0, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeNested() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("n", "nested(n1 uint8, n2 nullable(string), n3 int16)"),
                BinaryStreamUtilsTest.generateInput(1, 1, 1, 0, 1, 0x32, 1, 3, 0));
        Assert.assertTrue(value instanceof ProtonNestedValue);

        List<ProtonColumn> columns = ((ProtonNestedValue) value).getColumns();
        Object[][] values = (Object[][]) value.asObject();
        Assert.assertEquals(columns.size(), 3);
        Assert.assertEquals(columns.get(0).getColumnName(), "n1");
        Assert.assertEquals(columns.get(1).getColumnName(), "n2");
        Assert.assertEquals(columns.get(2).getColumnName(), "n3");
        Assert.assertEquals(values.length, 3);
        Assert.assertEquals(values[0], new Short[] { Short.valueOf("1") });
        Assert.assertEquals(values[1], new String[] { "2" });
        Assert.assertEquals(values[2], new Short[] { Short.valueOf("3") });
    }

    @Test(groups = { "unit" })
    public void testSerializeNested() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonNestedValue.of(
                ProtonColumn.of("n", "nested(n1 uint8, n2 nullable(string), n3 int16)").getNestedColumns(),
                new Object[][] { new Short[] { Short.valueOf("1") }, new String[] { "2" },
                        new Short[] { Short.valueOf("3") } });
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("n", "nested(n1 uint8, n2 nullable(string), n3 int16)"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 1, 1, 0, 1, 0x32, 1, 3, 0));
    }

    @Test(groups = { "unit" })
    public void testDeserializeTuple() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(null, config,
                ProtonColumn.of("t", "tuple(uint8, string)"), BinaryStreamUtilsTest.generateInput(1, 1, 0x61));
        Assert.assertTrue(value instanceof ProtonTupleValue);
        List<Object> values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 2);
        Assert.assertEquals(values.get(0), (short) 1);
        Assert.assertEquals(values.get(1), "a");

        value = ProtonRowBinaryProcessor.getMappedFunctions().deserialize(value, config,
                ProtonColumn.of("t", "tuple(uint32, int128, nullable(ipv4)))"), BinaryStreamUtilsTest.generateInput(
                        1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05, 0xa8, 0xc0));
        Assert.assertTrue(value instanceof ProtonTupleValue);
        values = (List<Object>) value.asObject();
        Assert.assertEquals(values.size(), 3);
        Assert.assertEquals(values.get(0), 1L);
        Assert.assertEquals(values.get(1), BigInteger.valueOf(2));
        Assert.assertEquals(values.get(2), InetAddress.getByName("192.168.5.1"));
    }

    @Test(groups = { "unit" })
    public void testSerializeTuple() throws IOException {
        ProtonConfig config = new ProtonConfig();
        ProtonValue value = ProtonTupleValue.of((byte) 1, "a");
        ByteArrayOutputStream bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("t", "tuple(uint8, string)"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 1, 0x61));

        value = ProtonTupleValue.of(1L, BigInteger.valueOf(2), InetAddress.getByName("192.168.5.1"));
        bas = new ByteArrayOutputStream();
        ProtonRowBinaryProcessor.getMappedFunctions().serialize(value, config,
                ProtonColumn.of("t", "tuple(uint32, int128, nullable(ipv4)))"), bas);
        Assert.assertEquals(bas.toByteArray(), BinaryStreamUtilsTest.generateBytes(1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0x05, 0xa8, 0xc0));
    }
}
