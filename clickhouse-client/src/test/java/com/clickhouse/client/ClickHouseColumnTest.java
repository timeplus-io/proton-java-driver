package com.clickhouse.client;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ClickHouseColumnTest {
    @DataProvider(name = "enumTypesProvider")
    private Object[][] getEnumTypes() {
        return new Object[][] { { "enum" }, { "enum8" }, { "enum16" } };
    }

    @Test(groups = { "unit" })
    public void testReadColumn() {
        String args = "aggregate_function(max, uint64), cc low_cardinality(nullable(string)), a uint8 null";
        List<ClickHouseColumn> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf("cc") - 2);
        Assert.assertEquals(list.size(), 1);
        Assert.assertFalse(list.get(0).isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(list.get(0).getEstimatedLength(), 1);
        list.clear();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.indexOf("cc") + 3, args.length(), null, list),
                args.lastIndexOf(','));
        list.clear();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.lastIndexOf("uint8"), args.length(), null, list),
                args.length() - 1);
        Assert.assertEquals(list.size(), 1);
        ClickHouseColumn column = list.get(0);
        Assert.assertNotNull(column);
        Assert.assertFalse(column.isLowCardinality());
        Assert.assertTrue(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.uint8);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "INTEGER not null, b datetime64(3) NULL";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.indexOf(','));
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertFalse(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.int32);
        Assert.assertTrue(column.isFixedLength(), "Should have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 4);
        list.clear();

        Assert.assertEquals(ClickHouseColumn.readColumn(args, args.indexOf('d'), args.length(), null, list),
                args.length() - 1);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertTrue(column.isNullable());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.datetime64);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(groups = { "unit" })
    public void testReadNestedColumn() {
        String args = "array(array(nullable(uint8)))";
        List<ClickHouseColumn> list = new LinkedList<>();
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        ClickHouseColumn column = list.get(0);
        Assert.assertEquals(column.getNestedColumns().size(), 1);
        Assert.assertEquals(column.getNestedColumns().get(0).getNestedColumns().size(), 1);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = " tuple(nullable(fixed_string(3)), array(uint8),string not null) ";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 1, args.length(), null, list), args.length() - 2);
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args.trim());
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 3);
        list.clear();

        args = "map(uint8 , uint8)";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "map(string, fixed_string(233))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "map(string, tuple(uint8, nullable(string), uint16 null))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertEquals(column.getNestedColumns().size(), 2);
        Assert.assertEquals(column.getKeyInfo().getOriginalTypeName(), "string");
        Assert.assertEquals(column.getValueInfo().getOriginalTypeName(), "tuple(uint8, nullable(string), uint16 null)");
        Assert.assertEquals(column.getValueInfo().getNestedColumns().size(), 3);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
        list.clear();

        args = "nested(\na array(nullable(uint8)), `b b` low_cardinality(nullable(datetime64(3))))";
        Assert.assertEquals(ClickHouseColumn.readColumn(args, 0, args.length(), null, list), args.length());
        Assert.assertEquals(list.size(), 1);
        column = list.get(0);
        Assert.assertEquals(column.getOriginalTypeName(), args);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(groups = { "unit" })
    public void testParse() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr", "nullable(array(nullable(uint8))");
        Assert.assertNotNull(column);

        List<ClickHouseColumn> list = ClickHouseColumn.parse("a String not null, b String null");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String not null, b Int8");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String, b String null");
        Assert.assertEquals(list.size(), 2);
        list = ClickHouseColumn.parse("a String default 'cc', b String null");
        Assert.assertEquals(list.size(), 2);
    }

    @Test(groups = { "unit" })
    public void testAggregationFunction() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("agg_func", "aggregate_function(group_bitmap, uint32)");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.aggregate_function);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.group_bitmap);
        Assert.assertEquals(column.getFunction(), "group_bitmap");
        Assert.assertEquals(column.getNestedColumns(), Collections.singletonList(ClickHouseColumn.of("", "uint32")));
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);

        column = ClickHouseColumn.of("aggFunc", "aggregate_function(quantiles(0.5, 0.9), nullable(uint64))");
        Assert.assertTrue(column.isAggregateFunction());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.aggregate_function);
        Assert.assertEquals(column.getAggregateFunction(), ClickHouseAggregateFunction.quantiles);
        Assert.assertEquals(column.getFunction(), "quantiles(0.5,0.9)");
        Assert.assertEquals(column.getNestedColumns(),
                Collections.singletonList(ClickHouseColumn.of("", "nullable(uint64)")));
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(groups = { "unit" })
    public void testArray() throws Exception {
        ClickHouseColumn column = ClickHouseColumn.of("arr",
                "array(array(array(array(array(map(low_cardinality(string), tuple(array(uint8),low_cardinality(string))))))))");
        Assert.assertTrue(column.isArray());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.array);
        Assert.assertEquals(column.getArrayNestedLevel(), 5);
        Assert.assertEquals(column.getArrayBaseColumn().getOriginalTypeName(),
                "map(low_cardinality(string), tuple(array(uint8),low_cardinality(string)))");
        Assert.assertFalse(column.getArrayBaseColumn().isArray());

        Assert.assertEquals(column.getArrayBaseColumn().getArrayNestedLevel(), 0);
        Assert.assertEquals(column.getArrayBaseColumn().getArrayBaseColumn(), null);
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);

        ClickHouseColumn c = ClickHouseColumn.of("arr", "array(low_cardinality(nullable(string)))");
        Assert.assertTrue(c.isArray());
        Assert.assertEquals(c.getDataType(), ClickHouseDataType.array);
        Assert.assertEquals(c.getArrayNestedLevel(), 1);
        Assert.assertEquals(c.getArrayBaseColumn().getOriginalTypeName(), "low_cardinality(nullable(string))");
        Assert.assertFalse(c.getArrayBaseColumn().isArray());
        Assert.assertFalse(column.isFixedLength(), "Should not have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), 1);
    }

    @Test(dataProvider = "enumTypesProvider", groups = { "unit" })
    public void testEnum(String typeName) throws Exception {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ClickHouseColumn.of("e", typeName + "('Query''Start' = a)"));
        Assert.assertThrows(IllegalArgumentException.class, () -> ClickHouseColumn.of("e", typeName + "(aa,1)"));
        ClickHouseColumn column = ClickHouseColumn.of("e", typeName + "('Query''Start' = 1, 'Query\\'Finish' = 10)");
        Assert.assertTrue(column.isEnum());
        Assert.assertEquals(column.getDataType(), ClickHouseDataType.of(typeName));
        Assert.assertThrows(IllegalArgumentException.class, () -> column.getEnumConstants().name(2));
        Assert.assertThrows(IllegalArgumentException.class, () -> column.getEnumConstants().value(""));
        Assert.assertEquals(column.getEnumConstants().name(1), "Query'Start");
        Assert.assertEquals(column.getEnumConstants().name(10), "Query'Finish");
        Assert.assertEquals(column.getEnumConstants().value("Query'Start"), 1);
        Assert.assertEquals(column.getEnumConstants().value("Query'Finish"), 10);
        Assert.assertTrue(column.isFixedLength(), "Should have fixed length in byte");
        Assert.assertEquals(column.getEstimatedLength(), column.getDataType().getByteLength());
    }
}
