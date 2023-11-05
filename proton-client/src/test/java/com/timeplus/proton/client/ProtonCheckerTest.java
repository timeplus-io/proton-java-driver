package com.timeplus.proton.client;

import java.math.BigInteger;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonCheckerTest {
    @Test(groups = { "unit" })
    public void testBetween() {
        // int
        Assert.assertEquals(ProtonChecker.between(0, "value", 0, 0), 0);
        Assert.assertEquals(ProtonChecker.between(0, "value", -1, 1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.between(1, "value", 2, 3));

        // long
        Assert.assertEquals(ProtonChecker.between(0L, "value", 0L, 0L), 0L);
        Assert.assertEquals(ProtonChecker.between(0L, "value", -1L, 1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.between(1L, "value", 2L, 3L));

        // bigint
        Assert.assertEquals(ProtonChecker.between(BigInteger.ZERO, "value", BigInteger.ZERO, BigInteger.ZERO),
                BigInteger.ZERO);
        Assert.assertEquals(
                ProtonChecker.between(BigInteger.ZERO, "value", BigInteger.valueOf(-1L), BigInteger.ONE),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.between(BigInteger.ONE, "value",
                BigInteger.valueOf(2), BigInteger.valueOf(3L)));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrEmpty() {
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(null));
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(""));
        Assert.assertFalse(ProtonChecker.isNullOrEmpty(" "));
    }

    @Test(groups = { "unit" })
    public void testIsNullOrBlank() {
        Assert.assertTrue(ProtonChecker.isNullOrBlank(null));
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(new StringBuilder()));
        Assert.assertTrue(ProtonChecker.isNullOrEmpty(new StringBuffer()));
        Assert.assertTrue(ProtonChecker.isNullOrBlank(""));
        Assert.assertTrue(ProtonChecker.isNullOrBlank(" \t\r\n  "));
    }

    @Test(groups = { "unit" })
    public void testNonBlank() {
        Assert.assertEquals(ProtonChecker.nonBlank(" 1", "value"), " 1");

        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonBlank(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonBlank("", ""));
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonBlank(" ", ""));
    }

    @Test(groups = { "unit" })
    public void testNonEmpty() {
        Assert.assertEquals(ProtonChecker.nonEmpty(" ", "value"), " ");

        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonEmpty(null, null));
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonEmpty("", ""));
    }

    @Test(groups = { "unit" })
    public void testNonNull() {
        Object obj;
        Assert.assertEquals(ProtonChecker.nonNull(obj = new Object(), "value"), obj);
        Assert.assertEquals(ProtonChecker.nonNull(obj = 1, "value"), obj);

        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.nonNull(null, null));
    }

    @Test(groups = { "unit" })
    public void testNotLessThan() {
        // int
        Assert.assertEquals(ProtonChecker.notLessThan(1, "value", 0), 1);
        Assert.assertEquals(ProtonChecker.notLessThan(0, "value", 0), 0);
        Assert.assertEquals(ProtonChecker.notLessThan(0, "value", -1), 0);
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.notLessThan(0, "value", 1));

        // long
        Assert.assertEquals(ProtonChecker.notLessThan(1L, "value", 0L), 1L);
        Assert.assertEquals(ProtonChecker.notLessThan(0L, "value", 0L), 0L);
        Assert.assertEquals(ProtonChecker.notLessThan(0L, "value", -1L), 0L);
        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.notLessThan(0L, "value", 1L));

        // bigint
        Assert.assertEquals(ProtonChecker.notLessThan(BigInteger.ONE, "value", BigInteger.ZERO), BigInteger.ONE);
        Assert.assertEquals(ProtonChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.ZERO), BigInteger.ZERO);
        Assert.assertEquals(ProtonChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.valueOf(-1L)),
                BigInteger.ZERO);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notLessThan(BigInteger.ZERO, "value", BigInteger.ONE));
    }

    @Test(groups = { "unit" })
    public void testNotLongerThan() {
        byte[] bytes;
        Assert.assertEquals(ProtonChecker.notLongerThan(bytes = null, "value", 0), bytes);
        Assert.assertEquals(ProtonChecker.notLongerThan(bytes = new byte[0], "value", 0), bytes);
        Assert.assertEquals(ProtonChecker.notLongerThan(bytes = new byte[1], "value", 1), bytes);

        Assert.assertThrows(IllegalArgumentException.class, () -> ProtonChecker.notLongerThan(null, null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notLongerThan(new byte[0], null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notLongerThan(new byte[2], null, 1));
    }

    @Test(groups = { "unit" })
    public void testNotWithDifferentLength() {
        byte[] bytes;
        Assert.assertEquals(ProtonChecker.notWithDifferentLength(bytes = null, "value", 0), bytes);
        Assert.assertEquals(ProtonChecker.notWithDifferentLength(bytes = new byte[0], "value", 0), bytes);
        Assert.assertEquals(ProtonChecker.notWithDifferentLength(bytes = new byte[1], "value", 1), bytes);

        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notWithDifferentLength((byte[]) null, null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notWithDifferentLength(new byte[0], null, -1));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonChecker.notWithDifferentLength(new byte[2], null, 1));
    }
}
