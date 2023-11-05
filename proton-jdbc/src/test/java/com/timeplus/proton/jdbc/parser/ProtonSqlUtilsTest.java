package com.timeplus.proton.jdbc.parser;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonSqlUtilsTest {
    @Test(groups = "unit")
    public void testIsQuote() {
        Assert.assertFalse(ProtonSqlUtils.isQuote('\0'));

        Assert.assertTrue(ProtonSqlUtils.isQuote('"'));
        Assert.assertTrue(ProtonSqlUtils.isQuote('\''));
        Assert.assertTrue(ProtonSqlUtils.isQuote('`'));
    }

    @Test(groups = "unit")
    public void testEscape() {
        char[] quotes = new char[] { '"', '\'', '`' };
        String str;
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(ProtonSqlUtils.escape(str = null, quote), str);
            Assert.assertEquals(ProtonSqlUtils.escape(str = "", quote),
                    String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(ProtonSqlUtils.escape(str = "\\any \\string\\", quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\" + String.valueOf(quote));
            Assert.assertEquals(
                    ProtonSqlUtils.escape(str = String.valueOf(quote) + "any " + String.valueOf(quote) + "string",
                            quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "any \\" + String.valueOf(quote) + "string"
                            + String.valueOf(quote));
            Assert.assertEquals(ProtonSqlUtils.escape(str = "\\any \\string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\\\any \\\\string\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
            Assert.assertEquals(
                    ProtonSqlUtils.escape(str = String.valueOf(quote) + "\\any \\" + String.valueOf(quote)
                            + "string\\" + String.valueOf(quote), quote),
                    String.valueOf(quote) + "\\" + String.valueOf(quote) + "\\\\any \\\\\\" + String.valueOf(quote)
                            + "string" + "\\\\\\" + String.valueOf(quote) + String.valueOf(quote));
        }
    }

    @Test(groups = "unit")
    public void testUnescape() {
        String str;
        Assert.assertEquals(ProtonSqlUtils.unescape(str = null), str);
        Assert.assertEquals(ProtonSqlUtils.unescape(str = ""), str);
        Assert.assertEquals(ProtonSqlUtils.unescape(str = "\\any \\string\\"), str);
        char[] quotes = new char[] { '"', '\'', '`' };
        for (int i = 0; i < quotes.length; i++) {
            char quote = quotes[i];
            Assert.assertEquals(ProtonSqlUtils.unescape(str = String.valueOf(quote) + "1" + String.valueOf(quote)),
                    "1");
            Assert.assertEquals(ProtonSqlUtils.unescape(str = String.valueOf(quote) + "\\any \\string\\"), str);
            Assert.assertEquals(ProtonSqlUtils.unescape(str = "\\any \\string\\" + String.valueOf(quote)), str);
            Assert.assertEquals(
                    ProtonSqlUtils.unescape(str = String.valueOf(quote) + "\\any" + String.valueOf(quote)
                            + String.valueOf(quote) + "\\string\\" + String.valueOf(quote)),
                    "any" + String.valueOf(quote) + "string\\");
            Assert.assertEquals(
                    ProtonSqlUtils.unescape(str = String.valueOf(quote) + String.valueOf(quote) + "\\"
                            + String.valueOf(quote) + "any" + String.valueOf(quote) + String.valueOf(quote)
                            + "\\string\\" + String.valueOf(quote)),
                    String.valueOf(quote) + String.valueOf(quote) + "any" + String.valueOf(quote) + "string\\");
        }
    }
}
