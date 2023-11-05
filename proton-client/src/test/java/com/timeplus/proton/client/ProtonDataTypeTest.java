package com.timeplus.proton.client;

import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonDataTypeTest {
    @Test(groups = { "unit" })
    public void testAlias() {
        for (String alias : ProtonDataType.allAliases) {
            Assert.assertTrue(ProtonDataType.isAlias(alias));
        }

        for (ProtonDataType t : ProtonDataType.values()) {
            Assert.assertFalse(ProtonDataType.isAlias(t.name()), t.name() + " should not be an alias");
        }
    }

    @Test(groups = { "unit" })
    public void testMapping() {
        for (ProtonDataType t : ProtonDataType.values()) {
            Assert.assertEquals(ProtonDataType.of(t.name()), t);
            if (!t.isCaseSensitive()) {
                Assert.assertEquals(ProtonDataType.of(t.name().toLowerCase()), t);
                Assert.assertEquals(ProtonDataType.of(t.name().toUpperCase()), t);
            }

            for (String alias : t.getAliases()) {
                Assert.assertEquals(ProtonDataType.of(alias), t);
                Assert.assertEquals(ProtonDataType.of(alias.toLowerCase()), t);
                Assert.assertEquals(ProtonDataType.of(alias.toUpperCase()), t);
            }
        }
    }

    @Test(groups = { "unit" })
    public void testMatch() {
        // List<String> matched = ProtonDataType.match("INT1");
        // Assert.assertEquals(matched.size(), 3);
        // Assert.assertEquals(matched.get(0), "INT1");
        // Assert.assertEquals(matched.get(1), "INT1 SIGNED");
        // Assert.assertEquals(matched.get(2), "INT1 UNSIGNED");

        List<String> matched = ProtonDataType.match("uint32");
        Assert.assertEquals(matched.size(), 1);
        Assert.assertEquals(matched.get(0), "uint32");
    }
}
