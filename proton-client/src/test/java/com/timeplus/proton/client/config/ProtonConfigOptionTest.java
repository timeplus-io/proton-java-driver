package com.timeplus.proton.client.config;

import java.io.Serializable;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.timeplus.proton.client.ProtonChecker;
import com.timeplus.proton.client.ProtonDataType;
import com.timeplus.proton.client.ProtonFormat;

public class ProtonConfigOptionTest {
    static enum ProtonTestOption implements ProtonOption {
        STR("string_option", "string", "string option"),
        STR0("string_option0", "string0", "string option without environment variable support"),
        STR1("string_option0", "string1",
                "string option without environment variable and system property support"),
        INT("integer_option", 2333, "integer option"),
        INT0("integer_option0", 23330, "integer option without environment variable support"),
        INT1("integer_option1", 23331,
                "integer option without environment variable and system property support"),
        BOOL("boolean_option", false, "boolean option"),
        BOOL0("boolean_option0", true, "boolean option without environment variable support"),
        BOOL1("boolean_option1", false,
                "boolean option without environment variable and system property support");

        private final String key;
        private final Serializable defaultValue;
        private final Class<? extends Serializable> clazz;
        private final String description;

        <T extends Serializable> ProtonTestOption(String key, T defaultValue, String description) {
            this.key = ProtonChecker.nonNull(key, "key");
            this.defaultValue = defaultValue;
            this.clazz = defaultValue.getClass();
            this.description = ProtonChecker.nonNull(description, "description");
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Serializable getDefaultValue() {
            return defaultValue;
        }

        @Override
        public Class<? extends Serializable> getValueType() {
            return clazz;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    @Test(groups = { "unit" })
    public void testFromString() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonOption.fromString(null, String.class));
        Assert.assertEquals(ProtonOption.fromString("", String.class), "");

        Assert.assertEquals(ProtonOption.fromString("", Boolean.class), Boolean.FALSE);
        Assert.assertEquals(ProtonOption.fromString("Yes", Boolean.class), Boolean.FALSE);
        Assert.assertEquals(ProtonOption.fromString("1", Boolean.class), Boolean.TRUE);
        Assert.assertEquals(ProtonOption.fromString("true", Boolean.class), Boolean.TRUE);
        Assert.assertEquals(ProtonOption.fromString("True", Boolean.class), Boolean.TRUE);

        Assert.assertEquals(ProtonOption.fromString("", Integer.class), Integer.valueOf(0));
        Assert.assertEquals(ProtonOption.fromString("0", Integer.class), Integer.valueOf(0));
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonOption.fromString(null, Integer.class));

        Assert.assertEquals(ProtonOption.fromString("0.1", Float.class), Float.valueOf(0.1F));
        Assert.assertEquals(ProtonOption.fromString("NaN", Float.class), Float.valueOf(Float.NaN));

        Assert.assertEquals(ProtonOption.fromString("map", ProtonDataType.class),
                ProtonDataType.map);
        // Assert.assertEquals(ProtonOption.fromString("RowBinary", ProtonFormat.class),
        //        ProtonFormat.RowBinary);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ProtonOption.fromString("NonExistFormat", ProtonFormat.class));
    }

    @Test(groups = { "unit" })
    public void testGetEffectiveDefaultValue() {
        // environment variables are set in pom.xml
        Assert.assertEquals(ProtonTestOption.STR.getEffectiveDefaultValue(),
                ProtonTestOption.STR.getDefaultValueFromEnvVar().get());
        Assert.assertEquals(ProtonTestOption.INT.getEffectiveDefaultValue(),
                Integer.parseInt(ProtonTestOption.INT.getDefaultValueFromEnvVar().get()));
        Assert.assertEquals(ProtonTestOption.BOOL.getEffectiveDefaultValue(),
                Boolean.valueOf(ProtonTestOption.BOOL.getDefaultValueFromEnvVar().get()));

        String sv = "system.property";
        int iv = 12345;
        boolean bv = true;
        System.setProperty(ProtonTestOption.STR0.getPrefix().toLowerCase() + "_"
                + ProtonTestOption.STR0.name().toLowerCase(), sv);
        System.setProperty(ProtonTestOption.INT0.getPrefix().toLowerCase() + "_"
                + ProtonTestOption.INT0.name().toLowerCase(), String.valueOf(iv));
        System.setProperty(ProtonTestOption.BOOL0.getPrefix().toLowerCase() + "_"
                + ProtonTestOption.BOOL0.name().toLowerCase(), String.valueOf(bv));

        Assert.assertEquals(ProtonTestOption.STR0.getEffectiveDefaultValue(), sv);
        Assert.assertEquals(ProtonTestOption.INT0.getEffectiveDefaultValue(), iv);
        Assert.assertEquals(ProtonTestOption.BOOL0.getEffectiveDefaultValue(), bv);

        Assert.assertEquals(ProtonTestOption.STR1.getEffectiveDefaultValue(),
                ProtonTestOption.STR1.getDefaultValue());
        Assert.assertEquals(ProtonTestOption.INT1.getEffectiveDefaultValue(),
                ProtonTestOption.INT1.getDefaultValue());
        Assert.assertEquals(ProtonTestOption.BOOL1.getEffectiveDefaultValue(),
                ProtonTestOption.BOOL1.getDefaultValue());
    }
}
