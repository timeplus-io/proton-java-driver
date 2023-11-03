package com.timeplus.proton.client.data;

import com.timeplus.proton.client.BaseProtonValueTest;
import org.testng.annotations.Test;
import com.timeplus.proton.client.ProtonDataType;

public class ProtonEnumValueTest extends BaseProtonValueTest {
    @Test(groups = { "unit" })
    public void testCopy() {
        sameValue(ProtonEnumValue.ofNull(ProtonDataType.class),
                ProtonEnumValue.ofNull(ProtonDataType.class), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.string),
                ProtonEnumValue.of(ProtonDataType.string),
                3, 9, Object.class, ProtonDataType.class, Object.class, Object.class);
        ProtonEnumValue v = ProtonEnumValue.of(ProtonDataType.array);
        sameValue(v, v.copy(), 3, 9, Object.class, ProtonDataType.class, Object.class, Object.class);
    }

    @Test(groups = { "unit" })
    public void testUpdate() {
        sameValue(ProtonEnumValue.ofNull(ProtonDataType.class),
                ProtonEnumValue.ofNull(ProtonDataType.class).update(ProtonDataType.date32).set(true, 0), 3,
                9,
                Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.date32),
                ProtonEnumValue.ofNull(ProtonDataType.class).update(ProtonDataType.date32), 3, 9,
                Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.date32),
                ProtonEnumValue.of(ProtonDataType.array).update(ProtonDataType.date32), 3, 9,
                Object.class,
                ProtonDataType.class, Object.class, Object.class);

        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update(false), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update(new boolean[] { false }), 3, 9,
                Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update('\0'), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update((byte) 0), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update((short) 0), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update(0), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update(0L), 3, 9, Object.class,
                ProtonDataType.class, Object.class, Object.class);
        sameValue(ProtonEnumValue.of(ProtonDataType.interval_year),
                ProtonEnumValue.of(ProtonDataType.string).update("interval_year"), 3, 9,
                Object.class,
                ProtonDataType.class, Object.class, Object.class);
    }
}
