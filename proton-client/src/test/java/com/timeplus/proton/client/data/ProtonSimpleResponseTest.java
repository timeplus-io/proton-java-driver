package com.timeplus.proton.client.data;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import com.timeplus.proton.client.ProtonColumn;
import com.timeplus.proton.client.ProtonConfig;
import com.timeplus.proton.client.ProtonRecord;
import com.timeplus.proton.client.ProtonResponse;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ProtonSimpleResponseTest {
    private final ProtonConfig config = new ProtonConfig();

    @Test(groups = { "unit" })
    public void testNullOrEmptyInput() {
        ProtonResponse nullResp = ProtonSimpleResponse.of(config, (List<ProtonColumn>) null, null);
        Assert.assertEquals(nullResp.getColumns(), Collections.emptyList());
        Assert.assertTrue(((List<?>) nullResp.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> nullResp.firstRecord());

        ProtonResponse emptyResp1 = ProtonSimpleResponse.of(config, ProtonColumn.parse("a String"), null);
        Assert.assertEquals(emptyResp1.getColumns(), ProtonColumn.parse("a String"));
        Assert.assertTrue(((List<?>) emptyResp1.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> emptyResp1.firstRecord());

        ProtonResponse emptyResp2 = ProtonSimpleResponse.of(config, ProtonColumn.parse("a String"),
                new Object[0][]);
        Assert.assertEquals(emptyResp2.getColumns(), ProtonColumn.parse("a String"));
        Assert.assertTrue(((List<?>) emptyResp2.records()).isEmpty());
        Assert.assertThrows(NoSuchElementException.class, () -> emptyResp2.firstRecord());
    }

    @Test(groups = { "unit" })
    public void testMismatchedColumnsAndRecords() {
        ProtonResponse resp = ProtonSimpleResponse
                .of(config, ProtonColumn.parse("a nullable(string), b uint8, c array(uint32)"),
                        new Object[][] { new Object[0], null, new Object[] { 's' },
                                new Object[] { null, null, null, null },
                                new Object[] { "123", 1, new int[] { 3, 2, 1 } } });
        int i = 0;
        for (ProtonRecord r : resp.records()) {
            switch (i) {
                case 0:
                case 1:
                case 3:
                    Assert.assertNull(r.getValue(0).asObject());
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), new long[0]);
                    break;
                case 2:
                    Assert.assertEquals(r.getValue(0).asObject(), "s");
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), new long[0]);
                    break;
                case 4:
                    Assert.assertEquals(r.getValue(0).asObject(), "123");
                    Assert.assertEquals(r.getValue(1).asObject(), (short) 1);
                    Assert.assertEquals(r.getValue(2).asObject(), new long[] { 3L, 2L, 1L });
                    break;
                default:
                    Assert.fail("Should not fail");
                    break;
            }
            i++;
        }
    }

    @Test(groups = { "unit" })
    public void testFirstRecord() {
        ProtonResponse resp = ProtonSimpleResponse.of(config,
                ProtonColumn.parse("a nullable(string), b uint8, c string"),
                new Object[][] { new Object[] { "aaa", 2, "ccc" }, null });
        ProtonRecord record = resp.firstRecord();
        Assert.assertEquals(record.getValue("A"), ProtonStringValue.of("aaa"));
        Assert.assertEquals(record.getValue("B"), ProtonShortValue.of(2));
        Assert.assertEquals(record.getValue("C"), ProtonStringValue.of("ccc"));

        ProtonRecord sameRecord = resp.firstRecord();
        Assert.assertTrue(record == sameRecord);
    }

    @Test(groups = { "unit" })
    public void testRecords() {
        ProtonResponse resp = ProtonSimpleResponse.of(config,
                ProtonColumn.parse("a nullable(string), b uint8, c string"),
                new Object[][] { new Object[] { "aaa1", null, "ccc1" }, new Object[] { "aaa2", 2, "ccc2" },
                        new Object[] { null, 3L, null } });
        int i = 0;
        for (ProtonRecord r : resp.records()) {
            switch (i) {
                case 0:
                    Assert.assertEquals(r.getValue(0).asObject(), "aaa1");
                    Assert.assertNull(r.getValue(1).asObject());
                    Assert.assertEquals(r.getValue(2).asObject(), "ccc1");
                    break;
                case 1:
                    Assert.assertEquals(r.getValue("a").asObject(), "aaa2");
                    Assert.assertEquals(r.getValue("B").asObject(), (short) 2);
                    Assert.assertEquals(r.getValue("c").asObject(), "ccc2");
                    break;
                case 2:
                    Assert.assertNull(r.getValue(0).asObject());
                    Assert.assertEquals(r.getValue(1).asObject(), (short) 3);
                    Assert.assertNull(r.getValue(0).asObject());
                    break;
                default:
                    Assert.fail("Should not fail");
                    break;
            }
            i++;
        }
    }
}
