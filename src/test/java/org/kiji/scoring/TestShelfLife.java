package org.kiji.scoring;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.scoring.impl.HBaseFreshKijiTableReader;
import org.kiji.scoring.impl.PolicyContext;

/**
 * Test the behavior of the stock ShelfLife KijiFreshnessPolicy.
 */
public class TestShelfLife {
  private static final Logger LOG = LoggerFactory.getLogger(TestNewerThan.class);

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;
  private FreshKijiTableReader mFreshReader;

  @Before
  public void setupTestBulkImporter() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(5L, "foo-val")
                    .withQualifier("visits").withValue(1L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("name").withValue(1L, "bar-val")
                    .withQualifier("visits").withValue(1L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
    mFreshReader = new HBaseFreshKijiTableReader(mTable, 1000);
  }

  @After
  public void teardownTestBulkImporter() throws Exception {
    mReader.close();
    mFreshReader.close();
    mTable.release();
  }

  @Test
  public void testIsFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final KijiRowData rowData = mReader.get(eid, request);
    final PolicyContext context =
        new PolicyContext(request, new KijiColumnName("info", "name"), mKiji.getConf());
    final ShelfLife policy = new ShelfLife();
    policy.load(String.valueOf(Long.MAX_VALUE));

    assertTrue(policy.isFresh(rowData, context));

    policy.load(String.valueOf(Long.MIN_VALUE));
    assertFalse(policy.isFresh(rowData, context));
  }
}
