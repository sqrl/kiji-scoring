/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.scoring.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.AlwaysFreshen;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.NeverFreshen;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.ShelfLife;

/**
 * Tests InternalFreshKijiTableReader.
 */
public class TestInternalFreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestInternalFreshKijiTableReader.class);

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  public static final class TestProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "name");
    }
    public String getOutputColumn() {
      return "info:name";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      producerContext.put("new-val");
    }
  }

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  public static final class TestTimeoutProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "name");
    }
    public String getOutputColumn() {
      return "info:name";
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException ie) {
        throw new RuntimeException("TestProducer thread interrupted during produce sleep.");
      }
      producerContext.put("new-val");
    }
  }

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  private static final class TestAlwaysFresh implements KijiFreshnessPolicy {
    public boolean isFresh(final KijiRowData rowData, final PolicyContext policyContext) {
      return true;
    }
    public boolean shouldUseClientDataRequest() {
      return false;
    }
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "name");
    }
    public String store() {
      return null;
    }
    public void load(final String policyState) {}
  }

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableReader mReader;
  private InternalFreshKijiTableReader mFreshReader;

  @Before
  public void setupEnvironment() throws Exception {
    // Get the test table layouts.
    final KijiTableLayout layout = KijiTableLayout.newLayout(
        KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));

    // Populate the environment.
    mKiji = new InstanceBuilder()
        .withTable("user", layout)
            .withRow("foo")
                .withFamily("info")
                    .withQualifier("name").withValue(5L, "foo-val")
                    .withQualifier("visits").withValue(5L, 42L)
            .withRow("bar")
                .withFamily("info")
                    .withQualifier("name").withValue(5L, "bar-val")
                    .withQualifier("visits").withValue(5L, 100L)
        .build();

    // Fill local variables.
    mTable = mKiji.openTable("user");
    mReader = mTable.openTableReader();
    mFreshReader = new InternalFreshKijiTableReader(mTable, 1000);
  }

  @After
  public void cleanupEnvironment() throws Exception {
    ResourceUtils.closeOrLog(mFreshReader);
    ResourceUtils.closeOrLog(mReader);
    ResourceUtils.releaseOrLog(mTable);
    ResourceUtils.releaseOrLog(mKiji);
  }

  @Test
  public void testMetaTable() throws Exception {
    final KijiMetaTable metaTable = mKiji.getMetaTable();
    final byte[] original = Bytes.toBytes("original");
    metaTable.putValue("user", "test.kiji.metatable.key", original);

    // Ensure that the InstanceBuilder metatable behaves as expected.
    assertEquals(Bytes.toString(metaTable.getValue("user", "test.kiji.metatable.key")), "original");
  }

  @Test
  public void testPolicyForName() throws Exception {
    final Class<? extends KijiFreshnessPolicy> expected = ShelfLife.class;
    assertEquals(expected, mFreshReader.policyForName("org.kiji.scoring.ShelfLife").getClass());
  }

  @Test
  public void testGetPolicies() throws Exception {
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "visits").add("info", "name");
    final KijiDataRequest completeRequest = builder.build();

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy("user", "info:name", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader = new InternalFreshKijiTableReader(mTable, 1000);
    assertEquals(2, freshReader.getPolicies(completeRequest).size());
    assertEquals(AlwaysFreshen.class,
        freshReader.getPolicies(completeRequest).get(new KijiColumnName("info", "name"))
        .getClass());
    assertEquals(NeverFreshen.class,
        freshReader.getPolicies(completeRequest).get(new KijiColumnName("info", "visits"))
        .getClass());
  }

  @Test
  public void testGetClientData() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("info", "name"),
        mFreshReader.getClientData(eid, request, true).get().getMostRecentValue("info", "name")
    );
  }

  @Test
  public void testProducerForName() throws Exception {
    final Class<? extends KijiProducer> expected = TestProducer.class;
    assertEquals(expected, mFreshReader.producerForName(
        "org.kiji.scoring.impl.TestInternalFreshKijiTableReader$TestProducer").getClass());
  }

  @Test
  public void testGetFutures() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");
    final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    final Future<KijiRowData> clientData = mFreshReader.getClientData(eid, request, true);

    // Get an empty list of futures.
    final List<Future<Boolean>> actual =
        mFreshReader.getFutures(
        usesClientDataRequest, usesOwnDataRequest, clientData, eid, request);
    assertEquals(0, actual.size());

    // Add two freshness policies.
    usesClientDataRequest.put(new KijiColumnName("info", "name"), new NeverFreshen());
    usesOwnDataRequest.put(new KijiColumnName("info", "name"), new TestAlwaysFresh());

    // Get a list of two futures, both are always fresh, so should return false to indicate no
    // reread.
    final List<Future<Boolean>> actual2 =
        mFreshReader.getFutures(
            usesClientDataRequest, usesOwnDataRequest, clientData, eid, request);
    assertEquals(2, actual2.size());
    for (Future<Boolean> future : actual2) {
      assertEquals(false, future.get());
    }
  }

  @Test
  public void testGetNoPolicy() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Getting a column with no freshness policy attached should behave the same as a normal get.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("info", "name"),
        mFreshReader.get(eid, request).getMostRecentValue("info", "name"));
  }

  @Test
  public void testGetFresh() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy("user", "info:name", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies. Allow 10 seconds so it is very
    // unlikely to timeout.
    final InternalFreshKijiTableReader freshReader = new InternalFreshKijiTableReader(mTable, 10000);

    // freshReader should return the same as regular reader because the data is fresh.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("info", "visits"),
        freshReader.get(eid, request).getMostRecentValue("info", "visits"));
    // Value should be unchanged.
    assertEquals(42L, mReader.get(eid, request).getMostRecentValue("info", "visits"));
  }

  @Test
  public void testGetStale() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy("user", "info:name", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader = new InternalFreshKijiTableReader(mTable, 10000);

    // freshReader should return different from regular reader because the data is stale.
    assertFalse(
        mReader.get(eid, request).getMostRecentValue("info", "name").equals(
        freshReader.get(eid, request).getMostRecentValue("info", "name")));
    // The new value should have been written.
    assertEquals(
        "new-val", mReader.get(eid, request).getMostRecentValue("info", "name").toString());
  }

  @Test
  public void testBulkGet() throws Exception {
    final EntityId eidFoo = mTable.getEntityId("foo");
    final EntityId eidBar = mTable.getEntityId("bar");
    final KijiDataRequest freshRequest = KijiDataRequest.create("info", "visits");
    final KijiDataRequest staleRequest = KijiDataRequest.create("info", "name");
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef().add("info", "visits").add("info", "name");
    final KijiDataRequest completeRequest = builder.build();

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy("user", "info:name", TestProducer.class, new AlwaysFreshen());
    manager.storePolicy("user", "info:visits", TestProducer.class, new NeverFreshen());

    // Open a new reader to pull in the new freshness policies.
    final InternalFreshKijiTableReader freshReader = new InternalFreshKijiTableReader(mTable, 10000);

    // Get the old data for comparison
    final List<KijiRowData> oldData =
        mReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), completeRequest);

    // Run a request which should return fresh.  nothing should be written.
    final List<KijiRowData> newData =
        freshReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), freshRequest);
    assertEquals(
        oldData.get(0).getMostRecentValue("info", "visits"),
        newData.get(0).getMostRecentValue("info", "visits"));
    assertEquals(
        oldData.get(1).getMostRecentValue("info", "visits"),
        newData.get(1).getMostRecentValue("info", "visits"));

    // Run a request which should return stale.  data should be written.
    final List<KijiRowData> newData2 =
        freshReader.bulkGet(Lists.newArrayList(eidFoo, eidBar), staleRequest);
    assertFalse(
        oldData.get(0).getMostRecentValue("info", "name").equals(
        newData2.get(0).getMostRecentValue("info", "name")));
    assertEquals("new-val", newData2.get(0).getMostRecentValue("info", "name").toString());
    assertFalse(
        oldData.get(1).getMostRecentValue("info", "name").equals(
        newData2.get(1).getMostRecentValue("info", "name")));
    assertEquals("new-val", newData2.get(1).getMostRecentValue("info", "name").toString());
  }

  @Test
  public void testGetStaleTimeout() throws Exception {
    final EntityId eid = mTable.getEntityId("foo");
    final KijiDataRequest request = KijiDataRequest.create("info", "name");

    // Create a KijiFreshnessManager and register some freshness policies.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(mKiji);
    manager.storePolicy("user", "info:name", TestTimeoutProducer.class, new AlwaysFreshen());

    mFreshReader = new InternalFreshKijiTableReader(mTable, 500);

    // The fresh reader should return stale data after a timeout.
    assertEquals(
        mReader.get(eid, request).getMostRecentValue("info", "name"),
        mFreshReader.get(eid, request).getMostRecentValue("info", "name"));

    // Wait for the producer to finish then try again.
    Thread.sleep(1000L);
    assertEquals("new-val",
        mReader.get(eid, request).getMostRecentValue("info", "name").toString());
  }
}
