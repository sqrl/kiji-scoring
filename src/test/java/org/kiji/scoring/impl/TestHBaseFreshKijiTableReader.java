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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.mapreduce.produce.impl.KijiProducers;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.util.InstanceBuilder;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.AlwaysFresh;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ShelfLife;
import org.kiji.scoring.impl.HBaseFreshKijiTableReader;

/**
 * Created with IntelliJ IDEA. User: aaron Date: 4/15/13 Time: 4:04 PM To change this template use
 * File | Settings | File Templates.
 */
public class TestHBaseFreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseFreshKijiTableReader.class);

  /** Dummy &lt;? extends KijiProducer&gt; class for testing */
  private final static class TestProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
    public String getOutputColumn() {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
    public void produce(
        final KijiRowData kijiRowData, final ProducerContext producerContext) throws IOException {}
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
  private HBaseFreshKijiTableReader mFreshReader;

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
    mFreshReader = new HBaseFreshKijiTableReader(mTable, 1000);
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
    // TODO test this when the manager is ready.
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
        "org.kiji.scoring.impl.TestHBaseFreshKijiTableReader$TestProducer").getClass());
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
    usesClientDataRequest.put(new KijiColumnName("info", "name"), new AlwaysFresh());
    usesOwnDataRequest.put(new KijiColumnName("info", "name"), new TestAlwaysFresh());

    // Get a list of two futures, all are always fresh, so should return false to indicate no
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
    //TODO: this
  }

  @Test
  public void testGetStale() throws Exception {
    //TODO: this
  }
}
