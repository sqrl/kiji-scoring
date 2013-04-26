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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
// All tests are disabled.
//import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.mapreduce.produce.ProducerContext;
import org.kiji.schema.EntityId;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.kiji.scoring.lib.AlwaysFreshen;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.lib.NeverFreshen;

/**
 * Benchmarking test for Freshness performance.
 */
public class IntegrationTestFreshnessBenchmark extends AbstractKijiIntegrationTest {
  private static final Logger LOG =
      LoggerFactory.getLogger(IntegrationTestFreshnessBenchmark.class);

  private static final class IncrementingProducer extends KijiProducer {
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "visits");
    }

    public String getOutputColumn() {
      return "info:visits";
    }

    public void produce(final KijiRowData kijiRowData, final ProducerContext producerContext)
        throws IOException {
      final Long old = kijiRowData.getMostRecentValue("info", "visits");
      producerContext.put(old + 1);
    }
  }

  private Kiji mKiji;
  private KijiTable mTable;
  private KijiTableWriter mWriter;
  private KijiTableReader mReader;
  private FreshKijiTableReader mFreshReader;
  private KijiFreshnessManager mManager;

  @Before
  public void setupIntegrationTestFreshnessBenchmark() throws IOException {
    mKiji = Kiji.Factory.open(getKijiURI());
    mKiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    mTable = mKiji.openTable("user");
    mWriter = mTable.openTableWriter();
    mReader = mTable.openTableReader();
    mManager = KijiFreshnessManager.create(mKiji);
    KijiFreshnessPolicy policy = new NeverFreshen();
    mManager.storePolicy(
        "user", "info:name", TestInternalFreshKijiTableReader.TestProducer.class, policy);
    mFreshReader = new InternalFreshKijiTableReader(mTable, 1000);
  }

  @After
  public void cleanupIntegrationTestFreshnessBenchmark() throws IOException {
    mFreshReader.close();
    mManager.removePolicy("user", "info:name");
    mReader.close();
    mWriter.close();
    mTable.release();
    mKiji.release();
  }

  private long getTime(
      KijiTableReader reader, EntityId eid, KijiDataRequest request) throws IOException {
    final long startTime = System.currentTimeMillis();
    reader.get(eid, request);
    return System.currentTimeMillis() - startTime;
  }

  private long max(List<Long> ls) {
    return Collections.max(ls);
  }

  private long median(List<Long> ls) {
    Collections.sort(ls);
    return ls.get(ls.size() / 2);
  }

  // Test Disabled
//  @Test
  public void testFresh() throws IOException {
    EntityId eid = mTable.getEntityId("foo");
    KijiDataRequest request = KijiDataRequest.create("info", "name");
    mWriter.put(eid, "info", "name", "foo-val");
    final ArrayList<Long> normalMetaTimes = Lists.newArrayList();
    final ArrayList<Long> freshMetaTimes = Lists.newArrayList();
    for (int times = 0; times < 10; times++) {
      final long normalStartTime = System.currentTimeMillis();
      final ArrayList<Long> normalTimes = Lists.newArrayList();
      for (int i = 0; i < 1000; i++) {
        normalTimes.add(getTime(mReader, eid, request));
      }
      final long normalEndTime = System.currentTimeMillis();
      final long freshStartTime = System.currentTimeMillis();
      final ArrayList<Long> freshTimes = Lists.newArrayList();
      for (int i = 0; i < 1000; i++) {
        freshTimes.add(getTime(mFreshReader, eid, request));
      }
      final long freshEndTime = System.currentTimeMillis();

      final long totalNormalTime = normalEndTime - normalStartTime;
      final long totalFreshTime = freshEndTime - freshStartTime;

      final long maxNormalTime = max(normalTimes);
      final long maxFreshTime = max(freshTimes);

      final long medianNormalTime = median(normalTimes);
      final long medianFreshTime = median(freshTimes);

      LOG.info("Normal first attempt number: {}", times);
      LOG.info("Total time for normal get(): {}", totalNormalTime);
      LOG.info("Total time for fresh get(): {}", totalFreshTime);
      LOG.info("Max time for normal get(): {}", maxNormalTime);
      LOG.info("Max time for fresh get(): {}", maxFreshTime);
      LOG.info("Median time for normal get(): {}", medianNormalTime);
      LOG.info("Median time for fresh get(): {}", medianFreshTime);

      normalMetaTimes.add(totalNormalTime);
      freshMetaTimes.add(totalFreshTime);
    }
    long normalTotal = 0L;
    for (Long time : normalMetaTimes) {
      normalTotal += time;
    }
    long freshTotal = 0L;
    for (Long time : freshMetaTimes) {
      freshTotal += time;
    }
    LOG.info("Average normal get() time over 10 sets of 10000: {}", normalTotal / 10000);
    LOG.info("Average fresh get() time over 10 sets of 10000: {}", freshTotal / 10000);
  }

  // Test Disabled
//  @Test
  public void testFreshReversed() throws IOException {
    EntityId eid = mTable.getEntityId("foo");
    KijiDataRequest request = KijiDataRequest.create("info", "name");
    mWriter.put(eid, "info", "name", "foo-val");
    final ArrayList<Long> normalMetaTimes = Lists.newArrayList();
    final ArrayList<Long> freshMetaTimes = Lists.newArrayList();
    for (int times = 0; times < 10; times++) {
      final long freshStartTime = System.currentTimeMillis();
      final ArrayList<Long> freshTimes = Lists.newArrayList();
      for (int i = 0; i < 1000; i++) {
        freshTimes.add(getTime(mFreshReader, eid, request));
      }
      final long freshEndTime = System.currentTimeMillis();
      final long normalStartTime = System.currentTimeMillis();
      final ArrayList<Long> normalTimes = Lists.newArrayList();
      for (int i = 0; i < 1000; i++) {
        normalTimes.add(getTime(mReader, eid, request));
      }
      final long normalEndTime = System.currentTimeMillis();

      final long totalNormalTime = normalEndTime - normalStartTime;
      final long totalFreshTime = freshEndTime - freshStartTime;

      final long maxNormalTime = max(normalTimes);
      final long maxFreshTime = max(freshTimes);

      final long medianNormalTime = median(normalTimes);
      final long medianFreshTime = median(freshTimes);

      LOG.info("Fresh first attempt number: {}", times);
      LOG.info("Total time for normal get(): {}", totalNormalTime);
      LOG.info("Total time for fresh get(): {}", totalFreshTime);
      LOG.info("Max time for normal get(): {}", maxNormalTime);
      LOG.info("Max time for fresh get(): {}", maxFreshTime);
      LOG.info("Median time for normal get(): {}", medianNormalTime);
      LOG.info("Median time for fresh get(): {}", medianFreshTime);

      normalMetaTimes.add(totalNormalTime);
      freshMetaTimes.add(totalFreshTime);
    }
    long normalTotal = 0L;
    for (Long time : normalMetaTimes) {
      normalTotal += time;
    }
    long freshTotal = 0L;
    for (Long time : freshMetaTimes) {
      freshTotal += time;
    }
    LOG.info("Average normal get() time over 10 sets of 10000: {}", normalTotal / 10000);
    LOG.info("Average fresh get() time over 10 sets of 10000: {}", freshTotal / 10000);
  }

// Test Disabled
//  @Test
  public void testIncrementingProducer() throws IOException {
    EntityId eid = mTable.getEntityId("foo");
    KijiDataRequest request = KijiDataRequest.create("info", "visits");
    mWriter.put(eid, "info", "visits", 0);
    final KijiFreshnessPolicy policy = new AlwaysFreshen();
    mManager.storePolicy("user", "info:visits", IncrementingProducer.class, policy);
    final FreshKijiTableReader freshReader = new InternalFreshKijiTableReader(mTable, 1000);

    for (int times = 0; times < 10; times++) {
      final ArrayList<Long> normalTimes = Lists.newArrayList();
      final ArrayList<Long> freshTimes = Lists.newArrayList();
      for (int i = 0; i < 1000; i++) {
        normalTimes.add(getTime(mReader, eid, request));
        freshTimes.add(getTime(freshReader, eid, request));
      }

      final long maxNormalTime = max(normalTimes);
      final long maxFreshTime = max(freshTimes);

      final long medianNormalTime = median(normalTimes);
      final long medianFreshTime = median(freshTimes);

      LOG.info("Normal first, incrementing producer attempt number: {}", times);
      LOG.info("Max time for normal get(): {}", maxNormalTime);
      LOG.info("Max time for fresh get(): {}", maxFreshTime);
      LOG.info("Median time for normal get(): {}", medianNormalTime);
      LOG.info("Median time for fresh get(): {}", medianFreshTime);

    }

    assertEquals(10000L, mReader.get(eid, request).getMostRecentValue("info", "visits"));
  }
}
