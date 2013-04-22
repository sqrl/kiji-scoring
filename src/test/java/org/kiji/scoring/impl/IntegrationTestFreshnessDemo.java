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

import org.junit.Test;
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
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ShelfLife;

/**
 * End to end demo of KijiScoring.
 */
public class IntegrationTestFreshnessDemo  extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestFreshnessDemo.class);

  private static final class DemoProducer extends KijiProducer {
    /** {@inheritDoc} */
    @Override
    public KijiDataRequest getDataRequest() {
      return KijiDataRequest.create("info", "visits");
    }

    /** {@inheritDoc} */
    @Override
    public String getOutputColumn() {
      return "info:visits";
    }

    /** {@inheritDoc} */
    @Override
    public void produce(final KijiRowData kijiRowData, final ProducerContext producerContext)
        throws IOException {
      final Long visits = kijiRowData.getMostRecentValue("info", "visits");
      producerContext.put(visits + 1);
    }
  }

  @Test
  public void freshnessDemo() throws IOException {
    // Get a Kiji instance.
    final Kiji kiji = Kiji.Factory.open(getKijiURI());
    // Create the "user" table.
    kiji.createTable(KijiTableLayouts.getLayout(KijiTableLayouts.COUNTER_TEST));
    // Get a table from the Kiji instance.
    final KijiTable table = kiji.openTable("user");
    // Get a KijiFreshnessManager for the Kiji instance.
    final KijiFreshnessManager manager = KijiFreshnessManager.create(kiji);
    // Create a ShelfLife freshness policy and load a 1 day shelf life duration.
    final KijiFreshnessPolicy policy = new ShelfLife(86400000);
    // Store the freshness policy in the meta table for the table "user" and column "info:visits"
    // using the ShelfLife freshness policy created above and the DemoProducer.
    manager.storePolicy("user", "info:visits", DemoProducer.class, policy);
    // Open a FreshKijiTableReader for the table with a timeout of 100 milliseconds.
    // Note: the FreshKijiTableReader must be opened after the freshness policy is registered.
    final FreshKijiTableReader freshReader = new InternalFreshKijiTableReader(table, 100);

    // Write an old value to the cell we plan to request with timestamp 1 and value 10.
    final EntityId eid = table.getEntityId("foo");
    final KijiTableWriter writer = table.openTableWriter();
    writer.put(eid, "info", "visits", 1L, 10L);
    writer.close();

    // Get a regular reader to confirm what's in the table.
    final KijiTableReader reader = table.openTableReader();
    // Create a data request for the desired cell and EntityId.
    final KijiDataRequest request = KijiDataRequest.create("info", "visits");
    final KijiRowData rowData = reader.get(eid, request);
    LOG.info("Most recent timestamp: {}", rowData.getTimestamps("info", "visits").first());
    LOG.info("Most recent value: {}", rowData.getMostRecentValue("info", "visits"));

    // Read from the table and get back a freshened value because 1L is more than a day ago.
    assertEquals(11L, freshReader.get(eid, request).getMostRecentValue("info", "visits"));
    // Read again and get back the same value because the DemoProducer wrote a new value.
    assertEquals(11L, freshReader.get(eid, request).getMostRecentValue("info", "visits"));

    // Cleanup
    freshReader.close();
    manager.close();
    table.release();
    kiji.release();
  }
}
