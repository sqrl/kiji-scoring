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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import com.google.common.collect.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.*;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.KijiFreshnessPolicyRecord;

@ApiAudience.Private
public class HBaseFreshKijiTableReader implements FreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFreshKijiTableReader.class);

  /** The kiji table instance. */
  private final KijiTable mTable;

  /** Default reader to which to delegate reads. */
  private KijiTableReader mReader;

  /** Map from column names to freshness policies and their required state. */
  private Map<String, KijiFreshnessPolicyRecord> mPolicyRecords;

  /**
   * Creates a new <code>HBaseFreshKijiTableReader</code> instance that sends read requests
   * to an HBase table and performs freshening on the returned data.
   */
  public HBaseFreshKijiTableReader(KijiTable table) throws IOException {
    mTable = table;
    final KijiFreshnessManager manager = new KijiFreshnessManager(table.getKiji());
    // opening a reader retains the table, so we do not need to call retain manually.
    mReader = mTable.openTableReader();
    try{
      final KijiMetaTable metaTable = mTable.getKiji().getMetaTable();
      final Set<String> keySet = metaTable.keySet(mTable.getName());
      mPolicyRecords = new HashMap<String, KijiFreshnessPolicyRecord>();
      // For all keys in the metatable, if those keys are freshness policy entries, cache them locally.
      for (String key: keySet) {
        if (key.startsWith("kiji.scoring.fresh.")) {
          final String columnName = key.substring(19);
          // TODO: convert this byte[] to an avro record
          mPolicyRecords.put(columnName, manager.retrievePolicy(table.getName(), columnName));
        }
      }
    } catch (IOException ioe) {
      // TODO something
    }
  }

  /**
   * Gets all freshness policies from the local cache necessary to validate a given data request.
   *
   * @param dataRequest the data request for which to find freshness policies.
   * @return A map from column name to KijiFreshnessPolicy.
   */
  private Map<String, KijiFreshnessPolicy> getPolicies(KijiDataRequest dataRequest) {
    final Collection<Column> columns = dataRequest.getColumns();
    Map<String, KijiFreshnessPolicy> policies = new HashMap<String, KijiFreshnessPolicy>();
    for (Column column: columns) {
      final KijiFreshnessPolicyRecord record = mPolicyRecords.get(column.getName());
      if (record != null) {
        // Instantiate and initialize the policies.
        KijiFreshnessPolicy policy;
        try {
          policy =
              (KijiFreshnessPolicy) ReflectionUtils.newInstance(Class.forName(record.getFreshnessPolicyClass()), null);
        } catch (ClassNotFoundException cnfe) {
          throw new RuntimeException(String.format(
              "Freshness Policy class %s was not found on the classpath",
              record.getFreshnessPolicyClass()));
        }
        policy.load(record.getFreshnessPolicyState());
        // Add the policy to the list of policies applicable to this data request.
        policies.put(column.getName(), policy);
      }
    }
    return policies;
  }

  /**
   * Executes isFresh on all freshness policies according to their various data requests.
   * Unfinished because the roadmap changed and this method is no longer useful.
   *
  private Map<String, Boolean> checkFreshness(
      Map<String, KijiFreshnessPolicy> policies,
      KijiDataRequest dataRequest) {
    Map<String, Boolean> freshness = new HashMap();
    Map<String, KijiFreshnessPolicy> deferred = new HashMap();
    for (Map.Entry<String, KijiFreshnessPolicy> entry: policies) {
      if (entry.getValue().shouldUseClientDataRequest()) {
        // defer execution and run this later along with other defered isFresh calls against the
        // user data request
        deferred.put(entry.getKey(), entry.getValue());
      } else {
        final boolean fresh =
          entry.getValue().isFresh(mReader.get(entry.getValue().getDataRequest));
        freshness.put(entry.getKey(), fresh);
      }
    }
    final KijiRowData clientData = mReader.get(eid, dataRequest);
    for (Map.Entry<String, KijiFreshnesPolicy> entry: deferred) {
      freshness.put(entry.getKey(), entry.getValue().isFresh(clientData));
    }
  }*/

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(final EntityId eid, final KijiDataRequest dataRequest) throws IOException {
    // TODO: use helper methods to separate workflow elements
    // One plan:
    // 1) get the freshness policy(ies) from the cache
    // 2) check if the freshness policy uses the client data request
    // 3) issue the appropriate data request
    // 4) call freshnessPolicy.isFresh()
    // 5) if (isFresh && shouldUseClientDataRequest) return to user
    //    if (isFresh && !shouldUse) send the user request to the table and return
    //    if (!isFresh) run producer then send user request to table and return
    // Another plan:
    // 1) get the freshness policies from the cache
    // 2) branch threads for each policy
    // 3) each thread checks for freshness
    // 4) each thread conditionally runs a producer
    // 5) all threads finish or the timeout occurs, ask each thread for shouldReread(),
    //      if any return true, read from the table and return

    Map<String, KijiFreshnessPolicy> policies = getPolicies(dataRequest);
    // If there are no freshness policies attached to the requested columns, return the requested
    // data.
    if (policies.size() == 0) {
      return mReader.get(eid, dataRequest);
    }

    // 1) check if any freshness policy uses the client data request
    // 2) if one does, start a future to get that data
    // 3) Start a future for each freshness policy that requires the client data.
    // 4) start a future for each freshness policy that uses its own data request
    //    all futures except the first return a Boolean for whether a reread is required.  If any
    //    future has not returned when timeout occurs, assume reread.

    final ExecutorService executor = FreshenerThreadPool.getInstance().get();
    final Map<String, KijiFreshnessPolicy> usesClientDataRequest =
        new HashMap<String, KijiFreshnessPolicy>();
    final Map<String, KijiFreshnessPolicy> usesOwnDataRequest =
        new HashMap<String, KijiFreshnessPolicy>();
    for (String key: policies.keySet()) {
      if (policies.get(key).shouldUseClientDataRequest()) {
        usesClientDataRequest.put(key, policies.get(key));
      } else {
        usesOwnDataRequest.put(key, policies.get(key));
      }
    }
    final Future<KijiRowData> clientData = (usesClientDastaRequest.size() != 0) ?
        executor.submit(new Callable<KijiRowData>() {
          public KijiRowData call() throws IOException {
            return mReader.get(eid, dataRequest);
          }
        }) : null;
    final List<Future<Boolean>> futures = Lists.newArrayList();
    if (usesClientDataRequest.size() != 0) {
      for (final String key: usesClientDataRequest.keySet()) {
        final Future<Boolean> requiresReread = executor.submit(new Callable<Boolean>() {
          public Boolean call() {
            KijiRowData rowData;
            try {
              rowData = clientData.get();
            } catch (InterruptedException ie) {
              // TODO: Figure out what to actually do here.
              throw new RuntimeException("interrupted");
            } catch (ExecutionException ee) {
              throw new RuntimeException("execution error");
            }
            final boolean isFresh = usesClientDataRequest.get(key).isFresh(rowData);
            if (isFresh) {
              // If isFresh, return false to indicate a reread is not necessary.
              return Boolean.FALSE;
            } else {
              KijiProducer producer;
              try {
                producer = (KijiProducer) ReflectionUtils.newInstance(
                    Class.forName(mPolicyRecords.get(key).getProducerClass()), null);
              } catch (ClassNotFoundException cnfe) {
                throw new RuntimeException(String.format(
                    "Producer class %s was not found on the classpath",
                    mPolicyRecords.get(key).getFreshnessPolicyClass()));
              }
              // TODO: Does the producer need to be initialized?
              // TODO: add the context
              producer.produce(rowData, CONTEXT);
              // If a producer runs, return true to indicate a reread is necessary.
              return Boolean.TRUE;

            }
          }
        });
        futures.add(requiresReread);
      }
    }
    for (final String key: usesOwnDataRequest.keySet()) {
      final Future<Boolean> requiresReread = executor.submit(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          final KijiRowData rowData =
              mReader.get(eid, usesOwnDataRequest.get(key).getDataRequest());
          final boolean isFresh = usesOwnDataRequest.get(key).isFresh(rowData);
          if (isFresh) {
            // If isFresh, return false to indicate a reread is not necessary.
            return Boolean.FALSE;
          } else {
            KijiProducer producer;
            try {
              producer = (KijiProducer) ReflectionUtils.newInstance(
                Class.forName(mPolicyRecords.get(key).getProducerClass()), null);
            } catch (ClassNotFoundException cnfe) {
              throw new RuntimeException(String.format(
                  "Producer class %s was not found on the classpath",
                  mPolicyRecords.get(key).getFreshnessPolicyClass()));
            }
            // TODO: Does the producer need to be initialized?
            // TODO: add the context
            producer.produce(rowData, CONTEXT);
            // If a producer runs, return true to indicate a reread is necessary.
            return Boolean.TRUE;
          }
        }
      });
      futures.add(requiresReread);
    }


    final Future<Boolean> superFuture = executor.submit(new Callable<Boolean>() {
      public Boolean call() throws InterruptedException, ExecutionException{
        boolean retVal = false;
        for (Future<Boolean> future: futures) {
          // block on completion of each future and update the return value to be true if any
          // future returns true.
          retVal = future.get() || retVal;
        }
        return retVal;
      }
    });

    // TODO: setup timeout
    final long timeOut = 1000;
    try {
      if (superFuture.get(timeOut, TimeUnit.MILLISECONDS)) {
        return mReader.get(eid, dataRequest);
      } else {
        if (clientData != null) {
          return clientData.get();
        } else {
          return mReader.get(eid, dataRequest);
        }
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException();
      //TODO: handle exceptions
    } catch (ExecutionException ee) {
      throw new RuntimeException();
    } catch (TimeoutException te) {
      return mReader.get(eid, dataRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(List<EntityId> eids, KijiDataRequest dataRequest) {
    // TODO: this
    return null;
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(KijiDataRequest dataRequest) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      KijiDataRequest dataRequest, KijiScannerOptions kijiScannerOptions) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    ResourceUtils.releaseOrLog(mTable);
  }
}
