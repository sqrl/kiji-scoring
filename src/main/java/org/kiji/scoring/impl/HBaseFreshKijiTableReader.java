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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.produce.KijiProducer;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiMetaTable;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.KijiFreshnessPolicyRecord;

@ApiAudience.Private
public final class HBaseFreshKijiTableReader implements FreshKijiTableReader {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFreshKijiTableReader.class);

  /** The kiji table instance. */
  private final KijiTable mTable;

  /** Default reader to which to delegate reads. */
  private KijiTableReader mReader;

  /** Freshener thread pool executor service. */
  private final ExecutorService mExecutor;

  /** Timeout duration for get requests. */
  private final int mTimeout;

  /** Map from column names to freshness policy records and their required state. */
  private Map<KijiColumnName, KijiFreshnessPolicyRecord> mPolicyRecords;

  /** Cache of freshness policy instances indexed by column names. */
  private Map<KijiColumnName, KijiFreshnessPolicy> mPolicyCache;

  /**
   * Creates a new <code>HBaseFreshKijiTableReader</code> instance that sends read requests
   * to an HBase table and performs freshening on the returned data.
   */
  public HBaseFreshKijiTableReader(KijiTable table, int timeout) throws IOException {
    mTable = table;
    final KijiFreshnessManager manager = new KijiFreshnessManager(table.getKiji());
    // opening a reader retains the table, so we do not need to call retain manually.
    mReader = mTable.openTableReader();
    final KijiMetaTable metaTable = mTable.getKiji().getMetaTable();
    final Set<String> keySet = metaTable.keySet(mTable.getName());
    mPolicyRecords = new HashMap<KijiColumnName, KijiFreshnessPolicyRecord>();
    // For all keys in the metatable, if those keys are freshness policy entries, cache them locally.
    for (String key: keySet) {
      if (key.startsWith("kiji.scoring.fresh.")) {
        final String columnName = key.substring(19);
        mPolicyRecords.put(new KijiColumnName(columnName), manager.retrievePolicy(table.getName(), columnName));
      }
    }
    mExecutor = FreshenerThreadPool.getInstance().get();
    mTimeout = timeout;
  }

  /**
   * Gets an instance of a KijiFreshnessPolicy from a String class name.
   *
   * @param policy The name of the freshness policy class to instantiate.
   * @return An instance of the named policy.
   */
  private KijiFreshnessPolicy policyForName(String policy) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policy).asSubclass(KijiFreshnessPolicy.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "KijiFreshnessPolicy class %s was not found on the classpath", policy));
    }
  }

  /**
   * Gets all freshness policies from the local cache necessary to validate a given data request.
   *
   * @param dataRequest the data request for which to find freshness policies.
   * @return A map from column name to KijiFreshnessPolicy.
   */
  private Map<KijiColumnName, KijiFreshnessPolicy> getPolicies(KijiDataRequest dataRequest) {
    final Collection<Column> columns = dataRequest.getColumns();
    Map<KijiColumnName, KijiFreshnessPolicy> policies =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    for (Column column: columns) {
      if (mPolicyCache != null && mPolicyCache.get(column.getColumnName()) != null) {
        policies.put(column.getColumnName(), mPolicyCache.get(column.getColumnName()));
      } else {
        if (mPolicyCache == null) {
          mPolicyCache = new HashMap<KijiColumnName, KijiFreshnessPolicy>();
        }
        final KijiFreshnessPolicyRecord record = mPolicyRecords.get(column.getColumnName());
        if (record != null) {
          // Instantiate and initialize the policies.
          final KijiFreshnessPolicy policy = policyForName(record.getFreshnessPolicyClass());
          policy.load(record.getFreshnessPolicyState());
          // Add the policy to the list of policies applicable to this data request.
          policies.put(column.getColumnName(), policy);
          mPolicyCache.put(column.getColumnName(), policy);
        }
      }
    }
    return policies;
  }

  /**
   * Gets a KijiRowData representing the data the user requested at the time they requested it.
   * May be used by freshness policies to determine freshness, and may be returned by a call to
   * {@link #get(EntityId, KijiDataRequest)}.  Should only be called once per call to get().
   *
   * @param eid The EntityId specified by the client's call to get().
   * @param dataRequest The client's data request.
   * @param requiresClientDataRequest The number of freshness policies that use the client's data request to test for
   *   freshness.
   * @return A Future&lt;KijiRowData&gt; representing the data requested by the user, or null if no
   *   freshness policies require the user's requested data.
   */
  private Future<KijiRowData> getClientData(
      final EntityId eid, final KijiDataRequest dataRequest, int requiresClientDataRequest) {
    Future<KijiRowData> clientData = null;
    if (requiresClientDataRequest != 0) {
      clientData = mExecutor.submit(new Callable<KijiRowData>() {
        public KijiRowData call() throws IOException {
          return mReader.get(eid, dataRequest);
        }
      });
    }
  return clientData;
  }

  /**
   * Gets an instance of a producer from a String class name.
   *
   * @param producer The name of the producer class to instantiate.
   * @return An instance of the named producer.
   */
  private KijiProducer producerForName(String producer) {
    try {
      //TODO: instead of null, configure with a Configuration appropriate for a freshening producer?
      return ReflectionUtils.newInstance(
        Class.forName(producer).asSubclass(KijiProducer.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format(
          "Producer class %s was not found on the classpath", producer));
    }
  }

  /**
   * Creates a future for each {@link org.kiji.scoring.KijiFreshnessPolicy} applicable to a given
   * {@link org.kiji.schema.KijiDataRequest}.
   *
   * @param usesClientDataRequest A map from column name to KijiFreshnessPolicies that use the
   *   client data request to fulfill isFresh() calls.
   * @param usesOwnDataRequest A map from column name to KijiFreshnessPolicies that use custom
   *   data requests to fulfill isFresh() calls.
   * @param clientData A Future&lt;KijiRowData&gt; representing the data requested by the client
   * @param eid The EntityId specified by the client's call to get().
   * @return A list of Future&lt;Boolean&gt; representing the need to reread data from the table
   *   to include producer output after freshening.
   */
  private List<Future<Boolean>> getFutures(
      final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest,
      final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest,
      final Future<KijiRowData> clientData,
      final EntityId eid,
      final KijiDataRequest clientRequest) {
    final List<Future<Boolean>> futures = Lists.newArrayList();
    for (final KijiColumnName key: usesClientDataRequest.keySet()) {
      final Future<Boolean> requiresReread = mExecutor.submit(new Callable<Boolean>() {
        public Boolean call() {
          KijiRowData rowData = null;
          final PolicyContext policyContext = new PolicyContext(clientRequest, key, mTable.getKiji().getConf());
          try {
            rowData = clientData.get();
          } catch (InterruptedException ie) {
            throw new RuntimeException("Freshening thread interrupted", ie);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
              LOG.warn("Client data could not be retrieved.  Freshness policies which operate against"
                  + "the client data request will not run. " + ee.getCause().getMessage());
            } else {
              throw new RuntimeException(ee.getCause());
            }
          }
          if (rowData != null) {
            final boolean isFresh = usesClientDataRequest.get(key).isFresh(rowData, policyContext);
            if (isFresh) {
              // If isFresh, return false to indicate a reread is not necessary.
              return Boolean.FALSE;
            } else {
              final KijiProducer producer =
                  producerForName(mPolicyRecords.get(key).getProducerClass());
              // TODO: Does the producer need to be initialized?
              // TODO: add the context
              // producer.produce(rowData, CONTEXT);
              // If a producer runs, return true to indicate a reread is necessary.  This assumes
              // the producer will write to the requested cells, eventually it may be appropriate
              // to actually check if this is true.
              return Boolean.TRUE;
            }
          } else {
            return Boolean.FALSE;
          }
        }
      });
      futures.add(requiresReread);
    }
    for (final KijiColumnName key: usesOwnDataRequest.keySet()) {
      final Future<Boolean> requiresReread = mExecutor.submit(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          final KijiRowData rowData =
              mReader.get(eid, usesOwnDataRequest.get(key).getDataRequest());
          final PolicyContext policyContext =
              new PolicyContext(clientRequest, key, mTable.getKiji().getConf());
          final boolean isFresh = usesOwnDataRequest.get(key).isFresh(rowData, policyContext);
          if (isFresh) {
            // If isFresh, return false to indicate that a reread is not necessary.
            return Boolean.FALSE;
          } else {
            final KijiProducer producer =
                producerForName(mPolicyRecords.get(key).getProducerClass());
            // TODO: Does the producer need to be initialized?
            // TODO: add the context
            // producer.produce(rowData, CONTEXT);
            // If a producer runs, return true to indicate that a reread is necessary.  This assumes
            // the producer will write to the requested cells, eventually it may be appropriate
            // to actually check if this is true.
            return Boolean.TRUE;
          }
        }
      });
      futures.add(requiresReread);
    }
    return futures;
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

    final Map<KijiColumnName, KijiFreshnessPolicy> policies = getPolicies(dataRequest);
    // If there are no freshness policies attached to the requested columns, return the requested
    // data.
    if (policies.size() == 0) {
      return mReader.get(eid, dataRequest);
    }

    final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest =
        new HashMap<KijiColumnName, KijiFreshnessPolicy>();
    for (KijiColumnName key: policies.keySet()) {
      if (policies.get(key).shouldUseClientDataRequest()) {
        usesClientDataRequest.put(key, policies.get(key));
      } else {
        usesOwnDataRequest.put(key, policies.get(key));
      }
    }

    final Future<KijiRowData> clientData =
        getClientData(eid, dataRequest, usesClientDataRequest.size());
    final List<Future<Boolean>> futures =
        getFutures(usesClientDataRequest, usesOwnDataRequest, clientData, eid, dataRequest);

    final Future<Boolean> superFuture = mExecutor.submit(new Callable<Boolean>() {
      public Boolean call() {
        boolean retVal = false;
        for (Future<Boolean> future: futures) {
          // block on completion of each future and update the return value to be true if any
          // future returns true.
          try {
            retVal = future.get() || retVal;
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
              LOG.warn("Custom freshness policy data request failed.  Failed freshness policy will"
                  + "not run. " + ee.getCause().getMessage());
            } else {
              throw new RuntimeException(ee.getCause());
            }
          } catch (InterruptedException ie) {
            throw new RuntimeException("Freshening thread interrupted.", ie);
          }
        }
        return retVal;
      }
    });

    try {
      if (superFuture.get(mTimeout, TimeUnit.MILLISECONDS)) {
        return mReader.get(eid, dataRequest);
      } else {
        if (clientData != null) {
          return clientData.get();
        } else {
          return mReader.get(eid, dataRequest);
        }
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException("Freshening thread interrupted.", ie);
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof RuntimeException) {
        throw new RuntimeException(ee.getCause());
      }
      // This should be unreachable, since superFuture's call method throws nothing but runtime.
      return mReader.get(eid, dataRequest);
    } catch (TimeoutException te) {
      return mReader.get(eid, dataRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      List<EntityId> eids, KijiDataRequest dataRequest) throws IOException {
    List<KijiRowData> results = Lists.newArrayList();
    for (EntityId eid : eids) {
      results.add(get(eid, dataRequest));
    }
    return results;
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
