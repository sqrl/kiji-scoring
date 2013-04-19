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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.util.ResourceUtils;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.KijiFreshProducerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.PolicyContext;
import org.kiji.scoring.avro.KijiFreshnessPolicyRecord;

/**
 * Implementation of a Fresh Kiji Table Reader for HBase.
 */
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
   *
   * @param table the table that will be read/scored.
   * @param timeout the maximum number of milliseconds to spend trying to score data. If the
   * process times out, stale or partially-scored data may be returned by {@link get()} calls.
   * @throws IOException if an error occurs while processing the table or freshness policy.
   */
  public HBaseFreshKijiTableReader(KijiTable table, int timeout) throws IOException {
    mTable = table;
    final KijiFreshnessManager manager = new KijiFreshnessManager(table.getKiji());
    // opening a reader retains the table, so we do not need to call retain manually.
    mReader = mTable.openTableReader();
    mPolicyRecords = manager.retrievePolicies(mTable.getName());
    mExecutor = FreshenerThreadPool.getInstance().get();
    mTimeout = timeout;
  }

  /**
   * Gets an instance of a KijiFreshnessPolicy from a String class name.
   *
   * @param policy The name of the freshness policy class to instantiate.
   * @return An instance of the named policy.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  KijiFreshnessPolicy policyForName(String policy) {
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
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  Map<KijiColumnName, KijiFreshnessPolicy> getPolicies(KijiDataRequest dataRequest) {
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
   * @param requiresClientDataRequest whether the client's data request is required for any
   *   freshness policies.
   * @return A Future&lt;KijiRowData&gt; representing the data requested by the user, or null if no
   *   freshness policies require the user's requested data.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  Future<KijiRowData> getClientData(
      final EntityId eid, final KijiDataRequest dataRequest, boolean requiresClientDataRequest) {
    Future<KijiRowData> clientData = null;
    if (requiresClientDataRequest) {
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
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  KijiProducer producerForName(String producer) {
    try {
      return ReflectionUtils.newInstance(
        Class.forName(producer).asSubclass(KijiProducer.class), mTable.getKiji().getConf());
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
   * @param clientRequest the client's original request.
   * @return A list of Future&lt;Boolean&gt; representing the need to reread data from the table
   *   to include producer output after freshening.
   *
   * Package private for testing purposes only, should not be accessed externally.
   */
  List<Future<Boolean>> getFutures(
      final Map<KijiColumnName, KijiFreshnessPolicy> usesClientDataRequest,
      final Map<KijiColumnName, KijiFreshnessPolicy> usesOwnDataRequest,
      final Future<KijiRowData> clientData,
      final EntityId eid,
      final KijiDataRequest clientRequest) {
    final List<Future<Boolean>> futures = Lists.newArrayList();
    for (final KijiColumnName key: usesClientDataRequest.keySet()) {
      final Future<Boolean> requiresReread = mExecutor.submit(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          KijiRowData rowData = null;
          //TODO store these and use a setter for client request
          final PolicyContext policyContext =
              new InternalPolicyContext(clientRequest, key, mTable.getKiji().getConf());
          try {
            rowData = clientData.get();
          } catch (InterruptedException ie) {
            throw new RuntimeException("Freshening thread interrupted", ie);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
              LOG.warn("Client data could not be retrieved.  "
                  + "Freshness policies which operate against "
                  + "the client data request will not run. " + ee.getCause().getMessage());
            } else {
              throw new RuntimeException(ee);
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

              final KijiFreshProducerContext context =
                  KijiFreshProducerContext.create(mTable, key, eid);
              producer.setup(context);
              producer.produce(mReader.get(eid, producer.getDataRequest()), context);
              producer.cleanup(context);

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
              new InternalPolicyContext(clientRequest, key, mTable.getKiji().getConf());
          final boolean isFresh = usesOwnDataRequest.get(key).isFresh(rowData, policyContext);
          if (isFresh) {
            // If isFresh, return false to indicate that a reread is not necessary.
            return Boolean.FALSE;
          } else {
            final KijiProducer producer =
                producerForName(mPolicyRecords.get(key).getProducerClass());
            final KijiFreshProducerContext context =
                KijiFreshProducerContext.create(mTable, key, eid);
            producer.setup(context);
            producer.produce(mReader.get(eid, producer.getDataRequest()), context);
            producer.cleanup(context);
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
        getClientData(eid, dataRequest, usesClientDataRequest.size() > 0);
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
              throw new RuntimeException(ee);
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
      throw new RuntimeException(ee);
    } catch (TimeoutException te) {
      return mReader.get(eid, dataRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      List<EntityId> eids, final KijiDataRequest dataRequest) throws IOException {
    final List<Future<KijiRowData>> futures = Lists.newArrayList();
    for (final EntityId eid : eids) {
      final Future<KijiRowData> future = mExecutor.submit(new Callable<KijiRowData>() {
        public KijiRowData call() throws IOException {
          return get(eid, dataRequest);
        }
      });
      futures.add(future);
    }
    final Future<List<KijiRowData>> superDuperFuture =
        mExecutor.submit(new Callable<List<KijiRowData>>() {
      public List<KijiRowData> call() {
        List<KijiRowData> results = Lists.newArrayList();
        for (Future<KijiRowData> future : futures) {
          try {
            results.add(future.get());
          } catch (InterruptedException ie) {
            throw new RuntimeException("Freshening thread interrupted.", ie);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof IOException) {
              LOG.warn("Custom freshness policy data request failed.  Failed freshness policy will"
                  + "not run. " + ee.getCause().getMessage());
            } else {
              throw new RuntimeException(ee);
            }
          }
        }
        return results;
      }
    });

    final List<KijiRowData> futureResult;
    try {
      futureResult = superDuperFuture.get(mTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ie) {
      throw new RuntimeException("Freshening thread interrupted.", ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    } catch (TimeoutException te) {
      return mReader.bulkGet(eids, dataRequest);
    }
    if (futureResult != null) {
      return futureResult;
    } else {
      return mReader.bulkGet(eids, dataRequest);
    }
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
