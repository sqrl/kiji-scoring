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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.kvstore.KeyValueStore;
import org.kiji.mapreduce.kvstore.KeyValueStoreReaderFactory;
import org.kiji.schema.EntityId;
import org.kiji.schema.InternalKijiError;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequest.Column;
import org.kiji.schema.KijiRowData;
import org.kiji.schema.KijiRowScanner;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableReader;
import org.kiji.schema.RuntimeInterruptedException;
import org.kiji.schema.util.ReferenceCountable;
import org.kiji.scoring.FreshKijiTableReader;
import org.kiji.scoring.FreshKijiTableReader.Builder.StatisticGatheringMode;
import org.kiji.scoring.FreshenerContext;
import org.kiji.scoring.KijiFreshnessManager;
import org.kiji.scoring.KijiFreshnessPolicy;
import org.kiji.scoring.ScoreFunction;
import org.kiji.scoring.avro.KijiFreshenerRecord;
import org.kiji.scoring.impl.InternalFreshKijiTableReader.ReaderState.State;
import org.kiji.scoring.impl.MultiBufferedWriter.SingleBuffer;
import org.kiji.scoring.statistics.FreshKijiTableReaderStatistics;
import org.kiji.scoring.statistics.FreshenerSingleRunStatistics;

/** Local implementation of FreshKijiTableReader. */
@ApiAudience.Private
public final class InternalFreshKijiTableReader implements FreshKijiTableReader {

  private static final Logger LOG = LoggerFactory.getLogger(InternalFreshKijiTableReader.class);

  // -----------------------------------------------------------------------------------------------
  // Inner classes.
  // -----------------------------------------------------------------------------------------------

  /**
   * Class for ensuring a predictable state transition for this reader. This class is not
   * thread safe and instances of it should be kept private within the reader for which it manages
   * state.
   */
  public static final class ReaderState {

    private final AtomicReference<State> mState = new AtomicReference<State>(State.INITIALIZING);

    /** All possible reader states. */
    public enum State {
      INITIALIZING, INITIALIZED, OPEN, CLOSING, CLOSED
    }

    /**
     * Progress the reader state directly from Initializing to Open.
     *
     * @throws IllegalStateException if the reader is not already initializing.
     */
    public void finishInitializingAndOpen() {
      Preconditions.checkState(
          mState.compareAndSet(State.INITIALIZING, State.OPEN), String.format(
          "Cannot finish initializing and open a reader which is not initializing. State was: %s",
          mState.get()));
    }

    /**
     * Progress the reader state from initialized to open.
     *
     * @throws IllegalStateException if the reader is not already initialized.
     */
    public void open() {
      Preconditions.checkState(
          mState.compareAndSet(State.INITIALIZED, State.OPEN), String.format(
          "Cannot open a reader which is not initialized.  State was: %s.", mState.get()));
    }

    /**
     * Progress the reader state from open to closing.
     *
     * @throws IllegalStateException if the reader is not already open.
     */
    public void beginClosing() {
      Preconditions.checkState(
          mState.compareAndSet(State.OPEN, State.CLOSING), String.format(
          "Cannot begin closing a reader which is not open.  State was: %s.", mState.get()));
    }

    /**
     * Progress the reader state from closing to closed.
     *
     * @throws IllegalStateException if the reader is not already closing.
     */
    public void finishClosing() {
      Preconditions.checkState(
          mState.compareAndSet(State.CLOSING, State.CLOSED), String.format(
          "Cannot finish closing a reader which is not closing.  State was: %s.", mState.get()));
    }

    /**
     * Ensure that the reader is in a given state.
     *
     * @param required the state in which the reader must be.
     * @throws IllegalStateException if the required and actual states do not match.
     */
    public void requireState(
        State required
    ) {
      final State actual = mState.get();
      Preconditions.checkState(actual == required, String.format(
          "Required state was: %s, but found %s.", required, actual));
    }

    /**
     * Get the current state of this reader.
     *
     * @return the current state of this reader.
     */
    public State getState() { return mState.get(); }
  }

  // -----------------------------------------------------------------------------------------------

  /** Encapsulation of all state necessary to perform freshening for a single column. */
  private static final class Freshener implements ReferenceCountable<Freshener> {

    private final KijiFreshnessPolicy mPolicy;
    private final ScoreFunction mScoreFunction;
    private final KeyValueStoreReaderFactory mFactory;
    private final KijiColumnName mAttachedColumn;
    private final AtomicInteger mRetainCounter = new AtomicInteger(1);
    private final Map<String, String> mParameters;

    /**
     * Initialize a new Freshener.
     *
     * @param policy the KijiFreshnessPolicy which governs this Freshener.
     * @param scoreFunction the ScoreFunction which generates scores for this Freshener.
     * @param factory the KVStoreReaderFactory which services the policy and score function.
     * @param attachedColumn the column to which this Freshener is attached.
     * @param parameters configuration parameters retrieved from the Freshener record which are
     *     accessible to the freshness policy and score function via
     *     {@link FreshenerContext#getParameters()} method.
     */
    public Freshener(
        final KijiFreshnessPolicy policy,
        final ScoreFunction scoreFunction,
        final KeyValueStoreReaderFactory factory,
        final KijiColumnName attachedColumn,
        final Map<String, String> parameters
    ) {
      mPolicy = policy;
      mScoreFunction = scoreFunction;
      mFactory = factory;
      mAttachedColumn = attachedColumn;
      mParameters = parameters;
    }

    /** {@inheritDoc} */
    @Override
    public Freshener retain() {
      final int counter = mRetainCounter.getAndIncrement();
      Preconditions.checkState(counter >= 1,
          "Cannot retain closed Freshener: %s retain counter was %s.",
          toString(), counter);
      return this;
    }

    /** {@inheritDoc} */
    @Override
    public void release() throws IOException {
      final int counter = mRetainCounter.decrementAndGet();
      Preconditions.checkState(counter >= 0,
          "Cannot release closed Freshener: %s retain counter is now %s.",
          toString(), counter);
      if (counter == 0) {
        close();
      }
    }

    /**
     * Cleanup contained resources.  Should only be called by {@link #release()}
     *
     * @throws IOException in case of an error cleaning up the score function.
     */
    private void close() throws IOException {
      mScoreFunction.cleanup(InternalFreshenerContext.create(
          mAttachedColumn, mParameters, mFactory));
      mFactory.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("attached_column", mAttachedColumn.toString())
          .add("policy_class", mPolicy.getClass().getName())
          .add("score_function_class", mScoreFunction.getClass().getName())
          .add("parameters", mParameters)
          .toString();
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Container class for all state which can be modified by a call to
   * {@link #rereadFreshenerRecords()} or {@link #rereadFreshenerRecords(java.util.List)}.
   */
  private static final class RereadableState {

    private final ImmutableList<KijiColumnName> mColumnsToFreshen;
    private final ImmutableMap<KijiColumnName, KijiFreshenerRecord> mFreshenerRecords;
    private final ImmutableMap<KijiColumnName, Freshener> mFresheners;

    /**
     * Initialize a new RereadableState.
     *
     * @param columnsToFreshen the columns which may be refreshed by the reader which holds this
     *     RereadableState.
     * @param freshenerRecords the KijiFreshenerRecords for the columnsToFreshen if there are any
     *     attached.
     * @param fresheners the cached Freshener objects which perform freshening.
     */
    private RereadableState(
        final List<KijiColumnName> columnsToFreshen,
        final Map<KijiColumnName, KijiFreshenerRecord> freshenerRecords,
        final Map<KijiColumnName, Freshener> fresheners
    ) {
      mColumnsToFreshen = ImmutableList.copyOf(columnsToFreshen);
      mFreshenerRecords = ImmutableMap.copyOf(freshenerRecords);
      mFresheners = ImmutableMap.copyOf(fresheners);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** All state necessary to process a freshening 'get' request. */
  private static final class FresheningRequestContext {

    /** Time in milliseconds since the epoch at which this request started. */
    private final long mStartTime;
    /**
     * Fresheners applicable to this request. These Fresheners should be released when they are no
     * longer needed.
     */
    private final ImmutableMap<KijiColumnName, Freshener> mFresheners;
    /**
     * FreshenerContexts associated with Fresheners used in this request. These contexts are fully
     * initialized.
     */
    private final Map<KijiColumnName, InternalFreshenerContext> mFreshenerContexts;
    /**
     * A regular KijiTableReader to retrieve data for freshness checking and scorer inputs. This
     * reader should not be closed during the request.
     */
    private final KijiTableReader mReader;
    /** The row to which this request applies. */
    private final EntityId mEntityId;
    /** The data request which should be refreshed before returning. */
    private final KijiDataRequest mClientDataRequest;
    /** A Future representing the current state of the requested data before freshening. */
    private final Future<KijiRowData> mClientDataFuture;
    /**
     * The buffer into which Fresheners will write their results. This writer should not be closed
     * during the request.
     */
    private final MultiBufferedWriter mBufferedWriter;
    /**
     * A single view into the MultiBufferedWriter to be shared by the entire request. This is only
     * used if mAllowPartial is false.
     */
    private final SingleBuffer mRequestBuffer;
    /** Whether this request allows partial freshening. */
    private final boolean mAllowPartial;
    /** The set of Fresheners which have not finished. */
    private final Map<KijiColumnName, KijiFreshenerRecord> mFreshenersRemaining;
    /** Statistics about individual completed Fresheners. */
    private final BlockingQueue<FreshenerSingleRunStatistics> mFreshenerSingleRunStatistics;
    /**
     * Whether any Freshener has written into a buffer for this request. This value may only move
     * from false to true.
     */
    private boolean mHasReceivedWrites = false;
    /**
     * Whether the request managed by this context has timed out. This value may only move from
     * false to true.
     */
    private boolean mHasTimedOut = false;

    /**
     * Initialize a new FresheningRequestContext.
     *
     * @param startTime the time in milliseconds since the epoch at which this request started.
     * @param fresheners Fresheners which should be run to fulfill this request.
     * @param freshenerContexts InternalFreshenerContext which service fresheners in this request.
     * @param freshenerRecords Freshener records for Fresheners applicable to this request.
     * @param reader the regular KijiTableReader to which to delegate table reads.
     * @param entityId the row from which to read.
     * @param dataRequest the section of the row which should be refreshed.
     * @param clientDataFuture a Future representing the current state of the requested data before
     *     freshening.
     * @param bufferedWriter the MultiBufferedWriter used by this context to buffer and commit
     *     writes.
     * @param allowPartial whether this context allows partial freshening.
     * @param statisticsQueue Queue for communicating statistics about completed Fresheners to the
     *     statistics gathering thread. This queue is thread safe and ordering of statistics in the
     *     queue does not matter.
     */
    // CSOFF: Parameter count
    public FresheningRequestContext(
        final long startTime,
        final ImmutableMap<KijiColumnName, Freshener> fresheners,
        final ImmutableMap<KijiColumnName, InternalFreshenerContext> freshenerContexts,
        final ImmutableMap<KijiColumnName, KijiFreshenerRecord> freshenerRecords,
        final KijiTableReader reader,
        final EntityId entityId,
        final KijiDataRequest dataRequest,
        final Future<KijiRowData> clientDataFuture,
        final MultiBufferedWriter bufferedWriter,
        final boolean allowPartial,
        final BlockingQueue<FreshenerSingleRunStatistics> statisticsQueue
    ) {
      // CSON
      mStartTime = startTime;
      mFresheners = fresheners;
      mReader = reader;
      mEntityId = entityId;
      mClientDataRequest = dataRequest;
      mClientDataFuture = clientDataFuture;
      mFreshenerContexts = freshenerContexts;
      mBufferedWriter = bufferedWriter;
      mAllowPartial = allowPartial;
      mFreshenerSingleRunStatistics = statisticsQueue;
      if (mAllowPartial) {
        // Each Freshener will have its own buffer when partial freshening is enabled, so the
        // request buffer is not needed.
        mRequestBuffer = null;
      } else {
        // Each Freshener may write only one value, so initialize the size of the buffer to the
        // number of Fresheners.
        mRequestBuffer = bufferedWriter.openSingleBuffer(fresheners.size());
      }
      mFreshenersRemaining = Maps.newHashMap(freshenerRecords);
    }

    /**
     * Signal the context that a Freshener has finished.
     *
     * @param attachedColumn the column to which the finishing Freshener is attached.
     * @param scoreFunctionRan whether a ScoreFunction was run for the Freshener which finished.
     * @return the number of unfinished Fresheners.
     */
    public int finishFreshener(
        final KijiColumnName attachedColumn,
        final boolean scoreFunctionRan
    ) {
      final long finishTime = System.nanoTime();
      mFreshenerSingleRunStatistics.add(FreshenerSingleRunStatistics.create(
          mHasTimedOut,
          finishTime - mStartTime,
          scoreFunctionRan,
          mFreshenersRemaining.get(attachedColumn)));
      final int remaining;
      synchronized (mFreshenersRemaining) {
        mFreshenersRemaining.remove(attachedColumn);
        remaining = mFreshenersRemaining.size();
      }
      if (remaining < 0) {
        throw new InternalKijiError("More Fresheners have finished than were started.");
      } else {
        return remaining;
      }
    }

    /**
     * Get a new unique SingleBuffer to be used by a single Freshener.
     *
     * @return a new unique SingleBuffer to be used by a single Freshener.
     */
    public SingleBuffer openUniqueBuffer() {
      // Because this unique buffer will be used by only a single Freshener which can write only a
      // single value, set the initial buffer size to 1.
      return mBufferedWriter.openSingleBuffer(1);
    }

    /**
     * Called when a Freshener finishes after writing a value into a buffer. Sets mHasReceivedWrites
     * which is used when a request times out with partial freshening enabled to tell whether there
     * is new data to read from the table.
     */
    public void freshenerWrote() {
      mHasReceivedWrites = true;
    }

    /**
     * Marks this request as having timed out. Fresheners which finished after this method is called
     * will be recorded as having timed out.
     */
    public void timeOut() {
      mHasTimedOut = true;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** Callable which performs a read from a table.  Used in a Future to read asynchronously, */
  private static final class TableReadCallable implements Callable<KijiRowData> {

    private final KijiTableReader mReader;
    private final EntityId mEntityId;
    private final KijiDataRequest mDataRequest;

    /**
     * Initialize a new TableReadCallable.
     *
     * @param reader the KijiTableReader to use to perform the read.
     * @param entityId the EntityId of the row from which to read data.
     * @param dataRequest the KijiDataRequest defining the data to read from the row.
     */
    public TableReadCallable(
        final KijiTableReader reader,
        final EntityId entityId,
        final KijiDataRequest dataRequest
    ) {
      mReader = reader;
      mEntityId = entityId;
      mDataRequest = dataRequest;
    }

    /** {@inheritDoc} */
    @Override
    public KijiRowData call() throws Exception {
      return mReader.get(mEntityId, mDataRequest);
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Callable which performs freshening for a specific column in the context of a specific get
   * request. Returns a boolean indicating whether any writes were committed.
   */
  private static final class FreshenerCallable implements Callable<Boolean> {

    /**
     * Return value if the completion of this Freshener caused a write to Kiji which indicates to
     * the framework that there is new data to be read from the table. This is returned if the
     * ScoreFunction is run while partial freshening is enabled and when the last Freshener finishes
     * for a request with partial freshening disabled if any ScoreFunction ran as part of that
     * request.
     */
    private static final boolean WROTE = true;

    /**
     * Return value if the completion of this Freshener did not cause a write to Kiji which
     * indicates to the framework that there is no new data to be read from the table. This is
     * returned if a KijiFreshnessPolicy returns fresh while partial freshening is enabled and when
     * the last Freshener finishes for a request with partial freshening disabled if all policies
     * returned fresh.
     */
    private static final boolean DID_NOT_WRITE = false;

    private final FresheningRequestContext mRequestContext;
    private final KijiColumnName mAttachedColumn;
    private final Future<KijiRowData> mRowDataToCheckFuture;

    /**
     * Initialize a new FreshenerCallable.
     *
     * @param requestContext all state necessary to perform freshening specific to this request.
     * @param attachedColumn the column to which this Freshener is attached.
     * @param rowDataToCheckFuture asynchronously collected KijiRowData to be checked by
     *     {@link KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData,
     *     org.kiji.scoring.FreshenerContext)}
     */
    public FreshenerCallable(
        final FresheningRequestContext requestContext,
        final KijiColumnName attachedColumn,
        final Future<KijiRowData> rowDataToCheckFuture
    ) {
      mRequestContext = requestContext;
      mAttachedColumn = attachedColumn;
      mRowDataToCheckFuture = rowDataToCheckFuture;
    }

    /** {@inheritDoc} */
    @Override
    public Boolean call() throws Exception {
      final Freshener freshener = mRequestContext.mFresheners.get(mAttachedColumn);
      try {
        final FreshenerContext freshenerContext =
            mRequestContext.mFreshenerContexts.get(mAttachedColumn);
        final KijiRowData clientData = getFromFuture(mRowDataToCheckFuture);
        final boolean isFresh = freshener.mPolicy.isFresh(clientData, freshenerContext);
        if (isFresh) {
          if (!mRequestContext.mAllowPartial
              && 0 == mRequestContext.finishFreshener(mAttachedColumn, false)) {
            // If this is the last thread, check for writes, flush, and indicate that data was
            // written
            if (mRequestContext.mHasReceivedWrites) {
              mRequestContext.mRequestBuffer.flush();
              return WROTE;
            } else {
              return DID_NOT_WRITE;
            }
          } else {
            // If partial freshening is on or this is not the last thread to finish,
            // this did not write.
            return DID_NOT_WRITE;
          }
        } else {
          final SingleBuffer buffer;
          if (mRequestContext.mAllowPartial) {
            buffer = mRequestContext.openUniqueBuffer();
          } else {
            buffer = mRequestContext.mRequestBuffer;
          }
          buffer.put(
              mRequestContext.mEntityId,
              mAttachedColumn.getFamily(),
              mAttachedColumn.getQualifier(),
              HConstants.LATEST_TIMESTAMP,
              freshener.mScoreFunction.score(
                  mRequestContext.mReader.get(
                      mRequestContext.mEntityId,
                      freshener.mScoreFunction.getDataRequest(freshenerContext)),
                  freshenerContext));
          mRequestContext.freshenerWrote();
          final int remainingFresheners = mRequestContext.finishFreshener(mAttachedColumn, true);
          if (mRequestContext.mAllowPartial) {
            // If partial freshening is enabled, flush the buffer immediately and indicate that data
            // was written.
            buffer.flush();
            return WROTE;
          } else {
            if (0 == remainingFresheners) {
              // If this is the last thread to finish, flush the request buffer and indicate that
              // data was written.
              mRequestContext.mRequestBuffer.flush();
              return WROTE;
            } else {
              // If this is not the last thread to finish, indicate that no data was written.
              return DID_NOT_WRITE;
            }
          }
        }
      } finally {
        freshener.release();
      }
    }
  }

  // -----------------------------------------------------------------------------------------------

  /**
   * Callable which collects the return values from a list of Futures into a list of those values.
   */
  private static final class FutureAggregatingCallable<T> implements Callable<List<T>> {

    private final ImmutableList<Future<T>> mFutures;

    /**
     * Initialize a new FutureAggregatingCallable.
     *
     * @param futures asynchronously calculated values to be collected.
     */
    public FutureAggregatingCallable(
        final ImmutableList<Future<T>> futures
    ) {
      mFutures = futures;
    }

    /** {@inheritDoc} */
    @Override
    public List<T> call() throws Exception {
      final List<T> collectedResults = Lists.newArrayList();
      for (Future<T> future : mFutures) {
        collectedResults.add(getFromFuture(future));
      }
      return collectedResults;
    }
  }

  // -----------------------------------------------------------------------------------------------

  /** TimerTask for periodically calling {@link FreshKijiTableReader#rereadFreshenerRecords()}. */
  private final class RereadTask extends TimerTask {

    private final long mRereadPeriod;

    /**
     * Initialize a new RereadTask for a given reader.
     *
     * @param rereadPeriod the time in milliseconds to wait between rereads.
     */
    public RereadTask(
        final long rereadPeriod
    ) {
      mRereadPeriod = rereadPeriod;
    }

    /**
     * Get the recurrence period of this timer task.
     *
     * @return the recurrence period of this timer task.
     */
    public long getRereadPeriod() { return mRereadPeriod; }

    /** {@inheritDoc} */
    @Override
    public void run() {
      try {
        rereadFreshenerRecords();
      } catch (IOException ioe) {
        LOG.warn("Failed to reread Freshener records for FreshKijiTableReader: {}.  Failure "
            + "occurred at {}. Will attempt again in {} milliseconds",
            mReader, scheduledExecutionTime(), mRereadPeriod);
      }
    }
  }

  /** Daemon thread which collects and aggregates FreshenerStatistics. */
  private final class StatisticsGathererThread extends Thread {

    /** TimerTask for periodically logging gathered statistics. */
    private final class StatisticsLoggerTask extends TimerTask {

      private final long mLoggingInterval;

      /**
       * Initialize a new StatisticsLoggerTask.
       *
       * @param loggingInterval time in milliseconds between log messages.
       */
      private StatisticsLoggerTask(
          final long loggingInterval
      ) {
        mLoggingInterval = loggingInterval;
      }

      /**
       * Get the logging interval for this logger.
       *
       * @return the logging interval for this logger.
       */
      public long getLoggingInterval() {
        return mLoggingInterval;
      }

      /** {@inheritDoc} */
      @Override
      public void run() {
        LOG.info("{}", mAggregatedStatistics);
      }
    }

    private final StatisticsLoggerTask mStatisticsLoggerTask;
    private final FreshKijiTableReaderStatistics mAggregatedStatistics =
        FreshKijiTableReaderStatistics.create(mStatisticGatheringMode);
    private volatile boolean mShutdown = false;

    /**
     * Initialize a new StatisticsGathererThread.
     *
     * @param loggingInterval the time in milliseconds between automatic logging of gathered
     *     statistics.  0 indicates no automatic logging.
     */
    private StatisticsGathererThread(
        final long loggingInterval
    ) {
      if (0 < loggingInterval) {
        final Timer timer = new Timer();
        mStatisticsLoggerTask = new StatisticsLoggerTask(loggingInterval);
        timer.scheduleAtFixedRate(mStatisticsLoggerTask, loggingInterval, loggingInterval);
      } else if (0 == loggingInterval) {
        mStatisticsLoggerTask = null;
      } else {
        throw new IllegalArgumentException(String.format(
            "Statistics logging interval cannot be less than 0, found: %d", loggingInterval));
      }
    }

    /** Collect and save a FreshenerSingleRunStatistics from the reader's StatisticsQueue. */
    private void collectStat() {
      try {
        final FreshenerSingleRunStatistics stats = mStatisticsQueue.take();
        // This switch is redundant right now because this thread is only created if the mode is ALL
        // but future modes will require it.
        switch (mStatisticGatheringMode) {
          case ALL: {
            mAggregatedStatistics.addFreshenerRunStatistics(stats);
            return;
          }
          case NONE: return;
          default:
        }
      } catch (InterruptedException ie) {
        throw new RuntimeInterruptedException(ie);
      }
    }

    /** {@inheritDoc} */
    @Override
    public void run() {
      while (!mShutdown) {
        collectStat();
      }
    }

    /** Stop gathering statistics. */
    public void shutdown() {
      mShutdown = true;
      if (null != mStatisticsLoggerTask) {
        mStatisticsLoggerTask.cancel();
      }
    }

    /**
     * Get statistics gathered by this thread.
     *
     * @return statistics gathered by this thread.
     */
    public FreshKijiTableReaderStatistics getStatistics() {
      return mAggregatedStatistics;
    }

    /**
     * Get the logging interval for statistics gathered by this Thread.
     *
     * @return the logging interval for statistics gathered by this Thread.
     */
    public long getLoggingInterval() {
      if (null != mStatisticsLoggerTask) {
        return mStatisticsLoggerTask.getLoggingInterval();
      } else {
        // 0 indicates no logging.
        return 0;
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // Static methods.
  // -----------------------------------------------------------------------------------------------

  /**
   * Gets an instance of a KijiFreshnessPolicy from a String class name.
   *
   * @param policyClassName The name of the freshness policy class to instantiate.
   * @return a new instance of a KijiFreshnessPolicy from a String class name.
   */
  private static KijiFreshnessPolicy policyForName(
      final String policyClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(policyClassName).asSubclass(KijiFreshnessPolicy.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Gets an instance of a ScoreFunction from a String class name.
   *
   * @param scoreFunctionClassName the fully qualified class name of the ScoreFunction subclass to
   *     instantiate.
   * @return An instance of the named producer.
   */
  private static ScoreFunction scoreFunctionForName(
      final String scoreFunctionClassName
  ) {
    try {
      return ReflectionUtils.newInstance(
          Class.forName(scoreFunctionClassName).asSubclass(ScoreFunction.class), null);
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  /**
   * Filters a map of KijiFreshenerRecords to include only those records whose columns are
   * contained in the columnsToFreshen list.  If a column in columnsToFreshen does not occur in the
   * records map, it will not be included in the returned map.
   *
   * <p>
   *   Specifying a column family in columnsToFreshen will collect all records for qualified columns
   *   in that family.
   * </p>
   *
   * @param columnsToFreshen a list of columns whose records should be extracted from the map.
   * @param allRecords a map containing all records for a table which will be filtered to include
   *    only the specified columns. This map is not modified by this method.
   * @return an ImmutableMap of column names from the columnsToFreshen list and associated
   *    KijiFreshenerRecords.
   */
  private static ImmutableMap<KijiColumnName, KijiFreshenerRecord> filterRecords(
      final Map<KijiColumnName, KijiFreshenerRecord> allRecords,
      final List<KijiColumnName> columnsToFreshen
  ) {
    if (null == columnsToFreshen || columnsToFreshen.isEmpty()) {
      // If no columns are specified, all records should be instantiated.
      return ImmutableMap.copyOf(allRecords);
    } else {
      final Map<KijiColumnName, KijiFreshenerRecord> collectedRecords = Maps.newHashMap();
      for (KijiColumnName column : columnsToFreshen) {
        if (column.isFullyQualified()) {
          final KijiFreshenerRecord record = allRecords.get(column);
          if (null != record) {
            collectedRecords.put(column, record);
          }
        } else {
          // For families, collect all records for columns in that family.
          for (Map.Entry<KijiColumnName, KijiFreshenerRecord> recordEntry : allRecords.entrySet()) {
            if (column.getFamily().equals(recordEntry.getKey().getFamily())) {
              collectedRecords.put(recordEntry.getKey(), recordEntry.getValue());
            }
          }
        }
      }
      return ImmutableMap.copyOf(collectedRecords);
    }
  }

  /**
   * Create a KeyValueStoreReaderFactory from the required stores of a ScoreFunction and
   * KijiFreshnessPolicy. Stores defined by the policy override those defined by the ScoreFunction.
   *
   * @param context context in which to run getRequiredStores(context).
   * @param scoreFunction ScoreFunction from which to get required stores.
   * @param policy KijiFreshnessPolicy from which to get required stores.
   * @return a new KeyValueStoreReaderFactory configured to read the required stores of the given
   *     ScoreFunction and KijiFreshnessPolicy.
   */
  public static KeyValueStoreReaderFactory createKVStoreReaderFactory(
      final InternalFreshenerContext context,
      final ScoreFunction scoreFunction,
      final KijiFreshnessPolicy policy
  ) {
    final Map<String, KeyValueStore<?, ?>> kvMap = Maps.newHashMap();
    kvMap.putAll(scoreFunction.getRequiredStores(context));
    kvMap.putAll(policy.getRequiredStores(context));
    return KeyValueStoreReaderFactory.create(kvMap);
  }

  /**
   * Create a map of Fresheners from a map of KijiFreshenerRecords.  Freshener components
   * are proactively created.
   *
   * @param records the records from which to create Fresheners.
   * @return a mapping from KijiColumnNames to associated Fresheners.
   * @throws IOException in case of an error setting up a producer.
   */
  private static ImmutableMap<KijiColumnName, Freshener> createFresheners(
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records
  ) throws IOException {
    return fillFresheners(records, ImmutableMap.<KijiColumnName, Freshener>of());
  }

  /**
   * Fills a partial map of Fresheners by creating new Fresheners for each record not already
   * reflected by the fresheners map.
   *
   * @param records a map of records for which to create Fresheners.
   * @param oldFresheners a partially filled map of Fresheners to be completed with new Fresheners
   *    built from the records map.
   * @return a map of Fresheners for each KijiFreshenerRecord in records.
   * @throws IOException in case of an error setting up a producer.
   */
  private static ImmutableMap<KijiColumnName, Freshener> fillFresheners(
      final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records,
      final ImmutableMap<KijiColumnName, Freshener> oldFresheners
  ) throws IOException {
    final Map<KijiColumnName, Freshener> fresheners = Maps.newHashMap();
    for (Map.Entry<KijiColumnName, KijiFreshenerRecord> entry : records.entrySet()) {
      if (!oldFresheners.containsKey(entry.getKey())) {
        // If there is not already a Freshener for this record, make one.

        final KijiFreshenerRecord record = entry.getValue();

        // Create the FreshenerSetupContext
        final InternalFreshenerContext context =
            InternalFreshenerContext.create(entry.getKey(), record.getParameters());

        // Instantiate the policy and score function.
        final KijiFreshnessPolicy policy = policyForName(record.getFreshnessPolicyClass());
        final ScoreFunction scoreFunction = scoreFunctionForName(record.getScoreFunctionClass());

        // Create the KVStoreReaderFactory from the required stores of the score function and
        // policy, and add the factory to the Freshener context.
        final KeyValueStoreReaderFactory factory =
            createKVStoreReaderFactory(context, scoreFunction, policy);
        context.setKeyValueStoreReaderFactory(factory);

        // Setup the policy and score function.
        policy.setup(context);
        scoreFunction.setup(context);

        // Build the Freshener from initialized components.
        final Freshener freshener = new Freshener(
            policy,
            scoreFunction,
            factory,
            entry.getKey(),
            entry.getValue().getParameters());
        fresheners.put(entry.getKey(), freshener);
      } else {
        // If there is already a Freshener for this key, save it.
        fresheners.put(entry.getKey(), oldFresheners.get(entry.getKey()));
      }
    }

    return ImmutableMap.copyOf(fresheners);
  }

  /**
   * Get a future from a given callable.  This method uses the singleton Executor to run threads
   * responsible for carrying out the operation of the Future.
   *
   * @param callable the callable to run in the new Future.
   * @param <RETVAL> the return type of the callable and Future.
   * @return a new Future representing asynchronous execution of the given callable.
   */
  private static <RETVAL> Future<RETVAL> getFuture(
      Callable<RETVAL> callable
  ) {
    return FreshenerThreadPool.getInstance().getExecutorService().submit(callable);
  }

  /**
   * Get the value from a given Future.  This blocks until the Future is complete.
   *
   * @param future the Future from which to get the resultant value.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future
  ) {
    try {
      return future.get();
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in milliseconds) before a TimeoutException.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout
  ) throws TimeoutException {
    return getFromFuture(future, timeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Get the value from a given Future with a timeout.  This blocks until the Future is complete or
   * the timeout expires.
   *
   * @param future the Future from which to get the resultant value.
   * @param timeout the time to wait (in units defined by timeUnit) before a TimeoutException.
   * @param timeUnit the unit of time to use for the timeout.
   * @param <RETVAL> the type of the value returned by the Future.
   * @return the return value of the given Future.
   * @throws TimeoutException if the Future does not return before the timeout period elapses.
   */
  private static <RETVAL> RETVAL getFromFuture(
      final Future<RETVAL> future,
      final long timeout,
      final TimeUnit timeUnit
  ) throws TimeoutException {
    try {
      return future.get(timeout, timeUnit);
    } catch (InterruptedException ie) {
      throw new RuntimeInterruptedException(ie);
    } catch (ExecutionException ee) {
      throw new RuntimeException(ee);
    }
  }

  /**
   * Filter a map of Fresheners down to only those attached to columns in a given list. Columns
   * which do not have Fresheners attached will not be reflected in the return value of this
   * method. An empty return indicates that no Fresheners are attached to the given columns.
   *
   * @param columnsToFreshen a list of columns for which to get Fresheners.
   * @param fresheners a map of all available fresheners. This map will not be modified by this
   *     method.
   * @return all Fresheners attached to columns in columnsToFresh available in fresheners.
   */
  private static ImmutableMap<KijiColumnName, Freshener> filterFresheners(
      final ImmutableList<KijiColumnName> columnsToFreshen,
      final ImmutableMap<KijiColumnName, Freshener> fresheners
  ) {
    final Map<KijiColumnName, Freshener> collectedFresheners = Maps.newHashMap();
    for (KijiColumnName column : columnsToFreshen) {
      if (column.isFullyQualified()) {
        final Freshener freshener = fresheners.get(column);
        if (null != freshener) {
          collectedFresheners.put(column, freshener);
        }
      } else {
        for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
          if (freshenerEntry.getKey().getFamily().equals(column.getFamily())) {
            collectedFresheners.put(freshenerEntry.getKey(), freshenerEntry.getValue());
          }
        }
      }
    }
    return ImmutableMap.copyOf(collectedFresheners);
  }

  /**
   * Create InternalFreshenerContext objects for the given set of Fresheners. These are fully
   * featured contexts which will be passed to
   * {@link KijiFreshnessPolicy#isFresh(org.kiji.schema.KijiRowData,
   * org.kiji.scoring.FreshenerContext)} and {@link ScoreFunction#score(org.kiji.schema.KijiRowData,
   * org.kiji.scoring.FreshenerContext)} and other per-request methods.
   *
   * @param clientRequest the data request which triggered the Freshener runs which require these
   *     contexts.
   * @param fresheners the Fresheners applicable to the client request, which will consume the
   *     contexts.
   * @return a mapping from attached column to InternalFreshenerContext corresponding to the input
   *     Fresheners.
   */
  private static ImmutableMap<KijiColumnName, InternalFreshenerContext> createFreshenerContexts(
      final KijiDataRequest clientRequest,
      final Map<KijiColumnName, Freshener> fresheners
  ) {
    final Map<KijiColumnName, InternalFreshenerContext> collectedContexts = Maps.newHashMap();

    for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
      final InternalFreshenerContext context = InternalFreshenerContext.create(
          clientRequest,
          freshenerEntry.getValue().mAttachedColumn,
          freshenerEntry.getValue().mParameters,
          Collections.<String, String>emptyMap(), // TODO add support for request time parameters.
          freshenerEntry.getValue().mFactory);
      collectedContexts.put(freshenerEntry.getKey(), context);
    }

    return ImmutableMap.copyOf(collectedContexts);
  }

  /**
   * Get a list of column names from a KijiDataRequest.
   *
   * @param request the request from which to get columns.
   * @return a list of column names from a KijiDataRequest.
   */
  private static ImmutableList<KijiColumnName> getColumnsFromRequest(
      final KijiDataRequest request
  ) {
    final List<KijiColumnName> collectedColumns = Lists.newArrayList();
    for (Column column : request.getColumns()) {
      collectedColumns.add(new KijiColumnName(column.getName()));
    }
    return ImmutableList.copyOf(collectedColumns);
  }

  /**
   * Get a Future for each Freshener from the request context which returns a boolean indicating
   * whether the Freshener wrote a value to the table necessitating a reread.
   *
   * @param requestContext context object representing all state relevant to a single freshening get
   *     request.
   * @return a Future for each Freshener from the request context.
   */
  private static ImmutableList<Future<Boolean>> getFuturesForFresheners(
      final FresheningRequestContext requestContext
  ) {
    final List<Future<Boolean>> collectedFutures =
        Lists.newArrayListWithCapacity(requestContext.mFresheners.size());

    for (Map.Entry<KijiColumnName, Freshener> entry : requestContext.mFresheners.entrySet()) {
      final InternalFreshenerContext context =
          requestContext.mFreshenerContexts.get(entry.getKey());

      final Future<KijiRowData> rowDataToCheckFuture;
      if (entry.getValue().mPolicy.shouldUseClientDataRequest(context)) {
        rowDataToCheckFuture = requestContext.mClientDataFuture;
      } else {
        rowDataToCheckFuture = getFuture(new TableReadCallable(
            requestContext.mReader,
            requestContext.mEntityId,
            entry.getValue().mPolicy.getDataRequest(context)));
      }
      final Future<Boolean> future = getFuture(new FreshenerCallable(
          requestContext,
          entry.getKey(),
          rowDataToCheckFuture));
      collectedFutures.add(future);
    }
    return ImmutableList.copyOf(collectedFutures);
  }

  /**
   * Get a Future for each EntityId in entityIds which represents the return value of a
   * {@link #get(org.kiji.schema.EntityId, org.kiji.schema.KijiDataRequest)} request made against
   * the given FreshKijiTableReader with the given KijiDataRequest.
   *
   * @param entityIds the rows to freshen.
   * @param dataRequest the data to retrieve from each row.
   * @param freshReader the FreshKijiTableReader to use to perform each freshening read.
   * @return a list of Futures corresponding to the values of freshening data on each row in
   *     entityIds.
   */
  private static ImmutableList<Future<KijiRowData>> getFuturesForEntities(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final FreshKijiTableReader freshReader
  ) {
    final List<Future<KijiRowData>> collectedFutures = Lists.newArrayList();

    for (EntityId entityId : entityIds) {
      collectedFutures.add(getFuture(new TableReadCallable(freshReader, entityId, dataRequest)));
    }

    return ImmutableList.copyOf(collectedFutures);
  }

  /**
   * Checks request context for writes and retrieves refreshed data from the table or cached stale
   * data from the clientDataFuture as appropriate.
   *
   * @param context context object representing all state relevant to a single freshening get
   *     request.  May be queried for the state of cached writes, the Future representing cached
   *     stale data, the KijiTableReader with which to perform reads if necessary, and the request's
   *     entityId and data request.
   * @return data from the specified row conforming to the specified data request.  Data will be
   *     fresh or stale depending on the state of Fresheners running for this request.
   * @throws IOException in case of an error reading from the table.
   */
  private static KijiRowData checkAndRead(
      final FresheningRequestContext context
  ) throws IOException {
    if (context.mAllowPartial && context.mHasReceivedWrites) {
      // If any writes have been cached, read from the table.
      return context.mReader.get(context.mEntityId, context.mClientDataRequest);
    }
    // If no writes have been cached or allowPartial is false, return stale data.
    return getStaleData(
        context.mClientDataFuture,
        context.mReader,
        context.mEntityId,
        context.mClientDataRequest);
  }

  /**
   * Gets cached stale data from the clientDataFuture if available. Falls back to reading from the
   * table if the clientDataFuture is not finished. Data is not guaranteed to be stale, but is
   * guaranteed to conform to the atomicity constraints set by partial freshening (i.e. if partial
   * freshening is disabled, this will return entirely fresh or entire stale data.)
   *
   * @param clientDataFuture asynchronously collected data for the client's requested entityId and
   *     data request.
   * @param reader the reader to use to read new data if the clientDataFuture is not finished.
   * @param entityId the entityId of the row to read if the clientDataFuture is not finished.
   * @param dataRequest an enumeration of the data to retrieve from the row if the clientDataFuture
   *     is not finished.
   * @return cached stale data if possible, otherwise the current state of the table. Returned data
   *     will conform to the atomicity guarantees provided by partial freshening, but this may
   *     return fresh data in some race conditions.
   * @throws IOException in case of an error reading from the table.
   */
  private static KijiRowData getStaleData(
      final Future<KijiRowData> clientDataFuture,
      final KijiTableReader reader,
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) throws IOException {
    if (clientDataFuture.isDone()) {
      return getFromFuture(clientDataFuture);
    } else {
      // If clientDataFuture is not ready to be retrieved we can only attempt to read data from the
      // table.  This data will still include exclusively fresh or stale data.
      return reader.get(entityId, dataRequest);
    }
  }

  // -----------------------------------------------------------------------------------------------
  // State.
  // -----------------------------------------------------------------------------------------------

  /** The current state of the reader (e.g. OPEN, CLOSED). */
  private final ReaderState mState;
  /** The table from which the reader reads. */
  private final KijiTable mTable;
  /** A Regular KijiTableReader used for retrieving data to check for freshness and score. */
  private final KijiTableReader mReader;
  /** The default time in milliseconds to wait for a freshening request to complete. */
  private final long mTimeout;
  /** A timer task which periodically calls this reader's {@link #rereadFreshenerRecords()}. */
  private final RereadTask mRereadTask;
  /** Whether this reader allows partially freshened data to be written. */
  private final boolean mAllowPartial;
  /** The buffered writer through which all fresheners run by this reader commit to the table. */
  private final MultiBufferedWriter mBufferedWriter;
  /** The KijiFreshnessManager used to retrieve updated Freshener records. */
  private final KijiFreshnessManager mFreshnessManager;
  /** All mutable state which may be modified by a called to {@link #rereadFreshenerRecords()}. */
  private volatile RereadableState mRereadableState;
  /** Level of statistics gathering (e.g. ALL, NONE). */
  private final StatisticGatheringMode mStatisticGatheringMode;
  /** Thread responsible for gathering and aggregating statistics. */
  private final StatisticsGathererThread mStatisticsGathererThread;
  /** Queue through which statistics about completed Fresheners are passed to the gatherer. */
  private final BlockingQueue<FreshenerSingleRunStatistics> mStatisticsQueue =
      new LinkedBlockingQueue<FreshenerSingleRunStatistics>();

  /**
   * Initializes a new InternalFreshKijiTableReader.
   *
   * @param table the KijiTable from which this reader will read and to which it will write.
   * @param timeout the time in milliseconds the reader should wait before returning stale data.
   * @param rereadPeriod the time in milliseconds between automatically rereading policy records.
   *     A value of 0 indicates no automatic rereads.
   * @param allowPartial whether to allow returning partially freshened data when available.
   * @param columnsToFreshen the set of columns which this reader will attempt to freshen.
   * @param statisticGatheringMode specifies what statistics to gather.
   * @param statisticsLoggingInterval time in milliseconds between automatic logging of statistics.
   *     0 indicates no automatic logging.
   * @throws IOException in case of an error reading from the metatable or setting up producers.
   */
  public InternalFreshKijiTableReader(
      final KijiTable table,
      final long timeout,
      final long rereadPeriod,
      final boolean allowPartial,
      final List<KijiColumnName> columnsToFreshen,
      final StatisticGatheringMode statisticGatheringMode,
      final long statisticsLoggingInterval
  ) throws IOException {
    // Initializing the reader state must be the first line of the constructor.
    mState = new ReaderState();

    mTable = table;
    // Opening a reader retains the table.
    mReader = table.openTableReader();
    mBufferedWriter = new MultiBufferedWriter(mTable);
    mTimeout = timeout;
    mAllowPartial = allowPartial;
    mFreshnessManager = KijiFreshnessManager.create(mTable.getKiji());
    final List<KijiColumnName> innerColumnsToFreshen = (null != columnsToFreshen)
        ? columnsToFreshen : Lists.<KijiColumnName>newArrayList();
    final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records = filterRecords(
        mFreshnessManager.retrieveFreshenerRecords(mTable.getName()), innerColumnsToFreshen);
    mRereadableState = new RereadableState(
        innerColumnsToFreshen,
        records,
        createFresheners(records));
    mStatisticGatheringMode = statisticGatheringMode;

    mStatisticsGathererThread = startStatisticsGatherer(statisticsLoggingInterval);
    mRereadTask = startPeriodicRereader(rereadPeriod);

    // Opening the reader must be the last line of the constructor.
    mState.finishInitializingAndOpen();
  }

  /**
   * Start a new StatisticsGathererThread if the reader's statistics gathering mode is not NONE.
   *
   * @param statisticsLoggingInterval time in milliseconds between logging statistics. 0 indicates
   *     no automatic logging.
   * @return a new StatisticsGathererThread, already started.
   */
  private StatisticsGathererThread startStatisticsGatherer(
      final long statisticsLoggingInterval
  ) {
    final StatisticsGathererThread gatherer;
    if (StatisticGatheringMode.NONE != mStatisticGatheringMode) {
      gatherer = new StatisticsGathererThread(statisticsLoggingInterval);
      gatherer.start();
    } else {
      gatherer = null;
    }
    return gatherer;
  }

  /**
   * Start a new periodic reread task with the given period.
   *
   * @param rereadPeriod the period in milliseconds between rereads.
   * @return a new periodic reread task with the given period, already started.
   */
  private RereadTask startPeriodicRereader(
      final long rereadPeriod
  ) {
    final RereadTask task;
    if (rereadPeriod > 0) {
      final Timer rereadTimer = new Timer();
      task = new RereadTask(rereadPeriod);
      rereadTimer.scheduleAtFixedRate(task, rereadPeriod, rereadPeriod);
    } else if (rereadPeriod == 0) {
      task = null;
    } else {
      throw new IllegalArgumentException(
          String.format("Reread time must be >= 0, found: %d", rereadPeriod));
    }
    return task;
  }

  // -----------------------------------------------------------------------------------------------
  // Public interface.
  // -----------------------------------------------------------------------------------------------

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(
      final EntityId entityId,
      final KijiDataRequest dataRequest
  ) throws IOException {
    return get(entityId, dataRequest, mTimeout);
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowData get(
      final EntityId entityId,
      final KijiDataRequest dataRequest,
      final long timeout
  ) throws IOException {
    mState.requireState(State.OPEN);

    // Get the start time for the request.
    final long startTime = System.nanoTime();

    // Get a snapshot of the rereadable state.
    final RereadableState rereadableState = mRereadableState;

    // Collect the Fresheners and Records applicable to this request.
    final ImmutableList<KijiColumnName> requestColumns = getColumnsFromRequest(dataRequest);
    final ImmutableMap<KijiColumnName, Freshener> fresheners =
        filterFresheners(requestColumns, rereadableState.mFresheners);
    final ImmutableMap<KijiColumnName, KijiFreshenerRecord> records =
        filterRecords(rereadableState.mFreshenerRecords, requestColumns);
    // If there are no Fresheners attached to the requested columns, return the requested data.
    if (fresheners.isEmpty()) {
      return mReader.get(entityId, dataRequest);
    } else {
      // Retain the Fresheners so that they cannot be cleaned up while in use.
      for (Map.Entry<KijiColumnName, Freshener> freshenerEntry : fresheners.entrySet()) {
        freshenerEntry.getValue().retain();
      }
    }

    final ImmutableMap<KijiColumnName, InternalFreshenerContext> freshenerContexts =
        createFreshenerContexts(dataRequest, fresheners);

    final Future<KijiRowData> clientDataFuture =
        getFuture(new TableReadCallable(mReader, entityId, dataRequest));

    final FresheningRequestContext requestContext = new FresheningRequestContext(
        startTime,
        fresheners,
        freshenerContexts,
        records,
        mReader,
        entityId,
        dataRequest,
        clientDataFuture,
        mBufferedWriter,
        mAllowPartial,
        mStatisticsQueue);

    final ImmutableList<Future<Boolean>> futures = getFuturesForFresheners(requestContext);

    final Future<List<Boolean>> superFuture =
        getFuture(new FutureAggregatingCallable<Boolean>(futures));

    try {
      if (getFromFuture(superFuture, timeout).contains(true)) {
        // If all Fresheners return in time and at least one has written a new value, read from the
        // table.
        return mReader.get(entityId, dataRequest);
      } else {
        // If all Fresheners return in time, but none have written new values, do not read from the
        // table.
        try {
          return getFromFuture(clientDataFuture, 0L);
        } catch (TimeoutException te) {
          // If client data is not immediately available, read from the table.
          return mReader.get(entityId, dataRequest);
        }
      }
    } catch (TimeoutException te) {
      requestContext.timeOut();
      // If superFuture times out, read partially freshened data from the table or return the cached
      // data based on whether partial freshness is allowed.
      return checkAndRead(requestContext);
    }
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest
  ) throws IOException {
    return bulkGet(entityIds, dataRequest, mTimeout);
  }

  /** {@inheritDoc} */
  @Override
  public List<KijiRowData> bulkGet(
      final List<EntityId> entityIds,
      final KijiDataRequest dataRequest,
      final long timeout
  ) throws IOException {
    mState.requireState(State.OPEN);

    final ImmutableList<Future<KijiRowData>> futures =
        getFuturesForEntities(entityIds, dataRequest, this);

    final Future<List<KijiRowData>> superDuperFuture =
        getFuture(new FutureAggregatingCallable<KijiRowData>(futures));

    try {
      return getFromFuture(superDuperFuture, timeout);
    } catch (TimeoutException te) {
      // If the request times out, read from the table.
      return mReader.bulkGet(entityIds, dataRequest);
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      final KijiDataRequest dataRequest
  ) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public KijiRowScanner getScanner(
      final KijiDataRequest dataRequest,
      final KijiScannerOptions scannerOptions
  ) throws IOException {
    throw new UnsupportedOperationException("Freshening Kiji table reader cannot create a row"
        + " scanner");
  }

  /** {@inheritDoc} */
  @Override
  public FreshKijiTableReaderStatistics getStatistics() {
    return mStatisticsGathererThread.getStatistics();
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   *   This implementation of FreshKijiTableReader ignores the withPreload parameter and always
   *   proactively instantiates objects needed for freshening.
   * </p>
   */
  @Override
  public void rereadFreshenerRecords() throws IOException {
    rereadFreshenerRecords(mRereadableState.mColumnsToFreshen);
  }

  @Override
  public void rereadFreshenerRecords(
      final List<KijiColumnName> columnsToFreshen
  ) throws IOException {
    mState.requireState(State.OPEN);
    // Collect and filter the current state of the meta table.
    final ImmutableMap<KijiColumnName, KijiFreshenerRecord> newRecords = filterRecords(
        mFreshnessManager.retrieveFreshenerRecords(mTable.getName()), columnsToFreshen);

    final Map<KijiColumnName, Freshener> oldFresheners = Maps.newHashMap();

    for (Map.Entry<KijiColumnName, Freshener> entry : mRereadableState.mFresheners.entrySet()) {
      if (!newRecords.containsKey(entry.getKey())) {
        // If the column no longer has a freshness policy record, release the old capsule.
        entry.getValue().release();
      } else {
        if (newRecords.get(entry.getKey())
            != mRereadableState.mFreshenerRecords.get(entry.getKey())) {
          // If the column still has a freshness policy record and that record has changed, release
          // the old capsule.
          entry.getValue().release();
        } else {
          // If the record has not changed, keep the old Freshener.
          oldFresheners.put(entry.getKey(), entry.getValue());
        }
      }
    }

    mRereadableState = new RereadableState(
        columnsToFreshen,
        newRecords,
        fillFresheners(newRecords, ImmutableMap.copyOf(oldFresheners))
    );
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    // beginClosing() must be the first line of close().
    mState.beginClosing();

    if (null != mStatisticsGathererThread) {
      mStatisticsGathererThread.shutdown();
    }

    if (null != mRereadTask) {
      mRereadTask.cancel();
    }

    mFreshnessManager.close();
    mReader.close();
    mBufferedWriter.close();

    // finishClosing() must be the last line of close().
    mState.finishClosing();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(InternalFreshKijiTableReader.class)
        .add("table", mTable)
        .add("timeout", mTimeout)
        .add("automatic_reread_period",
            (null != mRereadTask) ? mRereadTask.getRereadPeriod() : "no_automatic_reread")
        .add("allows_partial_freshening", mAllowPartial)
        .add("freshens_columns", Joiner.on(", ").join(mRereadableState.mColumnsToFreshen))
        .add("statistics_gathering_mode", mStatisticGatheringMode)
        .add("statistics_logging_period", (null != mStatisticsGathererThread)
            ? mStatisticsGathererThread.getLoggingInterval() : "no_statistics_gathered")
        .addValue(mState)
        .toString();
  }
}
