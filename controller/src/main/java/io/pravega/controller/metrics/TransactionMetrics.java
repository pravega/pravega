/**
 * Copyright Pravega Authors.
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
package io.pravega.controller.metrics;

import com.google.common.base.Preconditions;
import io.pravega.shared.metrics.OpStatsLogger;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.shared.MetricsNames.ABORTING_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION_SEGMENTS_LATENCY;
import static io.pravega.shared.MetricsNames.COMMITTING_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_SEGMENTS_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_SEGMENTS_LATENCY;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_GEN_ID_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_ADD_TO_INDEX_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_ADD_TIMEOUT_SVC_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_IN_STORE_LATENCY;
import static io.pravega.shared.MetricsNames.COMMITTING_TRANSACTION_ADD_TO_INDEX_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_RECORD_OFFSETS_LATENCY;
import static io.pravega.shared.MetricsNames.COMMITTING_TRANSACTION_REMOVE_TIMEOUT_SVC_LATENCY;
import static io.pravega.shared.MetricsNames.COMMITTING_TRANSACTION_SEAL_LATENCY;
import static io.pravega.shared.MetricsNames.COMMITTING_TRANSACTION_WRITE_EVENT_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_START_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_COMPLETE_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_ROLLOVER_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_BATCH_COUNT;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_ORDERER_BATCH_COMMITTING_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_ORDERER_BATCH_PURGE_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_BATCH_CREATE_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_SEGMENTS_MERGE_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_LATENCY_AVG;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_SEGMENTS_MERGE_LATENCY_AVG;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;
import static io.pravega.shared.MetricsTags.transactionTags;

/**
 * Class to encapsulate the logic to report Controller service metrics for Transactions.
 */
public final class TransactionMetrics extends AbstractControllerMetrics {

    private static final AtomicReference<TransactionMetrics> INSTANCE = new AtomicReference<>();

    private final OpStatsLogger createTransactionLatency;
    private final OpStatsLogger createTransactionSegmentsLatency;
    private final OpStatsLogger createTxnGenIdLatency;
    private final OpStatsLogger createTxnAddTxnToIndexLatency;
    private final OpStatsLogger createTxnCreateInStoreLatency;
    private final OpStatsLogger createTxnAddToTimeoutSvcLatency;
    private final OpStatsLogger commitTransactionLatency;
    private final OpStatsLogger commitTransactionLatencyAvg;
    private final OpStatsLogger commitTransactionSegmentsLatency;
    private final OpStatsLogger commitTransactionSegmentsLatencyAvg;
    private final OpStatsLogger committingTxnAddToIndexLatency;
    private final OpStatsLogger committingTxnSealLatency;
    private final OpStatsLogger committingTxnWriteEventLatency;
    private final OpStatsLogger committingTxnRemoveTimeoutSvcLatency;
    private final OpStatsLogger committingTransactionLatency;
    private final OpStatsLogger commitTxnStartLatency;
    private final OpStatsLogger commitTxnRecordOffsetsLatency;
    private final OpStatsLogger commitTxnRolloverLatency;
    private final OpStatsLogger commitTxnCompleteLatency;

    private final OpStatsLogger commitTxnBatchCreateOrdererFetchLatency;
    private final OpStatsLogger commitTxnBatchCreateLatency;
    private final OpStatsLogger commitTxnOrdererBatchPurgeStaleLatency;

    private final OpStatsLogger abortTransactionLatency;
    private final OpStatsLogger abortTransactionSegmentsLatency;
    private final OpStatsLogger abortingTransactionLatency;
    //COMMIT_TRANSACTION_SEGMENTS_MERGE_LATENCY
    private final OpStatsLogger commitTxnSegmentsMergeLatency;



    private TransactionMetrics() {
        createTransactionLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_LATENCY);
        createTransactionSegmentsLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_SEGMENTS_LATENCY);
        commitTransactionLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_LATENCY);
        commitTransactionLatencyAvg = STATS_LOGGER.createStats(COMMIT_TRANSACTION_LATENCY_AVG);
        commitTransactionSegmentsLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_SEGMENTS_LATENCY);
        commitTransactionSegmentsLatencyAvg = STATS_LOGGER.createStats(COMMIT_TRANSACTION_SEGMENTS_MERGE_LATENCY_AVG);
        committingTransactionLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_LATENCY);
        abortTransactionLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_LATENCY);
        abortTransactionSegmentsLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_SEGMENTS_LATENCY);
        abortingTransactionLatency = STATS_LOGGER.createStats(ABORTING_TRANSACTION_LATENCY);
        createTxnGenIdLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_GEN_ID_LATENCY);
        createTxnAddTxnToIndexLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_ADD_TO_INDEX_LATENCY);
        createTxnCreateInStoreLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_IN_STORE_LATENCY);
        createTxnAddToTimeoutSvcLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_ADD_TIMEOUT_SVC_LATENCY);

        committingTxnAddToIndexLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_ADD_TO_INDEX_LATENCY);
        committingTxnSealLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_SEAL_LATENCY);
        committingTxnWriteEventLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_WRITE_EVENT_LATENCY);
        committingTxnRemoveTimeoutSvcLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_REMOVE_TIMEOUT_SVC_LATENCY);

        commitTxnStartLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_START_LATENCY);
        commitTxnRecordOffsetsLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_RECORD_OFFSETS_LATENCY);
        commitTxnRolloverLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_ROLLOVER_LATENCY);
        commitTxnCompleteLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_COMPLETE_LATENCY);

        commitTxnBatchCreateOrdererFetchLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_ORDERER_BATCH_COMMITTING_LATENCY);
        commitTxnBatchCreateLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_BATCH_CREATE_LATENCY);
        commitTxnOrdererBatchPurgeStaleLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_ORDERER_BATCH_PURGE_LATENCY);
        commitTxnSegmentsMergeLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_SEGMENTS_MERGE_LATENCY);
    }

    /**
     * Mandatory call to initialize the singleton object.
     */
    public static synchronized void initialize() {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new TransactionMetrics());
        }
    }

    /**
     * Get the singleton {@link TransactionMetrics} instance. It is mandatory to call initialize before invoking this method.
     *
     * @return StreamMetrics instance.
     */
    public static TransactionMetrics getInstance() {
        Preconditions.checkState(INSTANCE.get() != null, "You need call initialize before using this class.");
        return INSTANCE.get();
    }

    /**
     * This method increments the global and Stream-related counters of created Transactions and reports the latency of
     * the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the create Transaction operation.
     */
    public void createTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION, 1, streamTags(scope, streamName));
        createTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular create Transaction.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionSegments(Duration latency) {
        createTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of adding txn event to Index.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionAddToIndex(Duration latency) {
        createTxnAddTxnToIndexLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of Generating Transaction Id.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionGenId(Duration latency) {
        createTxnGenIdLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency to add Transaction to Stream Store.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionInStore(Duration latency) {
        createTxnCreateInStoreLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency to add Transaction to timeout service.
     *
     * @param latency      Time elapsed to create the segments related to the created transaction.
     */
    public void createTransactionAddTimeoutSvc(Duration latency) {
        createTxnAddToTimeoutSvcLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of failed Transaction create operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     */
    public void createTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method accounts for the time taken for a client to set a Transaction to COMMITTING state.
     *
     * @param latency    Latency of the abort Transaction operation.
     */
    public void committingTransaction(Duration latency) {
        committingTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method records latency for adding Txn to Index in the Commit API.
     *
     * @param latency    Latency of the abort Transaction operation.
     */
    public void committingTransactionAddToIndex(Duration latency) {
        committingTxnAddToIndexLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method records latency for sealing Txn in the Commit API.
     *
     * @param latency    Latency for sealing Txn in the Commit API.
     */
    public void committingTransactionSeal(Duration latency) {
        committingTxnSealLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method records latency for Writing Event for a txn in COMMITTING/ABORTING State.
     *
     * @param latency    Latency for Writing Event for a txn in COMMITTING/ABORTING State.
     */
    public void committingTransactionWriteEvent(Duration latency) {
        committingTxnWriteEventLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method records latency for removing txn from Timeout Service.
     *
     * @param latency    Latency for removing txn from Timeout Service
     */
    public void committingTransactionRemoveTimeoutSvc(Duration latency) {
        committingTxnRemoveTimeoutSvcLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of committed Transactions and reports the latency
     * of the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the commit Transaction operation.
     */
    public void commitTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(COMMIT_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION, 1, streamTags(scope, streamName));
        commitTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    public void commitTransactionAvg(Duration latency) {
        commitTransactionLatencyAvg.reportSuccessValue(latency.toMillis());
    }

    public void commitTransactionSegmentMergeAvg(Duration latency) {
        commitTransactionSegmentsLatencyAvg.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular commit Transaction.
     *
     * @param latency      Time elapsed to merge the segments related to the committed transaction.
     */
    public void commitTransactionSegments(Duration latency) {
        commitTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency for merging all segments for all Transactions handled by a Commit event.
     *
     * @param latency      Time elapsed to merge the segments related to the committed transaction.
     */
    public void commitSegmentsMerge(Duration latency) {
        commitTxnSegmentsMergeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency for metadata change for Starting Transaction Commit as part of Commit Event processing.
     *
     * @param latency      Latency for StartTransactionCommit API
     */
    public void commitTransactionStart(Duration latency) {
        commitTxnStartLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency for recording commit offsets post segment merges.
     *
     * @param latency      Latency for recording commit offsets post segment merge.
     */
    public void commitTransactionRecordOffsets(Duration latency) {
        commitTxnRecordOffsetsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency for txn rollover in Commit Event Processing.
     *
     * @param latency      Latency for txn rollover in Commit Event Processing
     */
    public void commitTransactionRollover(Duration latency) {
        commitTxnRolloverLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency for completing Transaction Commit.
     *
     * @param latency      Latency for completing Transaction Commit.
     */
    public void commitTransactionComplete(Duration latency) {
        commitTxnCompleteLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the number of transactions committed by this Commit Event.
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param txnsInBatchCount   Number of transactions committed as part of this Commit Event Processing.
     */
    public void reportCommitTransactionBatchCount(String scope, String streamName, int txnsInBatchCount) {
        DYNAMIC_LOGGER.reportGaugeValue(COMMIT_TRANSACTION_BATCH_COUNT, txnsInBatchCount, streamTags(scope, streamName));
    }

    public void reportTransactionZkOrdererFetch(Duration latency) {
        commitTxnBatchCreateOrdererFetchLatency.reportSuccessValue(latency.toMillis());
    }

    public void reportTransactionBatchCreate(Duration latency) {
        commitTxnBatchCreateLatency.reportSuccessValue(latency.toMillis());
    }

    public void reportTransactionZkOrdererPurgeStale(Duration latency) {
        commitTxnOrdererBatchPurgeStaleLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed commit operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param txnId      Transaction id.
     */
    public void commitTransactionFailed(String scope, String streamName, String txnId) {
        commitTransactionFailed(scope, streamName);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION_FAILED, 1, transactionTags(scope, streamName, txnId));
    }

    /**
     * This method increments the global, Stream-related counters of failed commit operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     */
    public void commitTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(COMMIT_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method accounts for the time taken for a client to set a Transaction to ABORTING state.
     *
     * @param latency    Latency of the abort Transaction operation.
     */
    public void abortingTransaction(Duration latency) {
        abortingTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of aborted Transactions and reports the latency
     * of the operation.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param latency    Latency of the abort Transaction operation.
     */
    public void abortTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ABORT_TRANSACTION), 1);
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION, 1, streamTags(scope, streamName));
        abortTransactionLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method reports the latency of managing segments for a particular abort Transaction.
     *
     * @param latency      Time elapsed to delete the segments related to the aborted transaction.
     */
    public void abortTransactionSegments(Duration latency) {
        abortTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed abort operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     * @param txnId      Transaction id.
     */
    public void abortTransactionFailed(String scope, String streamName, String txnId) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ABORT_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1, transactionTags(scope, streamName, txnId));
    }

    /**
     * This method reports the current number of open Transactions for a Stream.
     *
     * @param scope                 Scope.
     * @param streamName            Name of the Stream.
     * @param ongoingTransactions   Number of open Transactions in the Stream.
     */
    public static void reportOpenTransactions(String scope, String streamName, int ongoingTransactions) {
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, ongoingTransactions, streamTags(scope, streamName));
    }

    public static synchronized void reset() {
        TransactionMetrics old = INSTANCE.get();
        if (old != null) {
            old.createTransactionLatency.close();
            old.createTransactionSegmentsLatency.close();
            old.commitTransactionLatency.close();
            old.commitTransactionSegmentsLatency.close();
            old.committingTransactionLatency.close();
            old.abortTransactionLatency.close();
            old.abortTransactionSegmentsLatency.close();
            old.abortingTransactionLatency.close();
            old.committingTxnAddToIndexLatency.close();
            old.committingTxnSealLatency.close();
            old.committingTxnWriteEventLatency.close();
            old.committingTxnRemoveTimeoutSvcLatency.close();
            old.commitTxnStartLatency.close();
            old.commitTxnRolloverLatency.close();
            old.commitTxnCompleteLatency.close();
            old.createTxnGenIdLatency.close();
            old.createTxnAddToTimeoutSvcLatency.close();
            old.createTxnCreateInStoreLatency.close();
        }
        INSTANCE.set(new TransactionMetrics());
    }
}
