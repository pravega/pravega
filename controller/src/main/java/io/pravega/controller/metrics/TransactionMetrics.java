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
import static io.pravega.shared.MetricsNames.CONTROLLER_EVENT_PROCESSOR_COMMIT_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_SEGMENTS_LATENCY;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;

/**
 * Class to encapsulate the logic to report Controller service metrics for Transactions.
 */
public final class TransactionMetrics extends AbstractControllerMetrics {

    @SuppressWarnings("MatchXpath")
    private static final AtomicReference<TransactionMetrics> INSTANCE = new AtomicReference<>();

    private final OpStatsLogger createTransactionLatency;
    private final OpStatsLogger createTransactionSegmentsLatency;
    private final OpStatsLogger commitTransactionLatency;
    private final OpStatsLogger commitTransactionSegmentsLatency;
    private final OpStatsLogger committingTransactionLatency;
    private final OpStatsLogger abortTransactionLatency;
    private final OpStatsLogger abortTransactionSegmentsLatency;
    private final OpStatsLogger abortingTransactionLatency;
    private final OpStatsLogger controllerEventProcessorCommitTransactionLatency;

    private TransactionMetrics() {
        createTransactionLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_LATENCY);
        createTransactionSegmentsLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_SEGMENTS_LATENCY);
        commitTransactionLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_LATENCY);
        commitTransactionSegmentsLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_SEGMENTS_LATENCY);
        committingTransactionLatency = STATS_LOGGER.createStats(COMMITTING_TRANSACTION_LATENCY);
        abortTransactionLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_LATENCY);
        abortTransactionSegmentsLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_SEGMENTS_LATENCY);
        abortingTransactionLatency = STATS_LOGGER.createStats(ABORTING_TRANSACTION_LATENCY);
        controllerEventProcessorCommitTransactionLatency = STATS_LOGGER.createStats(CONTROLLER_EVENT_PROCESSOR_COMMIT_TRANSACTION_LATENCY);
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

    /**
     * This method reports the latency of managing segments for a particular commit Transaction.
     *
     * @param latency      Time elapsed to merge the segments related to the committed transaction.
     */
    public void commitTransactionSegments(Duration latency) {
        commitTransactionSegmentsLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global and Stream-related counters of failed commit operations.
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
     * This method increments the global and Stream-related counters of failed abort operations.
     *
     * @param scope      Scope.
     * @param streamName Name of the Stream.
     */
    public void abortTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ABORT_TRANSACTION_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method reports the latency of ControllerEventProcessor commitTransaction event processing.
     *
     * @param latency       Latency of the controllerEventProcessorCommitTransactionLatency event operation.
     */
    public void controllerEventProcessorCommitTransactionLatency(Duration latency) {
        controllerEventProcessorCommitTransactionLatency.reportSuccessValue(latency.toMillis());
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
            old.controllerEventProcessorCommitTransactionLatency.close();
        }
        INSTANCE.set(new TransactionMetrics());
    }
}
