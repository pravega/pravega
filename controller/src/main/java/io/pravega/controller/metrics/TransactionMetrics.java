/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.metrics;

import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;

import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.ABORT_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.COMMIT_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_TRANSACTION_LATENCY;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.nameFromStream;
import static io.pravega.shared.MetricsNames.nameFromTransaction;

/**
 * Class to encapsulate the logic to report Controller service metrics for Transactions.
 */
public final class TransactionMetrics implements AutoCloseable {

    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("controller");

    private final OpStatsLogger createTransactionLatency = STATS_LOGGER.createStats(CREATE_TRANSACTION_LATENCY);
    private final OpStatsLogger commitTransactionLatency = STATS_LOGGER.createStats(COMMIT_TRANSACTION_LATENCY);
    private final OpStatsLogger abortTransactionLatency = STATS_LOGGER.createStats(ABORT_TRANSACTION_LATENCY);

    /**
     * This method increments the global and Stream-related counters of created Transactions and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the create Transaction operation.
     */
    public void createTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(CREATE_TRANSACTION, scope, streamName), 1);
        createTransactionLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the global and Stream-related counters of failed Transaction create operations.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void createTransactionFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_TRANSACTION_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(CREATE_TRANSACTION_FAILED, scope, streamName), 1);
    }

    /**
     * This method increments the global and Stream-related counters of committed Transactions and reports the latency
     * of the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the commit Transaction operation.
     */
    public void commitTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(COMMIT_TRANSACTION, scope, streamName), 1);
        commitTransactionLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed commit operations.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param txnId         Transaction id.
     */
    public void commitTransactionFailed(String scope, String streamName, String txnId) {
        DYNAMIC_LOGGER.incCounterValue(COMMIT_TRANSACTION_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(COMMIT_TRANSACTION_FAILED, scope, streamName), 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromTransaction(COMMIT_TRANSACTION_FAILED, scope, streamName, txnId), 1);
    }

    /**
     * This method increments the global and Stream-related counters of aborted Transactions and reports the latency
     * of the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the abort Transaction operation.
     */
    public void abortTransaction(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(ABORT_TRANSACTION, scope, streamName), 1);
        abortTransactionLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the global, Stream-related and Transaction-related counters of failed abort operations.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param txnId         Transaction id.
     */
    public void abortTransactionFailed(String scope, String streamName, String txnId) {
        DYNAMIC_LOGGER.incCounterValue(ABORT_TRANSACTION_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(ABORT_TRANSACTION_FAILED, scope, streamName), 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromTransaction(ABORT_TRANSACTION_FAILED, scope, streamName, txnId), 1);
    }

    /**
     * This method reports the current number of open Transactions for a Stream.
     *
     * @param scope                 Scope.
     * @param streamName            Name of the Stream.
     * @param ongoingTransactions   Number of open Transactions in the Stream.
     */
    public void reportOpenTransactions(String scope, String streamName, int ongoingTransactions) {
        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), ongoingTransactions);
    }

    @Override
    public void close() {
        createTransactionLatency.close();
        commitTransactionLatency.close();
        abortTransactionLatency.close();
    }
}
