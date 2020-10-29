/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.metrics;

import com.google.common.base.Preconditions;
import io.pravega.shared.metrics.OpStatsLogger;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.shared.MetricsNames.CREATE_SCOPE;
import static io.pravega.shared.MetricsNames.CREATE_SCOPE_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_SCOPE_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_SCOPE_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_STREAM;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.OPEN_TRANSACTIONS;
import static io.pravega.shared.MetricsNames.RETENTION_FREQUENCY;
import static io.pravega.shared.MetricsNames.SEAL_STREAM;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.SEAL_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.SEGMENTS_MERGES;
import static io.pravega.shared.MetricsNames.SEGMENTS_SPLITS;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.TRUNCATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_STREAM_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE_LATENCY;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE;
import static io.pravega.shared.MetricsNames.KVTABLE_SEGMENTS_COUNT;
import static io.pravega.shared.MetricsNames.CREATE_KVTABLE_FAILED;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE_LATENCY;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE;
import static io.pravega.shared.MetricsNames.DELETE_KVTABLE_FAILED;
import static io.pravega.shared.MetricsNames.ADD_SUBSCRIBER_LATENCY;
import static io.pravega.shared.MetricsNames.REMOVE_SUBSCRIBER_LATENCY;
import static io.pravega.shared.MetricsNames.ADD_SUBSCRIBER;
import static io.pravega.shared.MetricsNames.ADD_SUBSCRIBER_FAILED;
import static io.pravega.shared.MetricsNames.REMOVE_SUBSCRIBER;
import static io.pravega.shared.MetricsNames.REMOVE_SUBSCRIBER_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER_FAILED;
import static io.pravega.shared.MetricsNames.UPDATE_SUBSCRIBER_LATENCY;


import static io.pravega.shared.MetricsNames.globalMetricName;
import static io.pravega.shared.MetricsTags.streamTags;

/**
 * Class to encapsulate the logic to report Controller service metrics for Streams.
 */
public final class StreamMetrics extends AbstractControllerMetrics {

    private static final AtomicReference<StreamMetrics> INSTANCE = new AtomicReference<>();

    private final OpStatsLogger createStreamLatency;
    private final OpStatsLogger deleteStreamLatency;
    private final OpStatsLogger sealStreamLatency;
    private final OpStatsLogger updateStreamLatency;
    private final OpStatsLogger addSubscriberLatency;
    private final OpStatsLogger truncateStreamLatency;
    private final OpStatsLogger removeSubscriberLatency;
    private final OpStatsLogger updateSubscriberLatency;
    private final OpStatsLogger createKeyValueTableLatency;
    private final OpStatsLogger deleteKeyValueTableLatency;

    private final OpStatsLogger createScopeLatency;
    private final OpStatsLogger deleteScopeLatency;

    private StreamMetrics() {
        createStreamLatency = STATS_LOGGER.createStats(CREATE_STREAM_LATENCY);
        deleteStreamLatency = STATS_LOGGER.createStats(DELETE_STREAM_LATENCY);
        sealStreamLatency = STATS_LOGGER.createStats(SEAL_STREAM_LATENCY);
        updateStreamLatency = STATS_LOGGER.createStats(UPDATE_STREAM_LATENCY);
        truncateStreamLatency = STATS_LOGGER.createStats(TRUNCATE_STREAM_LATENCY);
        addSubscriberLatency = STATS_LOGGER.createStats(ADD_SUBSCRIBER_LATENCY);
        removeSubscriberLatency = STATS_LOGGER.createStats(REMOVE_SUBSCRIBER_LATENCY);
        createScopeLatency = STATS_LOGGER.createStats(CREATE_SCOPE_LATENCY);
        deleteScopeLatency = STATS_LOGGER.createStats(DELETE_SCOPE_LATENCY);
        createKeyValueTableLatency = STATS_LOGGER.createStats(CREATE_KVTABLE_LATENCY);
        deleteKeyValueTableLatency = STATS_LOGGER.createStats(DELETE_KVTABLE_LATENCY);
        updateSubscriberLatency = STATS_LOGGER.createStats(UPDATE_SUBSCRIBER_LATENCY);
    }

    /**
     * Mandatory call to initialize the singleton object.
     */
    public static synchronized void initialize() {
        if (INSTANCE.get() == null) {
            INSTANCE.set(new StreamMetrics());
        }
    }

    /**
     * Get the singleton {@link StreamMetrics} instance. It is mandatory to call initialize before invoking this method.
     *
     * @return StreamMetrics instance.
     */
    public static StreamMetrics getInstance() {
        Preconditions.checkState(INSTANCE.get() != null, "You need call initialize before using this class.");
        return INSTANCE.get();
    }

    /**
     * This method increments the global and KeyValueTable-specific counters of KeyValueTable creations,
     * initializes other kvt-specific metrics and reports the latency of the operation.
     *
     * @param scope             Scope.
     * @param kvtName           Name of the KeyValueTable.
     * @param minNumSegments    Initial number of segments for the KeyValueTable.
     * @param latency           Latency of the create KeyValueTable operation.
     */
    public void createKeyValueTable(String scope, String kvtName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_KVTABLE, 1);
        DYNAMIC_LOGGER.reportGaugeValue(KVTABLE_SEGMENTS_COUNT, minNumSegments, streamTags(scope, kvtName));
        createKeyValueTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of failed KeyValueTable creations in the system as well as the failed creation
     * attempts for this specific KeyValueTable.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     */
    public void createKeyValueTableFailed(String scope, String kvtName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_KVTABLE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_KVTABLE_FAILED, 1, streamTags(scope, kvtName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream deletions and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     * @param latency       Latency of the deleteKeyValueTable
     *                      operation.
     */
    public void deleteKeyValueTable(String scope, String kvtName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_KVTABLE, 1);
        deleteKeyValueTableLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param kvtName       Name of the KeyValueTable.
     */
    public void deleteKeyValueTableFailed(String scope, String kvtName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_KVTABLE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_KVTABLE_FAILED, 1, streamTags(scope, kvtName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream creations, initializes other
     * stream-specific metrics and reports the latency of the operation.
     *
     * @param scope             Scope.
     * @param streamName        Name of the Stream.
     * @param minNumSegments    Initial number of segments for the Stream.
     * @param latency           Latency of the createStream operation.
     */
    public void createStream(String scope, String streamName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM, 1);
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0, streamTags(scope, streamName));
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_COUNT, minNumSegments, streamTags(scope, streamName));
        createStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of Scope creations and reports the latency of the operation.
     *
     * @param latency           Latency of the createStream operation.
     */
    public void createScope(Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_SCOPE, 1);
        createScopeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of failed Stream creations in the system as well as the failed creation
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void createStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global counter of failed Scope creations in the system as well as the failed creation
     * attempts for this specific Scope.
     *
     * @param scope         Scope.
     */
    public void createScopeFailed(String scope) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CREATE_SCOPE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(CREATE_SCOPE_FAILED, 1, streamTags(scope, ""));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream deletions and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the deleteStream operation.
     */
    public void deleteStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM, 1);
        deleteStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the global counter of Scope deletions and reports the latency of the operation.
     *
     * @param latency       Latency of the deleteStream operation.
     */
    public void deleteScope(Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_SCOPE, 1);
        deleteScopeLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void deleteStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     */
    public void deleteScopeFailed(String scope) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(DELETE_SCOPE_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(DELETE_SCOPE_FAILED, 1, streamTags(scope, ""));
    }

    /**
     * This method increments the global and Stream-specific counters of seal Stream operations, set the number of open
     * Transactions to 0, and reports the latency of the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the sealStream operation.
     */
    public void sealStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM, 1);
        DYNAMIC_LOGGER.reportGaugeValue(OPEN_TRANSACTIONS, 0, streamTags(scope, streamName));
        sealStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream seal operations in the system as well as the failed seal
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void sealStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(SEAL_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream updates and reports the latency of the
     * operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the updateStream operation.
     */
    public void updateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_STREAM), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM, 1, streamTags(scope, streamName));
        updateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream update operations in the system as well as the failed
     * update attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void updateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters for addSubscriber operation on a Stream
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the addSubscriber operation.
     */
    public void addSubscriber(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ADD_SUBSCRIBER), 1);
        DYNAMIC_LOGGER.incCounterValue(ADD_SUBSCRIBER, 1, streamTags(scope, streamName));
        addSubscriberLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed addSubscriber operation attempts on a Stream.
     *
     * @param scope         Scope Name.
     * @param streamName    Stream Name.
     */
    public void addSubscriberFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(ADD_SUBSCRIBER_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(ADD_SUBSCRIBER_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters for removeSubscriber operation on a Stream
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the removeSubscriber operation.
     */
    public void deleteSubscriber(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(REMOVE_SUBSCRIBER), 1);
        DYNAMIC_LOGGER.incCounterValue(REMOVE_SUBSCRIBER, 1, streamTags(scope, streamName));
        removeSubscriberLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed removeSubscriber operation attempts on a Stream.
     *
     * @param scope         Scope Name.
     * @param streamName    Stream Name.
     */
    public void deleteSubscriberFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(REMOVE_SUBSCRIBER_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(REMOVE_SUBSCRIBER_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters for updateTruncationSC operation on a Stream
     * and reports the latency.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the updateTruncationStreamCut operation.
     */
    public void updateTruncationSC(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_SUBSCRIBER), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_SUBSCRIBER, 1, streamTags(scope, streamName));
        updateSubscriberLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter for failed removeSubscriber operation attempts on a Stream.
     *
     * @param scope         Scope Name.
     * @param streamName    Stream Name.
     */
    public void updateTruncationSCFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(UPDATE_SUBSCRIBER_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(UPDATE_SUBSCRIBER_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the global and Stream-specific counters of Stream truncations and reports the latency of
     * the operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the truncateStream operation.
     */
    public void truncateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(TRUNCATE_STREAM), 1);
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM, 1, streamTags(scope, streamName));
        truncateStreamLatency.reportSuccessValue(latency.toMillis());
    }

    /**
     * This method increments the counter of failed Stream truncate operations in the system as well as the failed
     * truncate attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void truncateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(TRUNCATE_STREAM_FAILED), 1);
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM_FAILED, 1, streamTags(scope, streamName));
    }

    /**
     * This method increments the Stream-specific counter of retention operations.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public static void reportRetentionEvent(String scope, String streamName) {
        DYNAMIC_LOGGER.recordMeterEvents(RETENTION_FREQUENCY, 1, streamTags(scope, streamName));
    }

    /**
     * Reports the number of active segments for a Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param numSegments   Number of active segments in this Stream.
     */
    public static void reportActiveSegments(String scope, String streamName, int numSegments) {
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_COUNT, numSegments, streamTags(scope, streamName));
    }

    /**
     * Reports the number of segment splits and merges related to a particular scale operation on a Stream. Both global
     * and Stream-specific counters are updated.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param splits        Number of segment splits in the scale operation.
     * @param merges        Number of segment merges in the scale operation.
     */
    public static void reportSegmentSplitsAndMerges(String scope, String streamName, long splits, long merges) {
        DYNAMIC_LOGGER.reportGaugeValue(globalMetricName(SEGMENTS_SPLITS), splits);
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_SPLITS, splits, streamTags(scope, streamName));
        DYNAMIC_LOGGER.reportGaugeValue(globalMetricName(SEGMENTS_MERGES), merges);
        DYNAMIC_LOGGER.reportGaugeValue(SEGMENTS_MERGES, merges, streamTags(scope, streamName));
    }

    /**
     * Closes all the OpsStatLogger objects and cleans up the instance.
     */
    public static synchronized void reset() {
        if (INSTANCE.get() != null) {
            INSTANCE.get().createStreamLatency.close();
            INSTANCE.get().deleteStreamLatency.close();
            INSTANCE.get().sealStreamLatency.close();
            INSTANCE.get().updateStreamLatency.close();
            INSTANCE.get().truncateStreamLatency.close();
            INSTANCE.get().addSubscriberLatency.close();
            INSTANCE.get().removeSubscriberLatency.close();
            INSTANCE.get().updateSubscriberLatency.close();
            INSTANCE.get().createScopeLatency.close();
            INSTANCE.get().deleteScopeLatency.close();
            INSTANCE.set(null);
        }
    }
}
