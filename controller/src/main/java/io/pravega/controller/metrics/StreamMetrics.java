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

import static io.pravega.shared.MetricsNames.CREATE_STREAM;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_FAILED;
import static io.pravega.shared.MetricsNames.CREATE_STREAM_LATENCY;
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
    private final OpStatsLogger truncateStreamLatency;

    private StreamMetrics() {
        createStreamLatency = STATS_LOGGER.createStats(CREATE_STREAM_LATENCY);
        deleteStreamLatency = STATS_LOGGER.createStats(DELETE_STREAM_LATENCY);
        sealStreamLatency = STATS_LOGGER.createStats(SEAL_STREAM_LATENCY);
        updateStreamLatency = STATS_LOGGER.createStats(UPDATE_STREAM_LATENCY);
        truncateStreamLatency = STATS_LOGGER.createStats(TRUNCATE_STREAM_LATENCY);
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
            INSTANCE.set(null);
        }
    }
}
