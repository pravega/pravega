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
import static io.pravega.shared.MetricsNames.nameFromStream;

/**
 * Class to encapsulate the logic to report Controller service metrics for Streams.
 */
public final class StreamMetrics implements AutoCloseable {

    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("controller");

    private final OpStatsLogger createStreamLatency = STATS_LOGGER.createStats(CREATE_STREAM_LATENCY);
    private final OpStatsLogger deleteStreamLatency = STATS_LOGGER.createStats(DELETE_STREAM_LATENCY);
    private final OpStatsLogger sealStreamLatency = STATS_LOGGER.createStats(SEAL_STREAM_LATENCY);
    private final OpStatsLogger updateStreamLatency = STATS_LOGGER.createStats(UPDATE_STREAM_LATENCY);
    private final OpStatsLogger truncateStreamLatency = STATS_LOGGER.createStats(TRUNCATE_STREAM_LATENCY);

    /**
     * This method increments the global counter of created Streams in the system, initializes stream-specific metrics
     * and reports the latency of the operation.
     *
     * @param scope             Scope.
     * @param streamName        Name of the Stream.
     * @param minNumSegments    Initial number of segments for the Stream.
     * @param latency           Latency of the createStream operation.
     */
    public void createStream(String scope, String streamName, int minNumSegments, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(CREATE_STREAM, scope, streamName), 1); // Streams can be re-created
        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), 0);
        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, streamName), minNumSegments);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, streamName), 0);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEGMENTS_MERGES, scope, streamName), 0);
        createStreamLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the global counter of failed Stream creations in the system as well as the failed creation
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void createStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(CREATE_STREAM_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(CREATE_STREAM_FAILED, scope, streamName), 1);
    }

    /**
     * This method increments the counter of deleted Streams in the system, freezes stream-specific metrics and
     * reports the latency of the successful deleteStream operation.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     * @param latency       Latency of the deleteStream operation.
     */
    public void deleteStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(DELETE_STREAM, scope, streamName), 1); // Streams can be re-created
        deleteStreamLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the counter of failed Stream deletions in the system as well as the failed deletion
     * attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void deleteStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(DELETE_STREAM_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(DELETE_STREAM_FAILED, scope, streamName), 1);
    }

    public void sealStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEAL_STREAM, scope, streamName), 1); // Streams can be re-created
        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(OPEN_TRANSACTIONS, scope, streamName), 0);
        sealStreamLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the counter of failed Stream seal operations in the system as well as the failed
     * seal attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void sealStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(SEAL_STREAM_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(SEAL_STREAM_FAILED, scope, streamName), 1);
    }

    public void updateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(UPDATE_STREAM, scope, streamName), 1);
        updateStreamLatency.reportSuccessEvent(latency);
    }

    /**
     * This method increments the counter of failed Stream seal operations in the system as well as the failed
     * seal attempts for this specific Stream.
     *
     * @param scope         Scope.
     * @param streamName    Name of the Stream.
     */
    public void updateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(UPDATE_STREAM_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(UPDATE_STREAM_FAILED, scope, streamName), 1);
    }

    public void truncateStream(String scope, String streamName, Duration latency) {
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(TRUNCATE_STREAM, scope, streamName), 1);
        truncateStreamLatency.reportSuccessEvent(latency);
    }

    public void truncateStreamFailed(String scope, String streamName) {
        DYNAMIC_LOGGER.incCounterValue(TRUNCATE_STREAM_FAILED, 1);
        DYNAMIC_LOGGER.incCounterValue(nameFromStream(TRUNCATE_STREAM_FAILED, scope, streamName), 1);
    }

    public void retention(String scope, String streamName) {
        DYNAMIC_LOGGER.recordMeterEvents(nameFromStream(RETENTION_FREQUENCY, scope, streamName), 1);
    }

    public void reportActiveSegments(String scope, String streamName, int minNumSegments) {
        DYNAMIC_LOGGER.reportGaugeValue(nameFromStream(SEGMENTS_COUNT, scope, streamName), minNumSegments);
    }

    public void reportSegmentSplitsAndMerges(String scope, String streamName, long splits, long merges) {
        DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_SPLITS, scope, streamName), splits);
        DYNAMIC_LOGGER.updateCounterValue(nameFromStream(SEGMENTS_MERGES, scope, streamName), merges);
    }

    @Override
    public void close() {
        createStreamLatency.close();
        deleteStreamLatency.close();
        sealStreamLatency.close();
        updateStreamLatency.close();
        truncateStreamLatency.close();
    }
}
