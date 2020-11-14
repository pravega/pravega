/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the {@link BaseChunkStorage} class.
 */
public class ChunkStorageMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("ChunkStorage");

    static final OpStatsLogger READ_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    static final OpStatsLogger CREATE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CREATE_LATENCY);
    static final OpStatsLogger DELETE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_DELETE_LATENCY);
    static final OpStatsLogger CONCAT_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CONCAT_LATENCY);

    static final Counter READ_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    static final Counter WRITE_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    static final Counter CONCAT_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_BYTES);

    static final Counter CREATE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CREATE_COUNT);
    static final Counter DELETE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_DELETE_COUNT);
    static final Counter CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_COUNT);

    static final Counter LARGE_CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_LARGE_CONCAT_COUNT);
}
