/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the {@link BaseMetadataStore} and {@link TableBasedMetadataStore} classes.
 */
public class StorageMetadataMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("ChunkedStorageMetadata");

    static final OpStatsLogger GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_GET_LATENCY);
    static final OpStatsLogger COMMIT_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_COMMIT_LATENCY);

    static final OpStatsLogger TABLE_GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_TABLE_GET_LATENCY);
    static final OpStatsLogger TABLE_WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_TABLE_WRITE_LATENCY);
}
