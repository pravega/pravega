/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.rocksdb;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Metrics for RocksDB.
 */
final class Metrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("rocksdb");
    private static final OpStatsLogger INSERT_LATENCY = STATS_LOGGER.createStats(MetricsNames.CACHE_INSERT_LATENCY);
    private static final OpStatsLogger GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.CACHE_GET_LATENCY);

    static void insert(long elapsedMillis) {
        INSERT_LATENCY.reportSuccessValue(elapsedMillis);
    }

    static void get(long elapsedMillis) {
        GET_LATENCY.reportSuccessValue(elapsedMillis);
    }
}
