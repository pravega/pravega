/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.rocksdb;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Metrics for RocksDB.
 */
final class RocksDBMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("rocksdb");
    private static final OpStatsLogger INSERT_LATENCY = STATS_LOGGER.createStats(MetricsNames.CACHE_INSERT_LATENCY);
    private static final OpStatsLogger INSERT_SIZE = STATS_LOGGER.createStats(MetricsNames.CACHE_INSERT_SIZE);
    private static final OpStatsLogger GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.CACHE_GET_LATENCY);
    private static final OpStatsLogger GET_SIZE = STATS_LOGGER.createStats(MetricsNames.CACHE_GET_SIZE);
    private static final OpStatsLogger DELETE_LATENCY = STATS_LOGGER.createStats(MetricsNames.CACHE_DELETE_LATENCY);
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    static void insert(long elapsedMillis, long insertDataSize) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_WRITE_BYTES, insertDataSize);
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_INSERT_COUNT, 1);
        INSERT_LATENCY.reportSuccessValue(elapsedMillis);
        INSERT_SIZE.reportSuccessValue(insertDataSize);
    }

    static void get(long elapsedMillis, long getDataSize) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_READ_BYTES, getDataSize);
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_GET_COUNT, 1);
        GET_LATENCY.reportSuccessValue(elapsedMillis);
        GET_SIZE.reportSuccessValue(getDataSize);
    }

    public static void delete(long elapsedMillis) {
        DELETE_LATENCY.reportSuccessValue(elapsedMillis);
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_DELETE_COUNT, 1);
    }
}
