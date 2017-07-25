/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.segmentstore.storage.StorageMetricsBase;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the HDFSStorage class.
 */
public final class HDFSMetrics extends StorageMetricsBase {
    private static final StatsLogger HDFS_LOGGER = MetricsProvider.createStatsLogger("hdfs");

    public HDFSMetrics() {
        super(HDFS_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY),
        HDFS_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY),
        HDFS_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES),
        HDFS_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES));
    }

}