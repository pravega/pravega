/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import com.emc.pravega.shared.MetricsNames;
import com.emc.pravega.shared.metrics.Counter;
import com.emc.pravega.shared.metrics.MetricsProvider;
import com.emc.pravega.shared.metrics.OpStatsLogger;
import com.emc.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the HDFSStorage class.
 */
final class Metrics {
    private static final StatsLogger HDFS_LOGGER = MetricsProvider.createStatsLogger("hdfs");
    static final OpStatsLogger READ_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_WRITE_LATENCY);
    static final Counter READ_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_READ_BYTES);
    static final Counter WRITE_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_WRITE_BYTES);
}