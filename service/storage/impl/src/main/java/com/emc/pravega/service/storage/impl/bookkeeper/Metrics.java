/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.bookkeeper;

import com.emc.pravega.shared.MetricsNames;
import com.emc.pravega.shared.metrics.Counter;
import com.emc.pravega.shared.metrics.MetricsProvider;
import com.emc.pravega.shared.metrics.OpStatsLogger;
import com.emc.pravega.shared.metrics.StatsLogger;

/**
 * Metrics for BookKeeper.
 */
final class Metrics {
    static final StatsLogger DURABLE_DATALOG_LOGGER = MetricsProvider.createStatsLogger("durablelog");
    static final OpStatsLogger WRITE_LATENCY = DURABLE_DATALOG_LOGGER.createStats(MetricsNames.DURABLE_DATALOG_WRITE_LATENCY);
    static final Counter WRITE_BYTES = DURABLE_DATALOG_LOGGER.createCounter(MetricsNames.DURABLE_DATALOG_WRITE_BYTES);
}
