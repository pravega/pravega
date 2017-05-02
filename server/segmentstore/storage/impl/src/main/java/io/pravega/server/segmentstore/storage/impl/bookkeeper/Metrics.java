/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.storage.impl.bookkeeper;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Metrics for BookKeeper.
 */
final class Metrics {
    static final StatsLogger DURABLE_DATALOG_LOGGER = MetricsProvider.createStatsLogger("durablelog");
    static final OpStatsLogger WRITE_LATENCY = DURABLE_DATALOG_LOGGER.createStats(MetricsNames.DURABLE_DATALOG_WRITE_LATENCY);
    static final Counter WRITE_BYTES = DURABLE_DATALOG_LOGGER.createCounter(MetricsNames.DURABLE_DATALOG_WRITE_BYTES);
}
