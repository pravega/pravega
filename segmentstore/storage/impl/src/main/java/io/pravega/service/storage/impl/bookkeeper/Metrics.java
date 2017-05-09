/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage.impl.bookkeeper;

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
