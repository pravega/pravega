/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeperstorage;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

final class BookKeeperStorageMetrics {
    private static final StatsLogger BOOKKEEPER_LOGGER = MetricsProvider.createStatsLogger("bookkeeper");
    static final OpStatsLogger READ_LATENCY = BOOKKEEPER_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = BOOKKEEPER_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    static final Counter READ_BYTES = BOOKKEEPER_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    static final Counter WRITE_BYTES = BOOKKEEPER_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
}
