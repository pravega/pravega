/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.extendeds3;

import io.pravega.segmentstore.storage.impl.StorageMetricsBase;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsLogger;

public class ExtendedS3StorageMetrics extends StorageMetricsBase {
    private static final StatsLogger FILESYSTEM_LOGGER = MetricsProvider.createStatsLogger("extendeds3");

    public ExtendedS3StorageMetrics() {
        readLatency = FILESYSTEM_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
        writeLatency = FILESYSTEM_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
        readBytes = FILESYSTEM_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
        writeBytes = FILESYSTEM_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    }
}
