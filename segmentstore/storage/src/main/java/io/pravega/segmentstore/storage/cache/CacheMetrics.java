/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.cache;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;

/**
 * Metrics for {@link CacheStorage}
 */
final class CacheMetrics {
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    static void insert(int size) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_WRITE_BYTES, size);
    }

    static void append(int size) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_APPEND_BYTES, size);
    }

    static void get(int size) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_READ_BYTES, size);
    }

    static void delete(int size) {
        DYNAMIC_LOGGER.incCounterValue(MetricsNames.CACHE_DELETE_BYTES, size);
    }
}
