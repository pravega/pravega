/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.metadata;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the {@link BaseMetadataStore} and {@link TableBasedMetadataStore} classes.
 */
public class StorageMetadataMetrics {
    static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("ChunkedStorageMetadata");

    static final OpStatsLogger GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_GET_LATENCY);
    static final OpStatsLogger COMMIT_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_COMMIT_LATENCY);

    static final Counter METADATA_FOUND_IN_TXN = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_TXN_HIT_COUNT);
    static final Counter METADATA_FOUND_IN_BUFFER = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_BUFFER_HIT_COUNT);
    static final Counter METADATA_FOUND_IN_CACHE = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_CACHE_HIT_COUNT);
    static final Counter METADATA_FOUND_IN_STORE = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_STORE_HIT_COUNT);
    static final Counter METADATA_NOT_FOUND = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_MISS_COUNT);
    static final Counter METADATA_BUFFER_EVICTED_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_METADATA_BUFFER_EVICTED_COUNT);

    static final OpStatsLogger TABLE_GET_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_TABLE_GET_LATENCY);
    static final OpStatsLogger TABLE_WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_METADATA_TABLE_WRITE_LATENCY);
}
