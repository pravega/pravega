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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the {@link BaseChunkStorage} class.
 */
public class ChunkStorageMetrics {
    static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("ChunkStorage");

    static final OpStatsLogger READ_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    static final OpStatsLogger CREATE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CREATE_LATENCY);
    static final OpStatsLogger DELETE_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_DELETE_LATENCY);
    static final OpStatsLogger CONCAT_LATENCY = STATS_LOGGER.createStats(MetricsNames.STORAGE_CONCAT_LATENCY);

    static final OpStatsLogger SLTS_READ_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_READ_LATENCY);
    static final OpStatsLogger SLTS_WRITE_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_WRITE_LATENCY);
    static final OpStatsLogger SLTS_CREATE_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_CREATE_LATENCY);
    static final OpStatsLogger SLTS_DELETE_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_DELETE_LATENCY);
    static final OpStatsLogger SLTS_CONCAT_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_CONCAT_LATENCY);
    static final OpStatsLogger SLTS_TRUNCATE_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_TRUNCATE_LATENCY);
    static final OpStatsLogger SLTS_READ_INDEX_SCAN_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_READ_INDEX_SCAN_LATENCY);
    static final OpStatsLogger SLTS_READ_INDEX_NUM_SCANNED = STATS_LOGGER.createStats(MetricsNames.SLTS_READ_INDEX_NUM_SCANNED);

    static final OpStatsLogger SLTS_SYS_READ_INDEX_SCAN_LATENCY = STATS_LOGGER.createStats(MetricsNames.SLTS_SYS_READ_INDEX_SCAN_LATENCY);
    static final OpStatsLogger SLTS_SYS_READ_INDEX_NUM_SCANNED = STATS_LOGGER.createStats(MetricsNames.SLTS_SYS_READ_INDEX_NUM_SCANNED);

    static final Counter READ_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    static final Counter WRITE_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    static final Counter CONCAT_BYTES = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_BYTES);

    static final Counter CREATE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CREATE_COUNT);
    static final Counter DELETE_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_DELETE_COUNT);
    static final Counter CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_COUNT);

    static final Counter SLTS_READ_BYTES = STATS_LOGGER.createCounter(MetricsNames.SLTS_READ_BYTES);
    static final Counter SLTS_WRITE_BYTES = STATS_LOGGER.createCounter(MetricsNames.SLTS_WRITE_BYTES);
    static final Counter SLTS_CONCAT_BYTES = STATS_LOGGER.createCounter(MetricsNames.SLTS_CONCAT_BYTES);

    static final Counter SLTS_CREATE_COUNT = STATS_LOGGER.createCounter(MetricsNames.SLTS_CREATE_COUNT);
    static final Counter SLTS_DELETE_COUNT = STATS_LOGGER.createCounter(MetricsNames.SLTS_DELETE_COUNT);
    static final Counter SLTS_CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.SLTS_CONCAT_COUNT);
    static final Counter SLTS_TRUNCATE_COUNT = STATS_LOGGER.createCounter(MetricsNames.SLTS_TRUNCATE_COUNT);

    static final Counter LARGE_CONCAT_COUNT = STATS_LOGGER.createCounter(MetricsNames.STORAGE_LARGE_CONCAT_COUNT);
}
