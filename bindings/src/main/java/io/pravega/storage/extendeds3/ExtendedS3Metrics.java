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
package io.pravega.storage.extendeds3;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the ExtendedS3Storage class.
 */
final class ExtendedS3Metrics {
    private static final StatsLogger EXTENDED_S3_LOGGER = MetricsProvider.createStatsLogger("ExtendedS3");
    static final OpStatsLogger READ_LATENCY = EXTENDED_S3_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = EXTENDED_S3_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    static final OpStatsLogger CREATE_LATENCY = EXTENDED_S3_LOGGER.createStats(MetricsNames.STORAGE_CREATE_LATENCY);
    static final OpStatsLogger DELETE_LATENCY = EXTENDED_S3_LOGGER.createStats(MetricsNames.STORAGE_DELETE_LATENCY);
    static final OpStatsLogger CONCAT_LATENCY = EXTENDED_S3_LOGGER.createStats(MetricsNames.STORAGE_CONCAT_LATENCY);

    static final Counter READ_BYTES = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    static final Counter WRITE_BYTES = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    static final Counter CONCAT_BYTES = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_BYTES);

    static final Counter CREATE_COUNT = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_CREATE_COUNT);
    static final Counter DELETE_COUNT = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_DELETE_COUNT);
    static final Counter CONCAT_COUNT = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_CONCAT_COUNT);
    static final Counter LARGE_CONCAT_COUNT = EXTENDED_S3_LOGGER.createCounter(MetricsNames.STORAGE_LARGE_CONCAT_COUNT);
}
