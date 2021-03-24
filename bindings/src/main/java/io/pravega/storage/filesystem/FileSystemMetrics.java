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
package io.pravega.storage.filesystem;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the FilesystemStorage class.
 */
final class FileSystemMetrics {
    private static final StatsLogger FILESYSTEM_LOGGER = MetricsProvider.createStatsLogger("filesystem");
    static final OpStatsLogger READ_LATENCY = FILESYSTEM_LOGGER.createStats(MetricsNames.STORAGE_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = FILESYSTEM_LOGGER.createStats(MetricsNames.STORAGE_WRITE_LATENCY);
    static final Counter READ_BYTES = FILESYSTEM_LOGGER.createCounter(MetricsNames.STORAGE_READ_BYTES);
    static final Counter WRITE_BYTES = FILESYSTEM_LOGGER.createCounter(MetricsNames.STORAGE_WRITE_BYTES);
    static final Counter CREATE_COUNT = FILESYSTEM_LOGGER.createCounter(MetricsNames.STORAGE_CREATE_COUNT);
}