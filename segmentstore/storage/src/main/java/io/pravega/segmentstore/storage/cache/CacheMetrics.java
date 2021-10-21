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
package io.pravega.segmentstore.storage.cache;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Metrics for {@link DirectMemoryCache}.
 */
final class CacheMetrics implements AutoCloseable {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("cache");
    private final Counter writeBytes = STATS_LOGGER.createCounter(MetricsNames.CACHE_WRITE_BYTES);
    private final Counter appendBytes = STATS_LOGGER.createCounter(MetricsNames.CACHE_APPEND_BYTES);
    private final Counter readBytes = STATS_LOGGER.createCounter(MetricsNames.CACHE_READ_BYTES);
    private final Counter deleteBytes = STATS_LOGGER.createCounter(MetricsNames.CACHE_DELETE_BYTES);

    void insert(int size) {
        this.writeBytes.add(size);
    }

    void append(int size) {
        this.appendBytes.add(size);
    }

    void get(int size) {
        this.readBytes.add(size);
    }

    void delete(int size) {
        this.deleteBytes.add(size);
    }

    @Override
    public void close() {
        this.writeBytes.close();
        this.appendBytes.close();
        this.readBytes.close();
        this.deleteBytes.close();
    }
}
