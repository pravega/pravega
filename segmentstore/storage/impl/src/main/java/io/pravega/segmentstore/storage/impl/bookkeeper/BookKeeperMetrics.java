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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.DynamicLogger;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;
import java.time.Duration;

import static io.pravega.shared.MetricsTags.containerTag;

/**
 * Metrics for BookKeeper.
 */
final class BookKeeperMetrics {
    private static final StatsLogger STATS_LOGGER = MetricsProvider.createStatsLogger("bookkeeper");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();

    /**
     * BookKeeperLog-specific (i.e. per Container) Metrics.
     */
    final static class BookKeeperLog implements AutoCloseable {
        private final OpStatsLogger writeQueueSize;
        private final OpStatsLogger writeQueueFillRate;
        private final OpStatsLogger writeLatency;
        private final OpStatsLogger totalWriteLatency;
        private final Counter bkWriteBytes;
        private final String[] containerTag;

        BookKeeperLog(int containerId) {
            this.containerTag = containerTag(containerId);
            this.writeQueueSize = STATS_LOGGER.createStats(MetricsNames.BK_WRITE_QUEUE_SIZE, this.containerTag);
            this.writeQueueFillRate = STATS_LOGGER.createStats(MetricsNames.BK_WRITE_QUEUE_FILL_RATE, this.containerTag);
            this.writeLatency = STATS_LOGGER.createStats(MetricsNames.BK_WRITE_LATENCY, this.containerTag);
            this.totalWriteLatency = STATS_LOGGER.createStats(MetricsNames.BK_TOTAL_WRITE_LATENCY, this.containerTag);
            this.bkWriteBytes = STATS_LOGGER.createCounter(MetricsNames.BK_WRITE_BYTES, this.containerTag);
        }

        @Override
        public void close() {
            this.writeQueueSize.close();
            this.writeQueueFillRate.close();
            this.writeLatency.close();
            this.totalWriteLatency.close();
            this.bkWriteBytes.close();
        }

        void ledgerCount(int count) {
            DYNAMIC_LOGGER.reportGaugeValue(MetricsNames.BK_LEDGER_COUNT, count, this.containerTag);
        }

        void queueStats(QueueStats qs) {
            this.writeQueueSize.reportSuccessValue(qs.getSize());
            this.writeQueueFillRate.reportSuccessValue((int) (qs.getAverageItemFillRatio() * 100));
        }

        void writeCompleted(Duration elapsed) {
            this.totalWriteLatency.reportSuccessEvent(elapsed);
        }

        void bookKeeperWriteCompleted(int length, Duration elapsed) {
            this.writeLatency.reportSuccessEvent(elapsed);
            this.bkWriteBytes.add(length);
        }
    }
}
