/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Metrics for BookKeeper.
 */
final class Metrics {
    private static final StatsLogger DURABLE_DATALOG_LOGGER = MetricsProvider.createStatsLogger("durablelog");
    private static final DynamicLogger DYNAMIC_LOGGER = MetricsProvider.getDynamicLogger();
    private static final OpStatsLogger WRITE_LATENCY = DURABLE_DATALOG_LOGGER.createStats(MetricsNames.TIER1_WRITE_LATENCY);
    private static final Counter WRITE_BYTES = DURABLE_DATALOG_LOGGER.createCounter(MetricsNames.TIER1_WRITE_BYTES);
    private static final OpStatsLogger BK_WRITE_LATENCY = DURABLE_DATALOG_LOGGER.createStats(MetricsNames.BK_WRITE_LATENCY);

    static void writeCompleted(int length, Duration elapsed) {
        WRITE_LATENCY.reportSuccessEvent(elapsed);
        WRITE_BYTES.add(length);
    }

    static void bookKeeperWriteCompleted(Duration elapsed) {
        BK_WRITE_LATENCY.reportSuccessEvent(elapsed);
    }

    /**
     * BookKeeperLog-specific (i.e. per Container) Metrics.
     */
    final static class BookKeeperLog {
        private final String writeQueueSize;
        private final String writeQueueFillRate;
        private final String ledgerCount;

        BookKeeperLog(int containerId) {
            this.ledgerCount = MetricsNames.nameFromContainer(MetricsNames.BK_LEDGER_COUNT, containerId);
            this.writeQueueSize = MetricsNames.nameFromContainer(MetricsNames.BK_WRITE_QUEUE_SIZE, containerId);
            this.writeQueueFillRate = MetricsNames.nameFromContainer(MetricsNames.BK_WRITE_QUEUE_FILL_RATE, containerId);
        }

        void ledgerCount(int count) {
            DYNAMIC_LOGGER.reportGaugeValue(this.ledgerCount, count);
        }

        void queueStats(QueueStats qs) {
            int fillRate = (int) (qs.getAverageItemFillRate() * 100);
            DYNAMIC_LOGGER.reportGaugeValue(this.writeQueueSize, qs.getSize());
            DYNAMIC_LOGGER.reportGaugeValue(this.writeQueueFillRate, fillRate);
        }
    }
}
