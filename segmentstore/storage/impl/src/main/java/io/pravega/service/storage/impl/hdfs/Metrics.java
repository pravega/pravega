/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage.impl.hdfs;

import io.pravega.shared.MetricsNames;
import io.pravega.shared.metrics.Counter;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.shared.metrics.OpStatsLogger;
import io.pravega.shared.metrics.StatsLogger;

/**
 * Defines all Metrics used by the HDFSStorage class.
 */
final class Metrics {
    private static final StatsLogger HDFS_LOGGER = MetricsProvider.createStatsLogger("hdfs");
    static final OpStatsLogger READ_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_READ_LATENCY);
    static final OpStatsLogger WRITE_LATENCY = HDFS_LOGGER.createStats(MetricsNames.HDFS_WRITE_LATENCY);
    static final Counter READ_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_READ_BYTES);
    static final Counter WRITE_BYTES = HDFS_LOGGER.createCounter(MetricsNames.HDFS_WRITE_BYTES);
}