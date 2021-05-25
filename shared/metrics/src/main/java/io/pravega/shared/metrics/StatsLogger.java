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
package io.pravega.shared.metrics;

import java.util.function.Supplier;

/**
 * A simple interface provides create and register Counter/Gauge/OpStatsLogger,
 * and create logger of new scope.
 */
public interface StatsLogger {
    /**
     * Create op stats logger.
     *
     * @param name Stats Name
     * @param tags Tags associated with the stats.
     * @return logger for an OpStat described by the <i>name</i>.
     */
    OpStatsLogger createStats(String name, String... tags);

    /**
     * Create counter.
     *
     * @param name Stats Name
     * @param tags Tags associated with the counter.
     * @return counter described by the <i>name</i>
     */
    Counter createCounter(String name, String... tags);

    /**
     * Create meter.
     *
     * @param name the meter name
     * @param tags the tags associated with the meter.
     * @return Create and register Meter described by the <i>name</i>
     */
    Meter createMeter(String name, String... tags);

    /**
     * Register gauge.
     * <i>value</i> is usually get of Number: AtomicInteger::get, AtomicLong::get
     *
     * @param name          the name of gauge
     * @param tags          the tags associated with the Gauge.
     * @param valueSupplier the supplier to provide value through get()
     */
    Gauge registerGauge(String name, Supplier<Number> valueSupplier, String... tags);
    
    /**
     * Create the stats logger under scope <i>scope</i>.
     *
     * @param scope scope name.
     * @return stats logger under scope <i>scope</i>.
     */
    StatsLogger createScopeLogger(String scope);

}
