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

import java.time.Duration;

/**
 * This interface handles logging of statistics related to each operation (Write, Read etc.).
 */
public interface OpStatsLogger extends Metric {

    /**
     * Increment the succeeded op counter with the given eventLatency in NanoSeconds.
     *
     * @param duration the event latency
     */
    void reportSuccessEvent(Duration duration);

    /**
     * Increment the failed op counter with the given eventLatency in NanoSeconds.
     *
     * @param duration the event latency
     */
    void reportFailEvent(Duration duration);

    /**
     * An operation with the given value succeeded.
     *
     * @param value the value
     */
    void reportSuccessValue(long value);

    /**
     * An operation with the given value failed.
     *
     * @param value the value
     */
    void reportFailValue(long value);

    /**
     * To op Stats data. Need this function to support JMX exports and inner test.
     *
     * @return Returns an OpStatsData object with necessary values.
     */
    OpStatsData toOpStatsData();

    /**
     * Clear stats for this operation.
     */
    void clear();
}
