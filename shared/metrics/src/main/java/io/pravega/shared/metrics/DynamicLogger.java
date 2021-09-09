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

/**
 * A simple interface that only exposes simple type metrics: Counter/Gauge.
 */
public interface DynamicLogger {

    /**
     * Increase Counter with value <i>delta</i> .
     *
     * @param name  the name of Counter
     * @param delta the delta to be added
     * @param tags  the tags associated with the Counter.
     */
    void incCounterValue(String name, long delta, String... tags);

    /**
     * Updates the counter with value <i>value</i>.
     *
     * @param name  the name of counter
     * @param value the value to be updated
     * @param tags  the tags associated with the Counter.
     */
    void updateCounterValue(String name, long value, String... tags);

    /**
     * Notifies that the counter will not be updated.
     *
     * @param name the name of counter
     * @param tags the tags associated with the Counter.
     */
    void freezeCounter(String name, String... tags);

    /**
     * Report gauge value.
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the value to be reported
     * @param tags  the tags associated with the Gauge
     */
    <T extends Number> void reportGaugeValue(String name, T value, String... tags);

    /**
     * Notifies that the gauge value will not be updated.
     *
     * @param name  the name of gauge
     * @param tags  the tags associated with the Gauge.
     */
    void freezeGaugeValue(String name, String... tags);

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param name   the name of Meter
     * @param number the number of events occurrence
     * @param tags   the tags associated with the Meter.
     */
    void recordMeterEvents(String name, long number, String... tags);

    /**
     * Notifies that the meter will no longer be reported.
     *
     * @param name  the name of the meter
     * @param tags  the tags associated with the meter.
     */
    void freezeMeter(String name, String... tags);
}