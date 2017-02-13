/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.common.metrics;

/**
 * A simple interface that only exposes simple type metrics: Counter/Gauge.
 */
public interface DynamicLogger {

    /**
     * Increase Counter with value <i>delta</i> .
     *
     * @param name  the name of Counter
     * @param delta the delta to be added
     */
    void incCounterValue(String name, long delta);

    /**
     * Report gauge value.
     *
     * @param <T>   the type of value
     * @param name  the name of gauge
     * @param value the value to be reported
     */
    <T extends Number> void reportGaugeValue(String name, T value);

    /**
     * Record the occurrence of a given number of events in Meter.
     *
     * @param name   the name of Meter
     * @param number the number of events occurrence
     */
    void recordMeterEvents(String name, long number);
}