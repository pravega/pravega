/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.metrics;

import java.util.concurrent.TimeUnit;

/**
 * This interface handles logging of statistics related to each operation (PUBLISH, CONSUME etc.).
 */
public interface OpStatsLogger {

    /**
     * Increment the failed op counter with the given eventLatency.
     *
     * @param eventLatency the event latency
     * @param unit         the unit
     */
    public void registerFailedEvent(long eventLatency, TimeUnit unit);

    /**
     * Increment the succeeded op counter with the given eventLatency.
     *
     * @param eventLatency the event latency
     * @param unit         the unit
     */
    public void registerSuccessfulEvent(long eventLatency, TimeUnit unit);

    /**
     * An operation with the given value succeeded.
     *
     * @param value the value
     */
    public void registerSuccessfulValue(long value);

    /**
     * An operation with the given value failed.
     *
     * @param value the value
     */
    public void registerFailedValue(long value);

    /**
     * To op stats data op stats data. Need this function to support JMX exports.
     *
     * @return Returns an OpStatsData object with necessary values.
     */
    public OpStatsData toOpStatsData();

    /**
     * Clear stats for this operation.
     */
    public void clear();
}
