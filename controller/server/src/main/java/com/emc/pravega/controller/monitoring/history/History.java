/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.monitoring.history;

import com.emc.pravega.common.metric.Metric;

import java.util.List;

/**
 * History Interface. Implementations of this interface are used as history stores where any history could be captured
 * for a given stream of metric.
 *
 * @param <MetricType> Type of metric for which this history is recorded.
 * @param <ValueType>
 */
public interface History<MetricType extends Metric, ValueType> {
    /**
     * Record a new metric received.
     *
     * @param metric incoming metric
     */
    void record(MetricType metric);

    /**
     * Function to retrieve all stored values in the history.
     *
     * @return returns all stored values.
     */
    List<ValueType> getStoredValues();

    /**
     * Function to retrieve all stored aggregated values in the history.
     *
     * @return return all stored aggregates.
     */
    List<AggregatedValue> getAggregates();
}
