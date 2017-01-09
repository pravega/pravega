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
package com.emc.pravega.controller.autoscaling.schemes.threshold;

import com.emc.pravega.controller.autoscaling.Aggregate;
import com.emc.pravega.controller.autoscaling.AggregatedValue;
import com.emc.pravega.controller.autoscaling.FunctionalInterfaces;

/**
 * Base class for Aggregates for Event History used in threshold based auto-scaling scheme.
 * This class is a wrapper over aggregate function and corresponding aggregate value.
 *
 * @param <V> Type of aggregated Value
 * @param <T> Aggregated Value
 */
public class ThresholdAggregateBase<V, T extends AggregatedValue<V>> extends Aggregate<V, T, Event, EventHistory> {
    private final FunctionalInterfaces.AggregateFunction<T, Event, EventHistory> aggFunction;
    private final T aggregatedValue;

    public ThresholdAggregateBase(FunctionalInterfaces.AggregateFunction<T, Event, EventHistory> aggFunction, T aggregatedValue) {
        super(aggFunction, aggregatedValue);

        this.aggFunction = aggFunction;
        this.aggregatedValue = aggregatedValue;
    }

    @Override
    public T getAggregatedValue() {
        return aggregatedValue;
    }

    @Override
    public FunctionalInterfaces.AggregateFunction<T, Event, EventHistory> getAggFunction() {
        return aggFunction;
    }
}

