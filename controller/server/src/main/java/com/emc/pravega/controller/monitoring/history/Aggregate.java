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

import com.emc.pravega.controller.monitoring.FunctionalInterfaces;
import com.emc.pravega.stream.impl.Metric;

/**
 * Base class for aggregates.
 * Its a wrapper over aggregated value and its corresponding function.
 *
 * @param <V> Aggregated Value's stored value type.
 * @param <T> Aggregate Value's type.
 * @param <S> History's stored value type.
 * @param <H> History's type.
 */
public class Aggregate<V, T extends AggregatedValue<V>, S, M extends Metric, H extends History<M, S>> {
    private final FunctionalInterfaces.AggregateFunction<T, S, M, H> aggFunction;

    private final T aggregatedValue;

    public Aggregate(FunctionalInterfaces.AggregateFunction<T, S, M, H> aggFunction, T aggregatedValue) {
        this.aggFunction = aggFunction;
        this.aggregatedValue = aggregatedValue;
    }

    public T getAggregatedValue() {
        return aggregatedValue;
    }

    public FunctionalInterfaces.AggregateFunction<T, S, M, H> getAggFunction() {
        return aggFunction;
    }
}

