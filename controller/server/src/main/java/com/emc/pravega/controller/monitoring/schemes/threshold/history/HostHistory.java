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
package com.emc.pravega.controller.monitoring.schemes.threshold.history;

import com.emc.pravega.controller.monitoring.history.Aggregate;
import com.emc.pravega.controller.monitoring.history.AggregatedValue;
import com.emc.pravega.controller.monitoring.history.History;
import com.emc.pravega.stream.impl.HostMetric;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for storing history for Host's metric.
 */
public class HostHistory implements History<HostMetric, Void> {

    private final List<Aggregate> aggregates;

    public HostHistory(Aggregate... aggregates) {
        this.aggregates = Lists.newArrayList(aggregates);
    }

    @Override
    public void record(final HostMetric metric) {
        aggregates.stream().forEach(x -> x.getAggFunction().compute(metric, x.getAggregatedValue(), this));
    }

    @Override
    public List<Void> getStoredValues() {
        return Lists.newArrayList();
    }

    @Override
    public List<AggregatedValue> getAggregates() {
        return aggregates.stream().map(Aggregate::getAggregatedValue).collect(Collectors.toList());
    }
}
