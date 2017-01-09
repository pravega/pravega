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

import com.emc.pravega.controller.autoscaling.AggregatedValue;
import com.emc.pravega.controller.autoscaling.History;
import com.emc.pravega.stream.impl.HostMetric;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Class for storing history for Host's metric.
 * This is specific to threshold scheme.
 * It records and updates its weight after each handle metric.
 * Currently we have a very simplistic weight calculator -
 * weight = Max(1, max(cpu, memory, bandwidth) / peak-load)
 */
public class HostWeightHistory implements History<HostMetric, Double> {
    private static final double PEAK_LOAD = 0.9;

    private double weight;

    public HostWeightHistory() {
        weight = 1.0;
    }

    @Override
    public void record(final HostMetric metric) {
        // All metrics are assumed to be percentage of resource utilization
        weight = Math.max(1, Math.max(Math.max(metric.getCpu(), metric.getMemory()), metric.getBandwidth()) / PEAK_LOAD);
    }

    @Override
    public List<Double> getStoredValues() {
        return Lists.newArrayList(weight);
    }

    @Override
    public List<AggregatedValue> getAggregates() {
        return Lists.newArrayList();
    }
}
