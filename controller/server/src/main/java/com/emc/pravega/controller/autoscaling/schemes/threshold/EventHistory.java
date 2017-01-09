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
import com.emc.pravega.controller.autoscaling.HostMonitorWorker;
import com.emc.pravega.controller.autoscaling.util.RollingWindow;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.StreamMetric;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * History implementation for threshold scheme. This history generates and stores events for metrics.
 * and aggregates
 */
public class EventHistory implements History<StreamMetric, Event> {

    @FunctionalInterface
    public interface WeightFunction {
        Double weight(StreamMetric metric);
    }

    /**
     * Rolling window object in which events are stored.
     */
    private final RollingWindow<Event> rollingWindow;

    /**
     * Function to generate events from metrics.
     */
    private final EventFunction eventFunction;

    /**
     *
     */
    private final List<ThresholdAggregateBase> aggregates;

    /***
     * Stream Event history class that implements History for stream events.
     * It records the history over rolling window.
     * It stores all aggregates computed by aggregate functions.
     * It generates and stores events everytime new metric is recorded
     *
     * @param rollingWindow      rolling window
     * @param eventFunction      Event function
     * @param aggregateFunctions AggregatedValue function, AggregatedValue Zero
     */
    public EventHistory(final RollingWindow<Event> rollingWindow,
                        final EventFunction eventFunction,
                        final ThresholdAggregateBase... aggregateFunctions) {
        this.eventFunction = eventFunction;

        this.aggregates = Lists.newArrayList(aggregateFunctions);
        this.rollingWindow = rollingWindow;
    }

    @Override
    public void record(final StreamMetric metric) {
        // compute new aggregates by taking all the aggregate functions and running them against handle metric,
        // previous respective aggregated values and entire history
        aggregates.stream().forEach(x -> x.getAggFunction().compute(metric, x.getAggregatedValue(), this));

        // apply the event function
        final Event e = eventFunction.generateEvent(metric);
        if (e != null) {
            rollingWindow.addElement(e.getEventTime(), e);
        }
    }

    @Override
    public List<Event> getStoredValues() {
        return rollingWindow.getElements();
    }

    @Override
    public List<AggregatedValue> getAggregates() {
        return aggregates.stream().map(ThresholdAggregateBase::getAggregatedValue).collect(Collectors.toList());
    }

    @Data
    public static class WeightFunctionImpl implements WeightFunction {
        final Function<StreamMetric, HostMonitorWorker<Double, HostWeightHistory>> hostMonitorFunction;

        @Override
        public Double weight(final StreamMetric metric) {
            return metric.getAvgRate() * hostMonitorFunction.apply(metric).getStoredValues().get(0);
        }
    }

    @Data
    public static class EventFunction {
        private final ScalingPolicy policy;
        private final WeightFunction weightFunction;

        public Event generateEvent(final StreamMetric metric) {
            final long thresholdLow = policy.getTargetRate() / 2;
            final long thresholdHigh = policy.getTargetRate() * 2;

            if (weightFunction.weight(metric) > thresholdHigh) {
                return new Events.HighThreshold(metric.getSegmentId().getNumber(),
                        metric.getTimestamp(),
                        metric.getAvgRate(),
                        metric.getPeriod().toMillis());
            } else if (metric.getAvgRate() < thresholdLow) {
                return new Events.LowThreshold(metric.getSegmentId().getNumber(),
                        metric.getTimestamp(),
                        metric.getAvgRate(),
                        metric.getPeriod().toMillis());
            } else {
                return null;
            }
        }
    }
}
