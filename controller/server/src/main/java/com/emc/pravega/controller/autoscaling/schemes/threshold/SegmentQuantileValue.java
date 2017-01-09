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
import com.emc.pravega.stream.impl.StreamMetric;
import lombok.Data;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Aggregate for segment quantiles.
 * This class is used to store the latest quantile received in the metric per segment.
 */
public class SegmentQuantileValue implements AggregatedValue<Map<Integer, List<Double>>> {

    /**
     * Threadpool for scheduling periodic purging of old aggregates typically from
     * segments that are no longer active.
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(10);

    private final Map<Integer, SegmentMetricDistribution> aggregate;

    public SegmentQuantileValue() {
        this.aggregate = new HashMap<>();
        EXECUTOR.schedule(this::purge, 10, TimeUnit.MINUTES);
    }

    @Override
    public Map<Integer, List<Double>> getValue() {
        return aggregate.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, x -> x.getValue().getQuantiles()));
    }

    public void update(final StreamMetric m) {
        final List<Double> incoming = m.getQuantiles();
        aggregate.put(m.getSegmentId().getNumber(), new SegmentMetricDistribution(incoming, m.getTimestamp()));
    }

    public List<Double> getSegmentMetricDistribution(final int segmentNumber) {
        final List<Double> quantiles = aggregate.get(segmentNumber).getQuantiles();
        Collections.sort(quantiles);
        return quantiles;
    }

    private void purge() {
        final long current = System.currentTimeMillis();
        // look for segments that have not been updated for a long time
        aggregate.keySet().removeAll(aggregate.entrySet().stream()
                .filter(x -> current - x.getValue().getLastWrite() > Duration.ofMinutes(10).toMillis())
                .map(Map.Entry::getKey).collect(Collectors.toList()));
    }

    @Data
    private class SegmentMetricDistribution {
        private final List<Double> quantiles;
        private final long lastWrite;
    }
}
