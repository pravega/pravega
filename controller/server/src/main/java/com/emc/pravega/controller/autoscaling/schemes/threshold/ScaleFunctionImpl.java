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

import com.emc.pravega.controller.autoscaling.FunctionalInterfaces;
import lombok.Data;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Behaviour class injected into stream monitor to determine whether to scale.
 * This implementation is specific to threshold scheme.
 * It looks at the number of events in the sampling duration to see if the load is sustained for
 * the sampling duration.
 * Works for both scale ups and scale downs.
 */
@Data
public class ScaleFunctionImpl implements FunctionalInterfaces.ScaleFunction<Event, EventHistory> {

    /**
     * Desired target rate for segment. Extracted from scaling policy.
     */
    private final long targetRate;
    /**
     * Duration of rolling window. This is the maximum sampling duration.
     */
    private final long rollingWindowDuration;
    /**
     * Limit beyond which we scale.
     */
    private final double frequencyLimit;

    /**
     * Implements algorithm to identify if a segment has shown similar load characteristics for sustained periods of time.
     *
     * @param segmentNumber Segment number.
     * @param timeStamp     Time at which to check.
     * @param history       Rolling window event History for the stream.
     * @param direction     Scale up vs Scale down.
     * @return true if load characteristics are sustained for appropriate period of time.
     */
    @Override
    public boolean canScale(final int segmentNumber,
                            final long timeStamp,
                            final EventHistory history,
                            final Direction direction) {
        final List<Event> segmentEvents = history.getStoredValues().stream()
                .filter(x -> x.getSegmentNumber() == segmentNumber)
                .collect(Collectors.toList());
        Collections.reverse(segmentEvents);

        final Comparator<Long> comparator = direction.equals(Direction.Up) ? (x, y) -> x >= y : (x, y) -> x < y;

        boolean toScale = false;

        final Function<Long, Long> samplingDurationFunction = direction.equals(Direction.Up) ?
                z -> rollingWindowDuration / Math.max(1, z / (targetRate * 2)) :
                z -> rollingWindowDuration;

        for (final Event e : segmentEvents) {
            final long samplingDuration = samplingDurationFunction.apply(e.getMetricAvgValue());
            if (timeStamp - e.getEventTime() <= samplingDuration) {
                // sum(event * event-period) / sampling period
                double frequency = segmentEvents.stream()
                        .filter(x -> timeStamp - x.getEventTime() <= samplingDuration &&
                                comparator.apply(x.getMetricAvgValue(), e.getMetricAvgValue()))
                        .mapToLong(Event::getEventPeriod)
                        .sum() / samplingDuration;
                if (frequency > this.frequencyLimit) {
                    toScale = true;
                    break;
                }
            }
        }

        return toScale;
    }

    @FunctionalInterface
    interface Comparator<T> {
        boolean apply(T x, T y);
    }
}
