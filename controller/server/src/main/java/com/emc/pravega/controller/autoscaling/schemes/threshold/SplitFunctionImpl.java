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
import com.emc.pravega.controller.autoscaling.FunctionalInterfaces;
import com.emc.pravega.controller.store.stream.Segment;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Split function for event history with quantile aggregates.
 */
public class SplitFunctionImpl implements FunctionalInterfaces.SplitFunction<Event, EventHistory> {

    /**
     * This function gets quantile aggregate and based on the quantile, returns key ranges for new splits.
     * If quantile aggregate is not present, this funtion returns null.
     *
     * @param segment        Segment to split
     * @param history        Event History with aggregates
     * @param numberOfSplits Number of splits to be performed for the segment.
     * @return returns list of new ranges if a split is determined, otherwise null if split is not possible.
     */
    @Override
    public List<Pair<Double, Double>> split(final Segment segment,
                                            final EventHistory history,
                                            final int numberOfSplits) {

        final Optional<AggregatedValue> agg = history.getAggregates().stream()
                .filter(x -> x instanceof SegmentQuantileValue).findFirst();
        if (agg.isPresent()) {
            final SegmentQuantileValue aggregate = (SegmentQuantileValue) agg.get();

            // We are dealing with fixed sized quantile. Size fixed at max 6.
            // This will be less than 6 if and only if number of keys are less than 6
            // 6 quantiles will have 5 values in the list.
            // We need to include segment low and segment high in the quantile.
            final List<Double> quantiles =
                    aggregate.getSegmentMetricDistribution(segment.getNumber());

            if (quantiles.size() + 1 >= numberOfSplits) {
                final List<Pair<Double, Double>> newRanges = new ArrayList<>(numberOfSplits);

                // We will only support two or three splits for now.
                int splitIndex = 0;
                switch (numberOfSplits) {
                    case 2:
                        splitIndex = (quantiles.size() - 1) / numberOfSplits;
                        newRanges.add(new ImmutablePair<>(segment.getKeyStart(), quantiles.get(splitIndex)));
                        newRanges.add(new ImmutablePair<>(quantiles.get(splitIndex), segment.getKeyEnd()));
                        break;
                    case 3:
                        splitIndex = (quantiles.size() - 1) / numberOfSplits;
                        newRanges.add(new ImmutablePair<>(segment.getKeyStart(), quantiles.get(splitIndex)));
                        newRanges.add(new ImmutablePair<>(quantiles.get(splitIndex), quantiles.get(2 * splitIndex + 1)));
                        newRanges.add(new ImmutablePair<>(quantiles.get(2 * splitIndex + 1), segment.getKeyEnd()));
                        break;
                }
            }
        }
        return null;
    }
}
