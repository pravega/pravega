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
import com.emc.pravega.controller.store.stream.Segment;
import lombok.Data;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.emc.pravega.controller.autoscaling.FunctionalInterfaces.ScaleFunction;

/**
 * This function is supplied to core auto-scaled component (stream monitor) to inject behaviour for how to mergeCandidates.
 */
@Data
public class MergeFunctionImpl implements FunctionalInterfaces.MergeFunction<Event, EventHistory> {
    private final ScaleFunctionImpl scaleFunction;
    private final long cooldownPeriod;

    /**
     * This function calculates merge candidates for the given segment number.
     * We will fetch both neighbours (adjacent key ranged segments) and check if any of them is below low-threshold
     * for sustained periods. Any neighbour that is also Cold is a merge candidate.
     * <p>
     * Note: With the first implementation we are not doing any complex merges. We will only merge 3 to 1 or 2 to 1.
     * But in future we could look for doing complex merges like m to n.
     *
     * @param segment Segment that has been below threshold for sustained period of time.
     * @param history History for this stream over rolling window.
     * @return list of segments that can be safely merged with the given segment.
     */
    @Override
    public List<Integer> mergeCandidates(final Segment segment, final EventHistory history, final List<Segment> activeSegments) {

        // Merge with all neighbours that are below low threshold as that is a safe mergeCandidates.
        final Stream<Segment> neighbourStream = activeSegments.stream()
                .filter(x -> x.getKeyEnd() == segment.getKeyStart() ||
                        x.getKeyStart() == segment.getKeyEnd());

        final List<Integer> coldNeighbours = neighbourStream
                .filter(x -> System.currentTimeMillis() - x.getStart() > cooldownPeriod &&
                        scaleFunction.canScale(x.getNumber(),
                                System.currentTimeMillis(),
                                history,
                                ScaleFunction.Direction.Down))
                .map(Segment::getNumber)
                .collect(Collectors.toList());

        if (coldNeighbours.size() > 0) {
            coldNeighbours.add(segment.getNumber());
            return coldNeighbours;
        }

        // In Future we could try other schemes like :
        // if no cold neighbours, look at m to n merge options (2 to 2 or 3 to 2)
        // for this we will have to look at quantiles and average rates from aggregates to figure out if we want to
        // take all non hot neighbours and rebalance -- split non-hot neighbours into two.

        return null;
    }
}
