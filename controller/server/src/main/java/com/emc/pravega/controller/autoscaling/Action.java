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
package com.emc.pravega.controller.autoscaling;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Interface to define scaled actions.
 * Scale up and Scale Down are its two implementation.
 */
public interface Action {

    ArrayList<Integer> getSegments();

    ArrayList<AbstractMap.SimpleEntry<Double, Double>> getNewRanges();

    ActionType getType();

    enum ActionType {
        ScaleUp,
        ScaleDown
    }

    class ScaleUp implements Action {
        private final int segmentNumber;
        private final Map<Double, Double> ranges;

        public ScaleUp(int segmentNumber, Map<Double, Double> ranges) {
            this.segmentNumber = segmentNumber;
            this.ranges = ranges;
        }

        @Override
        public ArrayList<Integer> getSegments() {
            return Lists.newArrayList(segmentNumber);
        }

        @Override
        public ArrayList<AbstractMap.SimpleEntry<Double, Double>> getNewRanges() {
            final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
            newRanges.addAll(ranges.entrySet().stream().map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), x.getValue()))
                    .collect(Collectors.toList()));
            return newRanges;
        }

        @Override
        public ActionType getType() {
            return ActionType.ScaleUp;
        }
    }

    class ScaleDown implements Action {
        private final List<Integer> segments;
        private final Pair<Double, Double> newRange;

        public ScaleDown(List<Integer> segments, Pair<Double, Double> newRange) {
            this.segments = segments;
            this.newRange = newRange;
        }

        @Override
        public ArrayList<Integer> getSegments() {
            return Lists.newArrayList(segments);
        }

        @Override
        public ArrayList<AbstractMap.SimpleEntry<Double, Double>> getNewRanges() {
            final ArrayList<AbstractMap.SimpleEntry<Double, Double>> newRanges = new ArrayList<>();
            newRanges.add(new AbstractMap.SimpleEntry<>(newRange.getLeft(), newRange.getRight()));
            return newRanges;
        }

        @Override
        public ActionType getType() {
            return ActionType.ScaleDown;
        }
    }
}
