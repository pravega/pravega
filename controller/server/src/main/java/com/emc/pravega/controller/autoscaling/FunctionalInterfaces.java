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

import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.stream.impl.StreamMetric;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

public class FunctionalInterfaces {

    /**
     * Interface for injecting behaviour to answer the question - 'to scaled or not to scaled'.
     *
     * @param <V> History's value type.
     * @param <H> History type
     */
    @FunctionalInterface
    public interface ScaleFunction<V, H extends History<StreamMetric, V>> {
        boolean canScale(final int segmentNumber, final long timeStamp, final H history, final Direction direction);

        enum Direction {
            Up,
            Down
        }
    }

    /**
     * Function to identify the split given a hot segment.
     *
     * @param <V> History's value type.
     * @param <H> History type
     */
    @FunctionalInterface
    public interface SplitFunction<V, H extends History<StreamMetric, V>> {
        List<Pair<Double, Double>> split(final Segment segment, final H history, final int numberOfSplits);
    }

    /**
     * Function to determine merge canidates given a segment which is cold.
     *
     * @param <V> History's value type.
     * @param <H> History type
     */
    @FunctionalInterface
    public interface MergeFunction<V, H extends History<StreamMetric, V>> {
        List<Integer> mergeCandidates(final Segment segment, final H history, final List<Segment> activeSegments);
    }

    /**
     * Functions to compute aggregates over the history.
     *
     * @param <T> AggregatedValue Type
     * @param <V> History's value type.
     * @param <H> History type
     */
    @FunctionalInterface
    public interface AggregateFunction<T extends AggregatedValue, V, H extends History<StreamMetric, V>> {
        T compute(final StreamMetric metric, final T value, final H history);
    }
}
