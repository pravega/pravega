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

import com.emc.pravega.controller.autoscaling.FunctionalInterfaces.ScaleFunction.Direction;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.stream.impl.StreamMetric;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Class created per Stream to monitor all metrics related to that stream.
 *
 * @param <V> Type of values stored in the history.
 * @param <H> History where all interesting data computed from metric is stored.
 */
public class StreamMonitor<V, H extends History<StreamMetric, V>> extends MonitorWorker<StreamMetric> {

    private final FunctionalInterfaces.ScaleFunction<V, H> scaleFunction;
    private final FunctionalInterfaces.SplitFunction<V, H> splitFunction;
    private final FunctionalInterfaces.MergeFunction<V, H> mergeFunction;
    private final H history;
    private final ActionQueue actionQueue;
    private final ActionProcessor actionProcessor;
    private List<Segment> activeSegments;

    /**
     * One stream monitor is created per stream.
     * Allows for injection of different behaviours into the stream monitor object to determine how to scale, split and merge segments.
     *
     * @param actionQueue     Queue where actions need to be published.
     * @param actionProcessor Action processor for this stream that processes actions received.
     * @param history         History to be used for this monitor.
     * @param scaleFunction   Injected scale function
     * @param splitFunction   Injected split function
     * @param mergeFunction   Injected merge function
     * @param activeSegments  List of active segments
     */
    public StreamMonitor(final ActionQueue actionQueue,
                         final ActionProcessor actionProcessor,
                         final H history,
                         final FunctionalInterfaces.ScaleFunction<V, H> scaleFunction,
                         final FunctionalInterfaces.SplitFunction<V, H> splitFunction,
                         final FunctionalInterfaces.MergeFunction<V, H> mergeFunction,
                         final List<Segment> activeSegments) {
        super();
        this.history = history;

        this.scaleFunction = scaleFunction;
        this.splitFunction = splitFunction;
        this.mergeFunction = mergeFunction;

        this.actionQueue = actionQueue;
        this.actionProcessor = actionProcessor;
        this.actionProcessor.start();

        this.activeSegments = activeSegments;
    }

    /**
     * Scale event occured, update active segments.
     *
     * @param activeSegments list of active segments.
     */
    public void scaled(final List<Segment> activeSegments) {
        this.activeSegments = activeSegments;
    }

    /**
     * Method to process a new metric. When a metric is received, we check if we need to scale upon receing this metric
     * by calling scaleFunction.canScale.
     * ScaleFunction is the injected behaviour that will determine if we need to scale at this point or not.
     * If can scale returns true, we call the split (or merge functions) to determine segments to seal and new ranges to
     * create.
     *
     * @param metric incoming stream metric
     */
    @Override
    public void process(final StreamMetric metric) {
        history.record(metric);
        // scale up
        final int segmentNumber = metric.getSegmentId().getNumber();
        Optional<Segment> segment = activeSegments.stream().filter(x -> x.getNumber() == segmentNumber).findFirst();
        if (segment.isPresent()) {
            if (scaleFunction.canScale(metric.getSegmentId().getNumber(), metric.getTimestamp(), history, Direction.Up)) {
                // TODO: find out how many splits need to be made. Right now just doing two by default.
                // canScale should return number of splits too -- (metric val) / target rate
                final Map<Double, Double> newRanges = splitFunction.split(segment.get(), history, 2)
                        .stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
                if (newRanges != null) {
                    actionQueue.addAction(new Action.ScaleUp(segmentNumber, newRanges));
                }

            } else if (scaleFunction.canScale(metric.getSegmentId().getNumber(), metric.getTimestamp(), history, Direction.Down)) {
                final List<Integer> segmentsToMerge = mergeFunction.mergeCandidates(segment.get(), history, activeSegments);
                if (segmentsToMerge != null) {
                    final Stream<Segment> filtered = activeSegments.stream().filter(x -> segmentsToMerge.contains(x.getNumber()));
                    final Double low = filtered.mapToDouble(Segment::getKeyStart).min().getAsDouble();
                    final Double high = filtered.mapToDouble(Segment::getKeyStart).max().getAsDouble();
                    actionQueue.addAction(new Action.ScaleDown(segmentsToMerge, new ImmutablePair<>(low, high)));
                }
            }
        }
    }

    /**
     * Stop all processing and threads used by stream monitor and corresponding action processor.
     */
    @Override
    public void stop() {
        actionProcessor.stop();
        super.stop();
    }
}
