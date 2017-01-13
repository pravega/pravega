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
package com.emc.pravega.controller.monitoring.schemes.threshold;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.monitoring.InjectableBehaviours;
import com.emc.pravega.controller.monitoring.HostMonitor;
import com.emc.pravega.controller.monitoring.MetricManager;
import com.emc.pravega.controller.monitoring.StreamMonitor;
import com.emc.pravega.controller.monitoring.action.ScaleActionProcessor;
import com.emc.pravega.controller.monitoring.action.ScaleActionQueue;
import com.emc.pravega.controller.monitoring.history.Aggregate;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.LastMetricValue;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.LatestMetricAggregator;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.MovingRateAggregator;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.MovingRateValue;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.SegmentQuantileAggregator;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.SegmentQuantileValue;
import com.emc.pravega.controller.monitoring.schemes.threshold.aggregates.ThresholdAggregateBase;
import com.emc.pravega.controller.monitoring.schemes.threshold.history.EventHistory;
import com.emc.pravega.controller.monitoring.schemes.threshold.history.HostHistory;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.util.RollingWindow;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.common.metric.HostMetric;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.emc.pravega.controller.util.Config.ASYNC_TASK_POOL_SIZE;

/**
 * This is implementation of threshold based scheme for MetricManager. Here we inject all the behaviour corresponding
 * too threshold based scheme in the Auto-scaler's flow.
 * Auto-scaler is the entry point for metrics. Streams for which metrics needs to be monitored are registered
 * with this class.
 * This class then starts listening for metrics for the aforesaid streams and all the pravega hosts in the cluster.
 * This class is responsible for bootstrapping the auto-scaling
 * Note: The parts for listening to metric are merely representational.
 */
public class ThresholdMetricManager extends MetricManager<Void, HostHistory, Event, EventHistory> {

    // TODO: make these configurable
    /**
     * What should be percentage of events in the sampling window period before we kick off scaled.
     */
    private static final Double MIN_PERCENTAGE_FOR_SCALE = 0.8;
    /**
     * Rolling window over which history is recorded.
     */
    private static final Duration ROLLING_WINDOW = Duration.ofMinutes(10);
    /**
     * No segment should be a candidate for merge until cool-down period elapses since the time of its creation.
     */
    private static final Duration COOLDOWN_PERIOD = Duration.ofMinutes(30);

    private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(ASYNC_TASK_POOL_SIZE,
            new ThreadFactoryBuilder().setNameFormat("taskpool-%d").build());

    public ThresholdMetricManager(final StreamMetadataTasks streamMetadataTasks,
                                  final StreamMetadataStore streamStore,
                                  final HostControllerStore hostStore) {
        super(streamMetadataTasks, streamStore, hostStore);
    }

    @Override
    protected HostMonitor<Void, HostHistory> getHostMonitor(final Host host) {
        final HostHistory history = new HostHistory(
                new Aggregate<>(new LatestMetricAggregator<HostMetric, Void, HostHistory>(), new LastMetricValue<HostMetric>()));
        return new HostMonitor<>(history);
    }

    @Override
    public StreamMonitor<Event, EventHistory> getStreamMonitor(final ScaleActionQueue scaleActionQueue,
                                                               final ScaleActionProcessor actionProcessor,
                                                               final String stream,
                                                               final String scope,
                                                               final ScalingPolicy policy,
                                                               final List<Segment> activeSegments) {
        final RollingWindow<Event> rollingWindow = new RollingWindow<>(ROLLING_WINDOW, EXECUTOR);

        final EventHistory history = new EventHistory(rollingWindow,
                new EventHistory.EventFunction(policy),
                /**
                 * Two aggregates are attached to our history - one for quantile, second for moving rate.
                 */
                new ThresholdAggregateBase<>(new SegmentQuantileAggregator(), new SegmentQuantileValue()),
                new ThresholdAggregateBase<>(new MovingRateAggregator(), new MovingRateValue())
        );

        final ScaleFunctionImpl scaleFunction = new ScaleFunctionImpl(
                policy.getTargetRate(),
                ROLLING_WINDOW.toMillis(),
                MIN_PERCENTAGE_FOR_SCALE);

        final InjectableBehaviours.SplitFunction<Event, EventHistory> splitFunction = new SplitFunctionImpl();
        final InjectableBehaviours.MergeFunction<Event, EventHistory> mergeFunction = new MergeFunctionImpl(scaleFunction,
                COOLDOWN_PERIOD.toMillis());

        final StreamMonitor<Event, EventHistory> monitor = new StreamMonitor<>(scaleActionQueue,
                actionProcessor,
                history,
                scaleFunction,
                splitFunction,
                mergeFunction,
                stream,
                scope,
                activeSegments);
        return monitor;
    }
}
