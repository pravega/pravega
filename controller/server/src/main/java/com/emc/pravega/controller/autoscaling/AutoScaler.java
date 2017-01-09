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

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamData;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.impl.HostMetric;
import com.emc.pravega.stream.impl.Metric;
import com.emc.pravega.stream.impl.StreamMetric;

import java.util.List;

/**
 * This is the base class where all the metrics are received and processed.
 * Streams and hosts to be monitored are registered with this class. It listens for changes to those streams and hosts
 * and sends appropriate signals to corresponding objects.
 *
 * @param <V1> Host's history's value type
 * @param <H1> Host's history type
 * @param <V2> Stream's History's value type
 * @param <H2> Stream's history type.
 */
public abstract class AutoScaler<V1, H1 extends History<HostMetric, V1>, V2, H2 extends History<StreamMetric, V2>> {
    /**
     * Metric Collector object. This class is multiplexing all metrics to their respective monitors.
     */
    protected final MetricCollector<V1, H1, V2, H2> metricCollector;
    private final StreamMetadataTasks streamMetadataTasks;

    protected AutoScaler(final StreamMetadataTasks streamMetadataTasks) {
        this.streamMetadataTasks = streamMetadataTasks;
        this.metricCollector = new MetricCollector<>();
    }

    public void addHost(final Host host) {
        final HostMonitorWorker<V1, H1> hostMonitor = getHostMonitor(host);
        metricCollector.addHostMonitor(host.getIpAddr(), hostMonitor);
    }

    public void removeHost(Host host) {
        metricCollector.removeHostMonitor(host.getIpAddr());
    }

    public void addStream(final StreamData streamData) {
        final String stream = streamData.getName();
        final String scope = streamData.getScope();
        final ScalingPolicy policy = streamData.getStreamConfiguration().getScalingPolicy();
        final List<Segment> activeSegments = streamData.getActiveSegments();
        final ActionQueue actionQueue = new ActionQueue();
        final ActionProcessor actionProcessor = new ActionProcessor(actionQueue, streamMetadataTasks, stream, scope);

        final StreamMonitorWorker<V2, H2> monitor = getStreamMonitor(actionQueue, actionProcessor, stream, scope, policy, activeSegments);
        metricCollector.addStreamMonitor(stream, scope, monitor);
    }

    public void readMetrics(final List<Metric> metrics) {
        metricCollector.handleIncomingMetrics(metrics);
    }

    public void scale(final StreamData streamData) {
        final String stream = streamData.getName();
        final String scope = streamData.getScope();
        final List<Segment> activeSegments = streamData.getActiveSegments();

        metricCollector.getStreamMonitor(stream, scope).scaled(activeSegments);
    }

    public void policyUpdate(final StreamData streamData) {
        final String stream = streamData.getName();
        final String scope = streamData.getScope();

        // update all functions dependent on policy
        // if policy is updated, should we discard all the older history because the thresholds are no longer valid
        // so events generated may be incorrect based on current policy. Esp if the desired rate is increased.
        // If its decreased, then it implies that much lesser events were generated in the past. So even that is undesirable.
        // how to get active segments
        // stop previous action processor. create a new action processor.
        metricCollector.removeStreamMonitor(stream, scope);
        addStream(streamData);
    }

    protected abstract HostMonitorWorker<V1, H1> getHostMonitor(final Host host);

    protected abstract StreamMonitorWorker<V2, H2> getStreamMonitor(final ActionQueue actionQueue,
                                                              final ActionProcessor actionProcessor,
                                                              final String stream,
                                                              final String scope,
                                                              final ScalingPolicy policy,
                                                              final List<Segment> activeSegments);
}
