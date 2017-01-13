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
package com.emc.pravega.controller.monitoring;

import com.emc.pravega.common.cluster.Host;
import com.emc.pravega.common.metric.HostMetric;
import com.emc.pravega.common.metric.Metric;
import com.emc.pravega.common.metric.StreamMetric;
import com.emc.pravega.controller.monitoring.action.ScaleActionProcessor;
import com.emc.pravega.controller.monitoring.action.ScaleActionQueue;
import com.emc.pravega.controller.monitoring.datasets.StreamNotification;
import com.emc.pravega.controller.monitoring.datasets.StreamsSet;
import com.emc.pravega.controller.monitoring.history.History;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamData;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.stream.ScalingPolicy;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Optional;

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
public abstract class MetricManager<V1, H1 extends History<HostMetric, V1>, V2, H2 extends History<StreamMetric, V2>>
        implements Observer {
    private final LoadingCache<Pair<String, String>, StreamMonitor<V2, H2>> streams;
    private final LoadingCache<String, HostMonitor<V1, H1>> hosts;

    protected MetricManager(final StreamMetadataTasks streamMetadataTasks,
                            final StreamMetadataStore streamStore,
                            final HostControllerStore hostStore) {

        StreamMetadataTasks streamMetadataTasks1 = streamMetadataTasks;
        final StreamsSet streamSet = new StreamsSet(streamStore);
        streamSet.addObserver(this);

        streams = CacheBuilder.newBuilder()
                .initialCapacity(1000)
                .maximumSize(100000)
                .removalListener(new RemovalListener<Pair<String, String>, StreamMonitor<V2, H2>>() {
                    @Override
                    public void onRemoval(RemovalNotification<Pair<String, String>, StreamMonitor<V2, H2>> notification) {
                        if (notification != null && notification.getValue() != null) {
                            notification.getValue().stop();
                        }
                    }
                })
                .build(new CacheLoader<Pair<String, String>, StreamMonitor<V2, H2>>() {
                    @Override
                    public StreamMonitor<V2, H2> load(Pair<String, String> key) throws Exception {
                        StreamData streamData = streamSet.getStream(key.getLeft(), key.getRight());
                        final String stream = streamData.getName();
                        final String scope = streamData.getScope();
                        final ScalingPolicy policy = streamData.getStreamConfiguration().getScalingPolicy();
                        final List<Segment> activeSegments = streamData.getActiveSegments();
                        final ScaleActionQueue scaleActionQueue = new ScaleActionQueue();
                        final ScaleActionProcessor actionProcessor = new ScaleActionProcessor(scaleActionQueue,
                                streamMetadataTasks,
                                stream,
                                scope);

                        final StreamMonitor<V2, H2> monitor = getStreamMonitor(scaleActionQueue,
                                actionProcessor,
                                stream,
                                scope,
                                policy,
                                activeSegments);
                        monitor.run();
                        return monitor;
                    }
                });

        hosts = CacheBuilder.newBuilder()
                .initialCapacity(100)
                .maximumSize(1000)
                .build(new CacheLoader<String, HostMonitor<V1, H1>>() {
                    @Override
                    public HostMonitor<V1, H1> load(String key) throws Exception {

                        Optional<Host> host = hostStore.getHostContainersMap().keySet().stream()
                                .filter(x -> x.getIpAddr().equals(key)).findFirst();
                        if (host.isPresent()) {
                            return getHostMonitor(host.get());
                        } else {
                            return null;
                        }
                    }
                });
    }

    @Override
    public void update(Observable o, Object arg) {
        if (o.getClass().equals(StreamsSet.class)) {
            handleStreamChange((StreamNotification) arg);
        }
    }

    private void handleStreamChange(StreamNotification notification) {
        final StreamData stream = notification.getStream();

        switch (notification.getNotificationType()) {
            case Alter:
                policyUpdate(stream);
                break;
            case Scale:
                scaled(stream);
                break;
        }
    }

    /**
     * Receives all incoming metrics, both host and stream metrics and multiplexes them to appropriate monitor objects.
     *
     * @param metrics incoming metrics
     */
    public void handleIncomingMetrics(final List<Metric> metrics) {
        metrics.stream().forEach(x -> {
            switch (x.getType()) {
                case StreamMetric:
                    final StreamMetric streamMetric = (StreamMetric) x;
                    if (toMonitor(streamMetric.getSegmentId().getStream(), streamMetric.getSegmentId().getScope())) {
                        final Pair<String, String> stream = new ImmutablePair<>(streamMetric.getSegmentId().getStream(),
                                streamMetric.getSegmentId().getScope());
                        streams.getUnchecked(stream).incoming(streamMetric);
                    }
                    break;
                case HostMetric:
                    final HostMetric hostMetric = (HostMetric) x;
                    if (toMonitor(hostMetric.getHostIp())) {
                        final HostMonitor<V1, H1> hostmonitor = hosts.getUnchecked(hostMetric.getHostIp());
                        if (hostmonitor != null) {
                            hostmonitor.incoming(hostMetric);
                        }
                    }
                    break;
            }
        });
    }

    public void scaled(final StreamData streamData) {
        final String stream = streamData.getName();
        final String scope = streamData.getScope();
        final List<Segment> activeSegments = streamData.getActiveSegments();

        streams.getUnchecked(new ImmutablePair<>(stream, scope)).scaled(activeSegments);
    }

    public void policyUpdate(final StreamData streamData) {
        final ImmutablePair<String, String> key = new ImmutablePair<>(streamData.getName(), streamData.getScope());

        streams.getUnchecked(key).stop();
        streams.invalidate(key);

        // update all functions dependent on policy
        // if policy is updated, should we discard all the older history because the thresholds are no longer valid
        // so events generated may be incorrect based on current policy. Esp if the desired rate is increased.
        // If its decreased, then it implies that much lesser events were generated in the past. So even that is undesirable.
        // how to get active segments
        // stop previous action processor. create a new action processor.
    }

    protected abstract HostMonitor<V1, H1> getHostMonitor(final Host host);

    protected abstract StreamMonitor<V2, H2> getStreamMonitor(final ScaleActionQueue scaleActionQueue,
                                                              final ScaleActionProcessor actionProcessor,
                                                              final String stream,
                                                              final String scope,
                                                              final ScalingPolicy policy,
                                                              final List<Segment> activeSegments);

    private boolean toMonitor(String hostIp) {
        return true;
    }

    private boolean toMonitor(String stream, String scope) {
        return true;
    }
}
