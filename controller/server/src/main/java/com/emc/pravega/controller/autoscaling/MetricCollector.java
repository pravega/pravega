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

import com.emc.pravega.stream.impl.HostMetric;
import com.emc.pravega.stream.impl.Metric;
import com.emc.pravega.stream.impl.StreamMetric;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Metric collector is responsible for collecting all the metrics across all streams and hosts. It maintains a map of
 * metric source and its corresponding monitor.
 * It creates a monitor object per entity that it needs to monitor. So one streamMonitor object per stream and
 * one hostMonitor object per host.
 * All metrics specific to a stream is sent to its corresponding stream monitor object.
 * This class also listens for changes to any stream's policy, any scale event or any new stream addition and passes
 * these events to respective monitors to process.
 *
 * @param <V1> Host's History's Value type.
 * @param <H1> Hosts History type.
 * @param <V2> Streams History's value type.
 * @param <H2> Streams history type.
 */
public class MetricCollector<V1, H1 extends History<HostMetric, V1>, V2, H2 extends History<StreamMetric, V2>> {

    private final ConcurrentMap<Pair<String, String>, StreamMonitor<V2, H2>> streams;
    private final ConcurrentMap<String, HostMonitor<V1, H1>> hosts;

    public MetricCollector() {
        this.streams = new ConcurrentHashMap<>();
        this.hosts = new ConcurrentHashMap<>();
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
                    final Pair<String, String> stream = new ImmutablePair<>(streamMetric.getSegmentId().getStream(),
                            streamMetric.getSegmentId().getScope());
                    streams.get(stream).incoming(streamMetric);
                    break;
                case HostMetric:
                    final HostMetric hostMetric = (HostMetric) x;
                    hosts.get(hostMetric.getHostIp()).incoming(hostMetric);
                    break;
            }
        });
    }

    /**
     * Register a new stream monitor for a new stream.
     *
     * @param streamName    name of stream
     * @param scope         scope of stream
     * @param streamMonitor Stream monitor object
     */
    public void addStreamMonitor(final String streamName, final String scope, final StreamMonitor<V2, H2> streamMonitor) {
        final Pair<String, String> stream = new ImmutablePair<>(streamName, scope);
        streams.put(stream, streamMonitor);
    }

    /**
     * Remove a registered stream monitor.
     *
     * @param streamName name of stream
     * @param scope      scope of stream
     */
    public void removeStreamMonitor(final String streamName, final String scope) {
        final Pair<String, String> stream = new ImmutablePair<>(streamName, scope);
        final StreamMonitor<V2, H2> monitor = streams.get(stream);
        if (monitor != null) {
            monitor.stop();
            streams.remove(stream);
        }
    }

    /**
     * Add a new host monitor.
     *
     * @param hostIp      Ip of host to be added
     * @param hostMonitor Host monitor oject
     */
    public void addHostMonitor(final String hostIp, final HostMonitor<V1, H1> hostMonitor) {
        hosts.put(hostIp, hostMonitor);
    }

    /**
     * Remove host monitor.
     *
     * @param hostIp Ip of host to be removed
     */
    public void removeHostMonitor(final String hostIp) {
        hosts.remove(hostIp);
    }

    public StreamMonitor<V2, H2> getStreamMonitor(final String streamName, final String scope) {
        final Pair<String, String> stream = new ImmutablePair<>(streamName, scope);
        return streams.get(stream);
    }

    public HostMonitor<V1, H1> getHostMonitor(final String hostIp) {
        return hosts.get(hostIp);
    }
}
