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
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.host.HostNotification;
import com.emc.pravega.controller.store.stream.StreamData;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamNotification;
import com.emc.pravega.stream.impl.Metric;
import lombok.Data;

import java.util.List;
import java.util.Observable;
import java.util.Observer;

/**
 * This class is responsible for reading metrics from an external source.
 * It is also an observer for hostSet and streamSet and listens for any changes
 * to hosts or streams. It calls into appropriate methods of autoscaler corresponding to the signal received.
 * The signals could be about new host added, new stream created, any scale event done, any policy updated.
 */
@Data
public class MetricReader implements Observer, Runnable {
    private final AutoScaler autoScaler;
    private final StreamsSet streamObservable;
    private final HostSet clusterObservable;
    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;

    public MetricReader(final StreamMetadataStore streamStore,
                        final HostControllerStore hostStore,
                        final AutoScaler autoScaler) {
        this.autoScaler = autoScaler;
        this.streamStore = streamStore;
        this.hostStore = hostStore;

        streamObservable = new StreamsSet(streamStore);
        clusterObservable = new HostSet(hostStore);
        streamObservable.addObserver(this);
        clusterObservable.addObserver(this);
        initialize();
    }

    private void initialize() {
        // add all hosts
        clusterObservable.getHosts().stream().parallel().forEach(autoScaler::addHost);

        // add all applicable streams
        streamObservable.getStreams().stream().parallel()
                .filter(y -> toMonitor(y.getName(), y.getScope()))
                .forEach(autoScaler::addStream);
    }

    private boolean toMonitor(final String stream, final String scope) {
        // TODO: decide on a scheme to distribute streams across controller instances.
        // One such scheme could be consistent hashing based distribution
        return true;
    }

    @Override
    public void update(Observable o, Object arg) {
        if (o.getClass().equals(HostSet.class)) {
            handleHostChange((HostNotification) arg);
        } else if (o.getClass().equals(StreamsSet.class)) {
            handleStreamChange((StreamNotification) arg);
        }
    }

    private void handleStreamChange(StreamNotification notification) {
        final StreamData stream = notification.getStream();

        switch (notification.getNotificationType()) {
            case Add:
                autoScaler.addStream(stream);
                break;
            case Alter:
                autoScaler.policyUpdate(stream);
                break;
            case Scale:
                autoScaler.scale(stream);
                break;
        }
    }

    private void handleHostChange(HostNotification notification) {
        final Host host = notification.getHost();

        switch (notification.getNotificationType()) {
            case Add:
                autoScaler.addHost(host);
                break;
            case Remove:
                autoScaler.removeHost(host);
                break;
        }
    }

    @Override
    public void run() {
        // read metrics
        while (true) {
            List<Metric> metrics = null;
            autoScaler.readMetrics(metrics);
        }
    }
}
