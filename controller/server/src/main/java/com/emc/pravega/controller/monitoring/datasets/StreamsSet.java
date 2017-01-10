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
package com.emc.pravega.controller.monitoring.datasets;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.monitoring.StreamStoreChangeWorker;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamChangeListener;
import com.emc.pravega.controller.store.stream.StreamData;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.store.stream.StreamNotification;
import com.emc.pravega.stream.StreamConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Optional;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Class to maintain all the streams and receive changes to any metadata pertaining to all streams in the system.
 */
public class StreamsSet extends Observable implements StreamChangeListener {
    /**
     * Single Scheduler thread that periodically wakes up and posts refresh work for all streams in
     * streamStoreShangeWorker's queue. It does this for all streams in this stream set and then sleeps.
     * <p>
     * Since putting watches on stream store for all streams would we very costly, so we will perform this task periodically.
     * However, everytime a scale event occurs (either initiated by auto-scale or via manual scaling) it will result in
     * new segments being created. Stream monitor, upon seeing a new segment will also request Stream Change worker to fetch
     * latest active segments list.
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);

    private final Map<Pair<String, String>, StreamData> streams;

    public StreamsSet(final StreamMetadataStore streamMetadataStore) {
        streams = new HashMap<>();
        FutureHelpers.getAndHandleExceptions(streamMetadataStore.getAllStreams()
                .thenAccept(x -> x.stream().forEach(y -> streams.put(new ImmutablePair<>(y.getName(), y.getScope()), y))), RuntimeException::new);

        EXECUTOR.schedule(this::periodicUpdate, 20, TimeUnit.MINUTES);

        streamMetadataStore.registerListener(this);
        StreamStoreChangeWorker.addListener(this);
    }

    private void periodicUpdate() {
        streams.keySet().stream().forEach(x -> StreamStoreChangeWorker.requestStreamUpdate(x.getKey(), x.getValue()));
    }

    @Override
    public void addStream(final StreamData stream) {
        streams.put(new ImmutablePair<>(stream.getName(), stream.getScope()), stream);
        setChanged();
        notifyObservers(new StreamNotification(StreamNotification.NotificationType.Add, stream));
    }

    @Override
    public void removeStream(final String stream, final String scope) {
        // TODO
    }

    @Override
    public void updateStream(final String stream, final String scope, final StreamConfiguration streamConfiguration) {
        final ImmutablePair<String, String> key = new ImmutablePair<>(stream, scope);
        final StreamData previous = streams.get(key);
        if (!previous.getStreamConfiguration().equals(streamConfiguration)) {
            final StreamData streamData = new StreamData(stream, scope, streamConfiguration, previous.getActiveSegments(), previous.getLastScaleTimestamp());
            streams.put(key, streamData);
            setChanged();
            notifyObservers(new StreamNotification(StreamNotification.NotificationType.Alter, streamData));
        }
    }

    @Override
    public void scaledStream(final String stream, final String scope, final List<Segment> activeSegments) {
        final ImmutablePair<String, String> key = new ImmutablePair<>(stream, scope);
        final StreamData previous = streams.get(key);

        // if previous max segment number is less than Max segment number in new list, then a scale has occured
        final int previousMax = previous.getActiveSegments().stream().mapToInt(Segment::getNumber).max().getAsInt();
        final int newMax = activeSegments.stream().mapToInt(Segment::getNumber).max().getAsInt();

        if (previousMax < newMax) {
            final Optional<Long> lastScaleTimestamp = activeSegments.stream().map(Segment::getStart).reduce(Long::max);
            final StreamData streamData = new StreamData(stream, scope, previous.getStreamConfiguration(), activeSegments, lastScaleTimestamp.get());
            streams.put(key, streamData);
            setChanged();
            notifyObservers(new StreamNotification(StreamNotification.NotificationType.Scale, streamData));
        }
    }

    public Collection<StreamData> getStreams() {
        return streams.values();
    }
}
