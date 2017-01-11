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
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.StreamData;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Observable;
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

    private final LoadingCache<Pair<String, String>, StreamData> streams;

    public StreamsSet(final StreamMetadataStore streamStore) {
        streams = CacheBuilder.newBuilder()
                .initialCapacity(1000)
                .maximumSize(10000)
                .build(
                        new CacheLoader<Pair<String, String>, StreamData>() {
                            @Override
                            public StreamData load(Pair<String, String> key) throws Exception {
                                List<Segment> segments = FutureHelpers.getAndHandleExceptions(streamStore.getActiveSegments(key.getRight()), RuntimeException::new);
                                StreamConfiguration configuration = FutureHelpers.getAndHandleExceptions(streamStore.getConfiguration(key.getRight()), RuntimeException::new);
                                return new StreamData(key.getKey(), key.getValue(), configuration, segments);
                            }
                        });

        EXECUTOR.schedule(this::periodicUpdate, 20, TimeUnit.MINUTES);

        StreamStoreChangeWorker.addListener(this);
    }

    private void periodicUpdate() {
        streams.asMap().keySet().stream().forEach(x -> StreamStoreChangeWorker.requestStreamUpdate(x.getKey(), x.getValue()));
    }

    @Override
    public void updateStream(final StreamData streamData) {
        final ImmutablePair<String, String> key = new ImmutablePair<>(streamData.getName(), streamData.getScope());
        final StreamData previous = streams.getIfPresent(key);
        if (previous != null) {
            if (!previous.getStreamConfiguration().equals(streamData.getStreamConfiguration())) {
                streams.invalidate(key);
                setChanged();
                notifyObservers(new StreamNotification(StreamNotification.NotificationType.Alter, streamData));
            }

            // if previous max segment number is less than Max segment number in new list, then a scale has occured
            final int previousMax = previous.getActiveSegments().stream().mapToInt(Segment::getNumber).max().getAsInt();
            final int newMax = streamData.getActiveSegments().stream().mapToInt(Segment::getNumber).max().getAsInt();

            if (previousMax < newMax) {
                streams.invalidate(key);
                setChanged();
                notifyObservers(new StreamNotification(StreamNotification.NotificationType.Scale, streamData));
            }
        }
    }

    public StreamData getStream(String stream, String scope) {
        return streams.getUnchecked(new ImmutablePair<>(stream, scope));
    }
}
