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
import com.emc.pravega.controller.util.BackgroundWorker;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Class responsible for getting latest data from stream store for a given stream. It implements background worker.
 * It looks at work items in its queue, which is a request for data for a stream and goes to the store to fetch it.
 * This class has a private constructor and a private singleton instance worker.
 * Public apis exposed are to register listeners who would be notified whenever a pull happens.
 * It also exposes another public api for requesting data refresh for specific stream.
 */
@EqualsAndHashCode(callSuper = false)
public class StreamStoreChangeWorker extends BackgroundWorker<Pair<String, String>> {
    /**
     * Single thread as this executor only has one worker.
     */
    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1);

    private static StreamMetadataStore streamStore;

    private static List<StreamChangeListener> streamListeners;

    /**
     * private singleton worker.
     */
    private static StreamStoreChangeWorker singletonWorker;

    private final ConcurrentLinkedQueue<Pair<String, String>> queue = new ConcurrentLinkedQueue<>();

    /**
     * Private constructor.
     */
    private StreamStoreChangeWorker() {
        super(EXECUTOR);
    }

    /**
     * Initialize method. It creates a new Singleton worker that is used for all background work.
     *
     * @param streamMetadataStore handle to stream metadata store.
     */
    public static void initialize(final StreamMetadataStore streamMetadataStore) {
        streamStore = streamMetadataStore;
        singletonWorker = new StreamStoreChangeWorker();
        streamListeners = new ArrayList<>();
    }

    /**
     * Method to add stream change listeners. This class will update the listeners every time it pulls data
     * for streams.
     *
     * @param listener Listener object to register.
     */
    public static void addListener(final StreamChangeListener listener) {
        streamListeners.add(listener);
    }

    /**
     * Method to request worker to pull latest data from stream store for the said stream.
     *
     * @param stream stream name
     * @param scope  stream scope
     */
    public static void requestStreamUpdate(final String stream, final String scope) {
        singletonWorker.queue.add(new ImmutablePair<>(stream, scope));
    }

    @Override
    public Pair<String, String> getNextWork() {
        return queue.poll();
    }

    /**
     * This will asynchronously initiate the pull (stream store apis return future) and not block for their completion.
     * This frees up the executor thread to initiate pull for the next stream.
     * A call back is registered with the stream store call which notifies all registered listeners after the data is
     * pulled successfully.
     *
     * @param stream
     */
    @Override
    public void process(final Pair<String, String> stream) {
        final CompletableFuture<List<Segment>> activeSegments = streamStore.getActiveSegments(stream.getRight());
        final CompletableFuture<StreamConfiguration> configuration = streamStore.getConfiguration(stream.getRight());

        CompletableFuture.allOf(activeSegments, configuration)
                .thenAccept(x -> streamListeners.stream()
                        .forEach(listeners -> listeners.updateStream(new StreamData(stream.getKey(),
                                        stream.getValue(),
                                        FutureHelpers.getAndHandleExceptions(configuration, RuntimeException::new),
                                        FutureHelpers.getAndHandleExceptions(activeSegments, RuntimeException::new)))
                        ));
    }
}
