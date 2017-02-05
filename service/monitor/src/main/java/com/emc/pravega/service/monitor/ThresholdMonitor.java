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
package com.emc.pravega.service.monitor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.ScalingPolicy;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ControllerImpl;
import com.emc.pravega.stream.impl.JavaSerializer;
import com.emc.pravega.stream.impl.StreamConfigurationImpl;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.emc.pravega.service.monitor.MonitorFactory.CONTROLLER_ADDR;
import static com.emc.pravega.service.monitor.MonitorFactory.CONTROLLER_PORT;
import static com.emc.pravega.service.monitor.MonitorFactory.SCOPE;

/**
 * This looks at segment aggregates and determines if a scale operation has to be triggered.
 * If a scale has to be triggered, then it puts a new scale request into the request stream.
 */
@Slf4j
public class ThresholdMonitor implements SegmentTrafficMonitor {

    // Duration for which scale request posts to request stream will be muted for a segment.
    private static final long MUTE_DURATION = Duration.ofMinutes(10).toMillis();
    // Duration for which no scale operation will be performed on a segment after its creation
    private static final long MINIMUM_COOLDOWN_PERIOD = Duration.ofMinutes(5).toMillis();

    // TODO: read from config
    private static final String STREAM_NAME = "requeststream";
    private static final StreamConfiguration REQUEST_STREAM_CONFIG = new StreamConfigurationImpl(SCOPE, STREAM_NAME,
            new ScalingPolicy(ScalingPolicy.Type.BY_RATE_IN_EVENTS, 1000, 2, 1));

    private static final ScheduledExecutorService EXECUTOR = new ScheduledThreadPoolExecutor(100);
    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int INITIAL_CAPACITY = 1000;

    private static AtomicReference<ThresholdMonitor> singletonMonitor = new AtomicReference<>();
    private static AtomicReference<Controller> controllerRef = new AtomicReference<>();

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Cache<String, Pair<Long, Long>> cache;

    private EventStreamWriter<ScaleRequest> writer;

    private ThresholdMonitor(ClientFactory clientFactory) {
        // Schedule bootstrapRequestStream
        cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(20, TimeUnit.MINUTES)
                .removalListener((RemovalListener<String, Pair<Long, Long>>) notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        triggerScaleDown(notification.getKey());
                    }
                })
                .build();

        CompletableFuture.runAsync(() -> bootstrapRequestStream(clientFactory), EXECUTOR);
    }

    static SegmentTrafficMonitor getMonitorSingleton(ClientFactory clientFactory) {
        if (singletonMonitor.get() == null) {
            singletonMonitor.compareAndSet(null, new ThresholdMonitor(clientFactory));
        }
        return singletonMonitor.get();
    }

    @Synchronized
    private void bootstrapRequestStream(ClientFactory clientFactory) {
        if (controllerRef.get() == null) {
            controllerRef.compareAndSet(null, new ControllerImpl(CONTROLLER_ADDR, CONTROLLER_PORT));
        }

        CompletableFuture<Void> createStream = new CompletableFuture<>();
        CompletableFuture<Void> createWriter = new CompletableFuture<>();
        retryIndefinitely(() -> controllerRef.get().createStream(REQUEST_STREAM_CONFIG), createStream);

        createStream.thenAccept(x -> {
            retryIndefinitely(() -> {
                this.writer = clientFactory.createEventWriter(STREAM_NAME,
                        new JavaSerializer<>(),
                        new EventWriterConfig(null));
                initialized.set(true);
                return null;
            }, createWriter);
        });
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getKey() != null) {
            lastRequestTs = pair.getKey();
        }

        long timestamp = System.currentTimeMillis();

        if (timestamp - lastRequestTs > MUTE_DURATION) {
            Segment segment = Segment.fromScopedName(streamSegmentName);
            ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.UP, timestamp, numOfSplits);
            // Mute scale for timestamp for both scale up and down
            CompletableFuture.runAsync(() -> {
                try {
                    writer.writeEvent(event.getKey(), event).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }, EXECUTOR).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp)));
        }
    }

    private void triggerScaleDown(String streamSegmentName) {
        Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
        long lastRequestTs = 0;

        if (pair != null && pair.getValue() != null) {
            lastRequestTs = pair.getValue();
        }

        long timestamp = System.currentTimeMillis();
        if (timestamp - lastRequestTs > MUTE_DURATION) {
            Segment segment = Segment.fromScopedName(streamSegmentName);
            ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.DOWN, timestamp, 0);
            CompletableFuture.runAsync(() -> {
                try {
                    writer.writeEvent(event.getKey(), event).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
                // mute only scale downs
            }, EXECUTOR).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(0L, timestamp)));
        }
    }

    @Override
    public void process(String streamSegmentName, long targetRate, byte type, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        checkAndRun(() -> {
            if (type != WireCommands.CreateSegment.NO_SCALE) {
                if (System.currentTimeMillis() - startTime > MINIMUM_COOLDOWN_PERIOD) {
                    // process to see if a scale operation needs to be performed.
                    if (twoMinuteRate > 5 * targetRate ||
                            fiveMinuteRate > 2 * targetRate ||
                            tenMinuteRate > targetRate) {
                        int numOfSplits = (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate);
                        triggerScaleUp(streamSegmentName, numOfSplits);
                    }

                    if (twoMinuteRate < targetRate &&
                            fiveMinuteRate < targetRate &&
                            tenMinuteRate < targetRate &&
                            twentyMinuteRate < targetRate / 2) {
                        triggerScaleDown(streamSegmentName);
                    }
                }
            }
            return null;
        });
    }

    @Override
    public void notify(String segmentStreamName, NotificationType type) {
        checkAndRun(() -> {
            if (type.equals(NotificationType.SegmentCreated)) {
                cache.put(segmentStreamName, new ImmutablePair<>(System.currentTimeMillis(), System.currentTimeMillis()));
            } else if (type.equals(NotificationType.SegmentSealed)) {
                cache.invalidate(segmentStreamName);
            }
            return null;
        });
    }

    private void checkAndRun(Supplier<Void> supplier) {
        if (initialized.get()) {
            supplier.get();
        }
    }

    private static <T> void retryIndefinitely(Supplier<T> supplier, CompletableFuture<Void> promise) {
        try {
            supplier.get();
            promise.complete(null);
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            EXECUTOR.schedule(() -> retryIndefinitely(supplier, promise), 10, TimeUnit.SECONDS);
        }
    }

    @VisibleForTesting
    public static void setControllerRef(Controller controller) {
        controllerRef.set(controller);
    }
}

