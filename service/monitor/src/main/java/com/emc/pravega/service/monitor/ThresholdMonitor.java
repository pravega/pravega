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
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.JavaSerializer;
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

/**
 * This looks at segment aggregates and determines if a scale operation has to be triggered.
 * If a scale has to be triggered, then it puts a new scale request into the request stream.
 */
@Slf4j
public class ThresholdMonitor implements SegmentTrafficMonitor {

    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final long FIVE_MINUTES = Duration.ofMinutes(5).toMillis();
    private static final long TEN_MINUTES = Duration.ofMinutes(10).toMillis();
    private static final long TWENTY_MINUTES = Duration.ofMinutes(20).toMillis();

    // TODO: read from config
    private static final String STREAM_NAME = "requeststream";
    // Duration for which scale request posts to request stream will be muted for a segment.
    private static final Duration MUTE_DURATION = Duration.ofMinutes(10);
    // Duration for which no scale operation will be performed on a segment after its creation
    private static final Duration MINIMUM_COOLDOWN_PERIOD = Duration.ofMinutes(10);

    private static final ScheduledExecutorService EXECUTOR = new ScheduledThreadPoolExecutor(100);
    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int INITIAL_CAPACITY = 1000;

    private static long muteDuration = MUTE_DURATION.toMillis();
    private static long cooldownPeriod = MINIMUM_COOLDOWN_PERIOD.toMillis();

    private static long cacheCleanup = 10;
    private static long cacheExpiry = 20;
    private static TimeUnit unit = TimeUnit.MINUTES;

    private static AtomicReference<ThresholdMonitor> singletonMonitor = new AtomicReference<>();

    private final AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();

    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private final Cache<String, Pair<Long, Long>> cache;

    private EventStreamWriter<ScaleRequest> writer;
    private EventWriterConfig config;
    private JavaSerializer<ScaleRequest> serializer;

    private ThresholdMonitor() {
        cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(cacheExpiry, unit)
                .removalListener((RemovalListener<String, Pair<Long, Long>>) notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        triggerScaleDown(notification.getKey(), true);
                    }
                })
                .build();
    }

    private ThresholdMonitor(ClientFactory clientFactory) {
        this();
        serializer = new JavaSerializer<>();
        config = new EventWriterConfig(null);

        this.clientFactory.set(clientFactory);
        CompletableFuture.runAsync(this::bootstrapRequestWriters, EXECUTOR);
    }

    @VisibleForTesting
    ThresholdMonitor(EventStreamWriter<ScaleRequest> writer) {
        this();
        this.writer = writer;
        this.initialized.set(true);
    }

    @VisibleForTesting
    public static void setDefaults(Duration mute, Duration coolDown, long cacheCleanup, long cacheExpiry, TimeUnit unit) {
        muteDuration = mute.toMillis();
        cooldownPeriod = coolDown.toMillis();
        ThresholdMonitor.cacheCleanup = cacheCleanup;
        ThresholdMonitor.cacheExpiry = cacheExpiry;
        ThresholdMonitor.unit = unit;
    }

    static ThresholdMonitor getMonitorSingleton(ClientFactory clientFactory) {
        if (singletonMonitor.get() == null) {
            singletonMonitor.compareAndSet(null, new ThresholdMonitor(clientFactory));
        }
        return singletonMonitor.get();
    }

    @Synchronized
    private void bootstrapRequestWriters() {

        CompletableFuture<Void> createWriter = new CompletableFuture<>();

        retryIndefinitely(() -> {
            this.writer = clientFactory.get().createEventWriter(STREAM_NAME,
                    serializer,
                    config);
            initialized.set(true);
            // even if there is no activity, keep cleaning up the cache so that scale down can be triggered.
            // caches do not perform clean up if there is no activity. This is because they do not maintain their
            // own background thread.
            EXECUTOR.scheduleAtFixedRate(cache::cleanUp, 0, cacheCleanup, unit);
        }, createWriter);
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        checkAndRun(() -> {
            Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
            long lastRequestTs = 0;

            if (pair != null && pair.getKey() != null) {
                lastRequestTs = pair.getKey();
            }

            long timestamp = System.currentTimeMillis();

            if (timestamp - lastRequestTs > muteDuration) {
                Segment segment = Segment.fromScopedName(streamSegmentName);
                ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.UP, timestamp, numOfSplits, false);
                // Mute scale for timestamp for both scale up and down
                CompletableFuture.runAsync(() -> {
                    try {
                        writer.writeEvent(event.getKey(), event).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                }, EXECUTOR).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp)));
            }
        });
    }

    private void triggerScaleDown(String streamSegmentName, boolean silent) {
        checkAndRun(() -> {
            Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
            long lastRequestTs = 0;

            if (pair != null && pair.getValue() != null) {
                lastRequestTs = pair.getValue();
            }

            long timestamp = System.currentTimeMillis();
            if (timestamp - lastRequestTs > muteDuration) {
                Segment segment = Segment.fromScopedName(streamSegmentName);
                ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.DOWN, timestamp, 0, silent);
                CompletableFuture.runAsync(() -> {
                    try {
                        writer.writeEvent(event.getKey(), event).get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    // mute only scale downs
                }, EXECUTOR).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(0L, timestamp)));
            }
        });
    }

    @Override
    public void process(String streamSegmentName, long targetRate, byte type, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        checkAndRun(() -> {
            if (type != WireCommands.CreateSegment.NO_SCALE) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > cooldownPeriod) {
                    // process to see if a scale operation needs to be performed.
                    if ((twoMinuteRate > 5 * targetRate && currentTime - startTime > TWO_MINUTES) ||
                            (fiveMinuteRate > 2 * targetRate && currentTime - startTime > FIVE_MINUTES) ||
                            (tenMinuteRate > targetRate && currentTime - startTime > TEN_MINUTES)) {
                        int numOfSplits = (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate);
                        triggerScaleUp(streamSegmentName, numOfSplits);
                    }

                    if (twoMinuteRate < targetRate &&
                            fiveMinuteRate < targetRate &&
                            tenMinuteRate < targetRate &&
                            twentyMinuteRate < targetRate / 2 &&
                            currentTime - startTime > TWENTY_MINUTES) {
                        triggerScaleDown(streamSegmentName, false);
                    }
                }
            }
        });
    }

    @Override
    public void notify(String segmentStreamName, NotificationType type) {
        if (type.equals(NotificationType.SegmentCreated)) {
            cache.put(segmentStreamName, new ImmutablePair<>(System.currentTimeMillis(), System.currentTimeMillis()));
        } else if (type.equals(NotificationType.SegmentSealed)) {
            cache.invalidate(segmentStreamName);
        }
    }

    @VisibleForTesting
    void put(String streamSegmentName, ImmutablePair<Long, Long> lrImmutablePair) {
        cache.put(streamSegmentName, lrImmutablePair);
    }

    private void checkAndRun(Runnable supplier) {
        if (initialized.get()) {
            supplier.run();
        }
    }

    private static void retryIndefinitely(Runnable supplier, CompletableFuture<Void> promise) {
        try {
            if (!promise.isDone()) {
                supplier.run();
                promise.complete(null);
            }
        } catch (Exception e) {
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            EXECUTOR.schedule(() -> retryIndefinitely(supplier, promise), 10, TimeUnit.SECONDS);
        }
    }
}

