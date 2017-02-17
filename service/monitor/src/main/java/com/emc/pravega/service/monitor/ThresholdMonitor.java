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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
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

    // We are keeping processing limited to 2000 write requests. after every 2000 requests are pending a new thread is spawned,
    // but we will never have more than 2000 requests posted.
    // This limits our memory footprint.
    private static final ExecutorService EXECUTOR = new ThreadPoolExecutor(1, // core size
            10, // max size
            10 * 60L, // idle timeout
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2000)); // queue with a size

    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = new ScheduledThreadPoolExecutor(1);

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

        // Starting with initial delay, in case request stream has not been created, to give it time to start
        // However, we have this wrapped in retryIndefinitely which means the creation of writer will be retried.
        // We are introducing a delay to avoid exceptions in the log in case creation of writer is attempted before
        // creation of requeststream.
        SCHEDULED_EXECUTOR.schedule(() -> retryIndefinitely(() -> {
            try {
                this.writer = clientFactory.get().createEventWriter(STREAM_NAME,
                        serializer,
                        config);
            } catch (Throwable t) {
                System.err.println(t);
                throw new RuntimeException(t);
            }
            initialized.set(true);
            // even if there is no activity, keep cleaning up the cache so that scale down can be triggered.
            // caches do not perform clean up if there is no activity. This is because they do not maintain their
            // own background thread.
            // TODO: shivesh
            SCHEDULED_EXECUTOR.scheduleAtFixedRate(cache::cleanUp, 0, cacheCleanup, unit);
            log.debug("bootstrapping threshold monitor done");
        }, createWriter), 10, TimeUnit.SECONDS);
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
                log.debug("sending request for scale up for {}", streamSegmentName);

                Segment segment = Segment.fromScopedName(streamSegmentName);
                ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.UP, timestamp, numOfSplits, false);
                // Mute scale for timestamp for both scale up and down
                writeRequest(event).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp)));
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
                log.debug("sending request for scale down for {}", streamSegmentName);

                Segment segment = Segment.fromScopedName(streamSegmentName);
                ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber(), ScaleRequest.DOWN, timestamp, 0, silent);
                writeRequest(event).thenAccept(x -> cache.put(streamSegmentName, new ImmutablePair<>(0L, timestamp)));
                // mute only scale downs
            }
        });
    }

    private CompletableFuture<Void> writeRequest(ScaleRequest event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            CompletableFuture.runAsync(() -> {
                try {
                    writer.writeEvent(event.getKey(), event).get();
                    result.complete(null);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("error sending request to requeststream {}", e);
                    result.completeExceptionally(e);
                }
            }, EXECUTOR);
        } catch (RejectedExecutionException e) {
            log.error("our executor queue is full. failed to post scale event for {}/{}/{}", event.getScope(), event.getStream(), event.getSegmentNumber());
            result.completeExceptionally(e);
        }

        return result;
    }

    @Override
    public void process(String streamSegmentName, long targetRate, byte type, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        log.info("received traffic for {} with twoMinute rate = {} and targetRate = {}", streamSegmentName, twoMinuteRate, targetRate);
        checkAndRun(() -> {
            // note: we are working on caller's thread. We should not do any blocking computation here and return as quickly as
            // possible.
            // So we will decide whether to scale or not and then unblock by asynchronously calling 'writeEvent'
            if (type != WireCommands.CreateSegment.NO_SCALE) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > cooldownPeriod) {
                    log.debug("cool down period elapsed for {}", streamSegmentName);

                    // process to see if a scale operation needs to be performed.
                    if ((twoMinuteRate > 5 * targetRate && currentTime - startTime > TWO_MINUTES) ||
                            (fiveMinuteRate > 2 * targetRate && currentTime - startTime > FIVE_MINUTES) ||
                            (tenMinuteRate > targetRate && currentTime - startTime > TEN_MINUTES)) {
                        int numOfSplits = (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate);
                        log.debug("triggering scale up for {}", streamSegmentName);

                        triggerScaleUp(streamSegmentName, numOfSplits);
                    }

                    if (twoMinuteRate < targetRate &&
                            fiveMinuteRate < targetRate &&
                            tenMinuteRate < targetRate &&
                            twentyMinuteRate < targetRate / 2 &&
                            currentTime - startTime > TWENTY_MINUTES) {
                        log.debug("triggering scale down for {}", streamSegmentName);

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
            log.error("threshold monitor writer creation failed {}", e);
            // Until we are able to start these readers, keep retrying indefinitely by scheduling it back
            SCHEDULED_EXECUTOR.schedule(() -> retryIndefinitely(supplier, promise), 10, TimeUnit.SECONDS);
        }
    }
}

