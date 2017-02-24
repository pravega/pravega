/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.host.stat;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.common.util.Retry;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This looks at segment aggregates and determines if a scale operation has to be triggered.
 * If a scale has to be triggered, then it puts a new scale request into the request stream.
 */
@Slf4j
public class AutoScaleProcessor {

    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final long FIVE_MINUTES = Duration.ofMinutes(5).toMillis();
    private static final long TEN_MINUTES = Duration.ofMinutes(10).toMillis();
    private static final long TWENTY_MINUTES = Duration.ofMinutes(20).toMillis();
    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int INITIAL_CAPACITY = 1000;

    private final AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Cache<String, Pair<Long, Long>> cache;
    private final Serializer<ScaleRequest> serializer;
    private final AtomicReference<EventStreamWriter<ScaleRequest>> writer;
    private final EventWriterConfig writerConfig;
    private final AutoScalerConfig configuration;
    private final Executor executor;
    private final ScheduledExecutorService maintenanceExecutor;

    AutoScaleProcessor(AutoScalerConfig configuration,
                       Executor executor,
                       ScheduledExecutorService maintenanceExecutor) {
        this.configuration = configuration;

        cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(configuration.getCacheExpiry().getSeconds(), TimeUnit.SECONDS)
                .removalListener((RemovalListener<String, Pair<Long, Long>>) notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        triggerScaleDown(notification.getKey(), true);
                    }
                })
                .build();
        this.executor = executor;
        this.maintenanceExecutor = maintenanceExecutor;
        serializer = new JavaSerializer<>();
        writerConfig = EventWriterConfig.builder().build();
        writer = new AtomicReference<>();
        CompletableFuture.runAsync(this::bootstrapRequestWriters, maintenanceExecutor);
    }

    @VisibleForTesting
    AutoScaleProcessor(EventStreamWriter<ScaleRequest> writer, AutoScalerConfig configuration, Executor executor, ScheduledExecutorService maintenanceExecutor) {
        this(configuration, executor, maintenanceExecutor);
        this.writer.set(writer);
        this.initialized.set(true);
    }

    @VisibleForTesting
    AutoScaleProcessor(AutoScalerConfig configuration, ClientFactory cf,
                       Executor executor,
                       ScheduledExecutorService maintenanceExecutor) {
        this(configuration, executor, maintenanceExecutor);
        clientFactory.set(cf);
    }

    @Synchronized
    private void bootstrapRequestWriters() {

        CompletableFuture<Void> createWriter = new CompletableFuture<>();

        // Starting with initial delay, in case request stream has not been created, to give it time to start
        // However, we have this wrapped in consumeFailure which means the creation of writer will be retried.
        // We are introducing a delay to avoid exceptions in the log in case creation of writer is attempted before
        // creation of requeststream.
        maintenanceExecutor.schedule(() -> Retry.indefinitelyWithExpBackoff(100, 10, 10000,
                e -> log.error("error while creating writer for requeststream {}", e))
                .runAsync(() -> {
                    if (clientFactory.get() == null) {
                        clientFactory.compareAndSet(null, new ClientFactoryImpl(configuration.getInternalScope(), configuration.getConrollerUri()));
                    }

                    this.writer.set(clientFactory.get().createEventWriter(configuration.getInternalStream(),
                            serializer,
                            writerConfig));
                    initialized.set(true);
                    // even if there is no activity, keep cleaning up the cache so that scale down can be triggered.
                    // caches do not perform clean up if there is no activity. This is because they do not maintain their
                    // own background thread.
                    maintenanceExecutor.scheduleAtFixedRate(cache::cleanUp, 0, configuration.getCacheCleanup().getSeconds(), TimeUnit.SECONDS);
                    log.debug("bootstrapping auto-scale reporter done");
                    createWriter.complete(null);
                    return createWriter;
                }, maintenanceExecutor), 10, TimeUnit.SECONDS);
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        checkAndRun(() -> {
            Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
            long lastRequestTs = 0;

            if (pair != null && pair.getKey() != null) {
                lastRequestTs = pair.getKey();
            }

            long timestamp = System.currentTimeMillis();

            if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
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
            if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
                log.debug("sending request for scale down for {}", streamSegmentName);

                Segment segment = Segment.fromScopedName(streamSegmentName);
                ScaleRequest event = new ScaleRequest(segment.getScope(), segment.getStreamName(),
                        segment.getSegmentNumber(), ScaleRequest.DOWN, timestamp, 0, silent);
                writeRequest(event).thenAccept(x -> cache.put(streamSegmentName,
                        new ImmutablePair<>(0L, timestamp)));
                // mute only scale downs
            }
        });
    }

    private CompletableFuture<Void> writeRequest(ScaleRequest event) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        try {
            CompletableFuture.runAsync(() -> {
                try {
                    writer.get().writeEvent(event.getKey(), event).get();
                    result.complete(null);
                } catch (InterruptedException | ExecutionException e) {
                    log.error("error sending request to requeststream {}", e);
                    result.completeExceptionally(e);
                }
            }, executor);
        } catch (RejectedExecutionException e) {
            log.error("our executor queue is full. failed to post scale event for {}/{}/{}", event.getScope(), event.getStream(), event.getSegmentNumber());
            result.completeExceptionally(e);
        }

        return result;
    }

    void report(String streamSegmentName, long targetRate, byte type, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        log.info("received traffic for {} with twoMinute rate = {} and targetRate = {}", streamSegmentName, twoMinuteRate, targetRate);
        checkAndRun(() -> {
            // note: we are working on caller's thread. We should not do any blocking computation here and return as quickly as
            // possible.
            // So we will decide whether to scale or not and then unblock by asynchronously calling 'writeEvent'
            if (type != WireCommands.CreateSegment.NO_SCALE) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > configuration.getCooldownPeriod().toMillis()) {
                    log.debug("cool down period elapsed for {}", streamSegmentName);

                    // report to see if a scale operation needs to be performed.
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

    void notifyCreated(String segmentStreamName, byte type, long targetRate) {
        if (type != WireCommands.CreateSegment.NO_SCALE) {
            cache.put(segmentStreamName, new ImmutablePair<>(System.currentTimeMillis(), System.currentTimeMillis()));
        }
    }

    void notifySealed(String segmentStreamName) {
        cache.invalidate(segmentStreamName);
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
}

