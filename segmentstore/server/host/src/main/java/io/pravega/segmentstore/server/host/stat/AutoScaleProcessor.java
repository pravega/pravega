/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.stat;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalListeners;
import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.TagLogger;
import io.pravega.common.util.Retry;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.AutoScaleEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.LoggerFactory;

/**
 * This looks at segment aggregates and determines if a scale operation has to be triggered.
 * If a scale has to be triggered, then it puts a new scale request into the request stream.
 */
public class AutoScaleProcessor {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(AutoScaleProcessor.class));

    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();
    private static final long FIVE_MINUTES = Duration.ofMinutes(5).toMillis();
    private static final long TEN_MINUTES = Duration.ofMinutes(10).toMillis();
    private static final long TWENTY_MINUTES = Duration.ofMinutes(20).toMillis();
    private static final int MAX_CACHE_SIZE = 1000000;
    private static final int INITIAL_CAPACITY = 1000;

    private final AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Cache<String, Pair<Long, Long>> cache;
    private final Serializer<AutoScaleEvent> serializer;
    private final AtomicReference<EventStreamWriter<AutoScaleEvent>> writer;
    private final EventWriterConfig writerConfig;
    private final AutoScalerConfig configuration;
    private final ExecutorService executor;
    private final ScheduledExecutorService maintenanceExecutor;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;


    AutoScaleProcessor(AutoScalerConfig configuration,
                       ExecutorService executor, 
                       ScheduledExecutorService maintenanceExecutor) {
        this.configuration = configuration;
        this.executor = executor;
        this.maintenanceExecutor = maintenanceExecutor;

        serializer = new JavaSerializer<>();
        writerConfig = EventWriterConfig.builder().build();
        writer = new AtomicReference<>();

        cache = CacheBuilder.newBuilder()
                .initialCapacity(INITIAL_CAPACITY)
                .maximumSize(MAX_CACHE_SIZE)
                .expireAfterAccess(configuration.getCacheExpiry().getSeconds(), TimeUnit.SECONDS)
                .removalListener(RemovalListeners.asynchronous((RemovalListener<String, Pair<Long, Long>>) notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        triggerScaleDown(notification.getKey(), true);
                    }
                }, maintenanceExecutor))
                .build();

        CompletableFuture.runAsync(this::bootstrapRequestWriters, maintenanceExecutor);
    }

    @VisibleForTesting
    AutoScaleProcessor(EventStreamWriter<AutoScaleEvent> writer, AutoScalerConfig configuration,
            ExecutorService executor, ScheduledExecutorService maintenanceExecutor) {
        this(configuration, executor, maintenanceExecutor);
        this.writer.set(writer);
        this.initialized.set(true);
        maintenanceExecutor.scheduleAtFixedRate(cache::cleanUp, 0, configuration.getCacheCleanup().getSeconds(), TimeUnit.SECONDS);
    }

    @VisibleForTesting
    AutoScaleProcessor(AutoScalerConfig configuration, ClientFactory cf, ExecutorService executor,
            ScheduledExecutorService maintenanceExecutor) {
        this(configuration, executor, maintenanceExecutor);
        clientFactory.set(cf);
    }

    private void bootstrapRequestWriters() {

        CompletableFuture<Void> createWriter = new CompletableFuture<>();

        // Starting with initial delay, in case request stream has not been created, to give it time to start
        // However, we have this wrapped in consumeFailure which means the creation of writer will be retried.
        // We are introducing a delay to avoid exceptions in the log in case creation of writer is attempted before
        // creation of requeststream.
        maintenanceExecutor.schedule(() -> Retry.indefinitelyWithExpBackoff(100, 10, 10000,
                e -> {
                    log.warn("error while creating writer for requeststream");
                    log.debug("error while creating writer for requeststream {}", e);
                })
                .runAsync(() -> {
                    if (clientFactory.get() == null) {
                        ClientFactory factory = null;
                        if (configuration.isTlsEnabled()) {
                            factory = ClientFactory.withScope(NameUtils.INTERNAL_SCOPE_NAME,
                                    ClientConfig.builder().controllerURI(configuration.getControllerUri())
                                                .trustStore(configuration.getTlsCertFile())
                                                .validateHostName(configuration.isValidateHostName())
                                                .build());
                        } else {
                            factory = ClientFactory.withScope(NameUtils.INTERNAL_SCOPE_NAME,
                                    ClientConfig.builder().controllerURI(configuration.getControllerUri()).build());
                        }
                        clientFactory.compareAndSet(null, factory);
                    }

                    this.writer.set(clientFactory.get().createEventWriter(configuration.getInternalRequestStream(),
                            serializer,
                            writerConfig));
                    initialized.set(true);
                    // even if there is no activity, keep cleaning up the cache so that scale down can be triggered.
                    // caches do not perform clean up if there is no activity. This is because they do not maintain their
                    // own background thread.
                    maintenanceExecutor.scheduleAtFixedRate(cache::cleanUp, 0, configuration.getCacheCleanup().getSeconds(), TimeUnit.SECONDS);
                    log.info("bootstrapping auto-scale reporter done");
                    createWriter.complete(null);
                    return createWriter;
                }, maintenanceExecutor), 10, TimeUnit.SECONDS);
    }

    private void triggerScaleUp(String streamSegmentName, int numOfSplits) {
        if (initialized.get()) {
            Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
            long lastRequestTs = 0;

            if (pair != null && pair.getKey() != null) {
                lastRequestTs = pair.getKey();
            }

            long timestamp = System.currentTimeMillis();
            long requestId = requestIdGenerator.get();
            if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
                log.info(requestId, "sending request for scale up for {}", streamSegmentName);

                Segment segment = Segment.fromScopedName(streamSegmentName);
                AutoScaleEvent event = new AutoScaleEvent(segment.getScope(), segment.getStreamName(), segment.getSegmentId(),
                        AutoScaleEvent.UP, timestamp, numOfSplits, false, requestId);
                // Mute scale for timestamp for both scale up and down
                writeRequest(event, () -> cache.put(streamSegmentName, new ImmutablePair<>(timestamp, timestamp)));
            }
        }
    }

    private void triggerScaleDown(String streamSegmentName, boolean silent) {
        if (initialized.get()) {
            Pair<Long, Long> pair = cache.getIfPresent(streamSegmentName);
            long lastRequestTs = 0;

            if (pair != null && pair.getValue() != null) {
                lastRequestTs = pair.getValue();
            }

            long timestamp = System.currentTimeMillis();
            long requestId = requestIdGenerator.get();
            if (timestamp - lastRequestTs > configuration.getMuteDuration().toMillis()) {
                log.info(requestId, "sending request for scale down for {}", streamSegmentName);

                Segment segment = Segment.fromScopedName(streamSegmentName);
                AutoScaleEvent event = new AutoScaleEvent(segment.getScope(), segment.getStreamName(), segment.getSegmentId(),
                        AutoScaleEvent.DOWN, timestamp, 0, silent, requestId);
                writeRequest(event, () -> {
                    if (!silent) {
                        // mute only scale downs
                        cache.put(streamSegmentName, new ImmutablePair<>(0L, timestamp));
                    }
                });
            }
        }
    }

    private void writeRequest(AutoScaleEvent event, Runnable onSuccess) {
        executor.execute(() -> {
            writer.get().writeEvent(event.getKey(), event).whenComplete((r, e) -> {
                if (e != null) {
                    log.error(event.getTimestamp(), "error sending request to requeststream {}", e);
                } else {
                    log.debug(event.getTimestamp(), "scale event posted successfully");
                    onSuccess.run();
                }
            });
        });
    }

    void report(String streamSegmentName, long targetRate, byte type, long startTime, double twoMinuteRate, double fiveMinuteRate, double tenMinuteRate, double twentyMinuteRate) {
        log.info("received traffic for {} with twoMinute rate = {} and targetRate = {}", streamSegmentName, twoMinuteRate, targetRate);
        if (initialized.get()) {
            // note: we are working on caller's thread. We should not do any blocking computation here and return as quickly as
            // possible.
            // So we will decide whether to scale or not and then unblock by asynchronously calling 'writeEvent'
            if (type != WireCommands.CreateSegment.NO_SCALE) {
                long currentTime = System.currentTimeMillis();
                if (currentTime - startTime > configuration.getCooldownDuration().toMillis()) {
                    log.debug("cool down period elapsed for {}", streamSegmentName);

                    // report to see if a scale operation needs to be performed.
                    if ((twoMinuteRate > 5.0 * targetRate && currentTime - startTime > TWO_MINUTES) ||
                            (fiveMinuteRate > 2.0 * targetRate && currentTime - startTime > FIVE_MINUTES) ||
                            (tenMinuteRate > targetRate && currentTime - startTime > TEN_MINUTES)) {
                        int numOfSplits = Math.max(2, (int) (Double.max(Double.max(twoMinuteRate, fiveMinuteRate), tenMinuteRate) / targetRate));
                        log.debug("triggering scale up for {} with number of splits {}", streamSegmentName, numOfSplits);

                        triggerScaleUp(streamSegmentName, numOfSplits);
                    }

                    if (twoMinuteRate < targetRate &&
                            fiveMinuteRate < targetRate &&
                            tenMinuteRate < targetRate &&
                            twentyMinuteRate < targetRate / 2.0 &&
                            currentTime - startTime > TWENTY_MINUTES) {
                        log.debug("triggering scale down for {}", streamSegmentName);

                        triggerScaleDown(streamSegmentName, false);
                    }
                }
            }
        }
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

    @VisibleForTesting
    Pair<Long, Long> get(String streamSegmentName) {
        return cache.getIfPresent(streamSegmentName);
    }
}

