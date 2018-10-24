/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.retention;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamCutBucketService extends AbstractService implements BucketChangeListener {

    private final int bucketId;
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<Stream, CompletableFuture<Void>> retentionFutureMap;
    private final LinkedBlockingQueue<BucketChangeListener.StreamNotification> notifications;
    private final CompletableFuture<Void> latch;
    private CompletableFuture<Void> notificationLoop;
    private final RequestTracker requestTracker;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;

    StreamCutBucketService(int bucketId, StreamMetadataStore streamMetadataStore, StreamMetadataTasks streamMetadataTasks,
                           ScheduledExecutorService executor, RequestTracker requestTracker) {
        this.bucketId = bucketId;
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;

        this.notifications = new LinkedBlockingQueue<>();
        this.retentionFutureMap = new ConcurrentHashMap<>();
        this.latch = new CompletableFuture<>();
        this.requestTracker = requestTracker;
    }

    @Override
    protected void doStart() {
        RetryHelper.withIndefiniteRetriesAsync(() -> streamMetadataStore.getStreamsForBucket(bucketId, executor)
                .thenAccept(streams -> retentionFutureMap.putAll(streams.stream()
                        .map(s -> {
                            String[] splits = s.split("/");
                            log.info("Adding new stream {}/{} to bucket {} during bootstrap", splits[0], splits[1], bucketId);
                            return new StreamImpl(splits[0], splits[1]);
                        })
                        .collect(Collectors.toMap(s -> s, this::getStreamRetentionFuture))
                )),
                e -> log.warn("exception thrown getting streams for bucket {}, e = {}", bucketId, e), executor)
                .thenAccept(x -> {
                    log.info("streams collected for the bucket {}, registering for change notification and starting loop for processing notifications", bucketId);
                    streamMetadataStore.registerBucketChangeListener(bucketId, this);
                })
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                        notificationLoop = Futures.loop(this::isRunning, this::processNotification, executor);
                    }
                    latch.complete(null);
                });
    }

    private CompletableFuture<Void> processNotification() {
        return CompletableFuture.runAsync(() -> {
            StreamNotification notification = Exceptions.handleInterrupted(() -> notifications.poll(1, TimeUnit.SECONDS));
            if (notification != null) {
                final StreamImpl stream;
                switch (notification.getType()) {
                    case StreamAdded:
                        log.info("New stream {}/{} added to bucket {} ", notification.getScope(), notification.getStream(), bucketId);
                        stream = new StreamImpl(notification.getScope(), notification.getStream());
                        retentionFutureMap.computeIfAbsent(stream, x -> getStreamRetentionFuture(stream));
                        break;
                    case StreamRemoved:
                        log.info("Stream {}/{} removed from bucket {} ", notification.getScope(), notification.getStream(), bucketId);
                        stream = new StreamImpl(notification.getScope(), notification.getStream());
                        retentionFutureMap.remove(stream).cancel(true);
                        break;
                    case StreamUpdated:
                        // if retention has been disabled then cancel the future.
                        // if retention has been enabled then add the future if it does not exist.
                        // For now we will do nothing.
                        break;
                    case ConnectivityError:
                        log.info("Retention.StreamNotification for connectivity error");
                        break;
                }
            }
        }, executor);
    }

    private CompletableFuture<Void> getStreamRetentionFuture(StreamImpl stream) {
        // Randomly distribute retention work across RETENTION_FREQUENCY_IN_MINUTES spectrum by introducing a random initial
        // delay. This will ensure that not all streams become eligible for processing of retention at around similar times.
        long delay = Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis();
        long randomInitialDelay = ThreadLocalRandom.current().nextLong(delay);
        return Futures.delayedFuture(() -> performRetention(stream), randomInitialDelay, executor)
            .thenCompose(x -> RetryHelper.loopWithDelay(this::isRunning, () -> performRetention(stream),
                delay, executor));
    }

    private CompletableFuture<Void> performRetention(StreamImpl stream) {
        OperationContext context = streamMetadataStore.createContext(stream.getScope(), stream.getStreamName());

        // Track the new request for this automatic truncation.
        long requestId = requestIdGenerator.get();
        requestTracker.trackRequest(RequestTracker.buildRequestDescriptor("truncateStream", stream.getScope(),
                stream.getStreamName()), requestId);
        LoggerHelpers.debugLogWithTag(log, requestId, "Periodic background processing for retention called for stream {}/{}",
                stream.getScope(), stream.getStreamName());

        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getConfiguration(stream.getScope(), stream.getStreamName(), context, executor)
                .thenCompose(config -> streamMetadataTasks.retention(stream.getScope(), stream.getStreamName(),
                        config.getRetentionPolicy(), System.currentTimeMillis(), context,
                        this.streamMetadataTasks.retrieveDelegationToken()))
                .exceptionally(e -> {
                    LoggerHelpers.warnLogWithTag(log, requestId, "Exception thrown while performing auto retention for stream {} ", stream, e);
                    throw new CompletionException(e);
                }), RetryHelper.UNCONDITIONAL_PREDICATE, 5, executor)
                .exceptionally(e -> {
                    LoggerHelpers.warnLogWithTag(log, requestId, "Unable to perform retention for stream {}. " +
                            "Ignoring, retention will be attempted in next cycle.", stream, e);
                    return null;
                });
    }

    @Override
    protected void doStop() {
        Futures.await(latch);
        if (notificationLoop != null) {
            notificationLoop.thenAccept(x -> {
                // cancel all retention futures
                retentionFutureMap.forEach((key, value) -> value.cancel(true));
                streamMetadataStore.unregisterBucketListener(bucketId);
            }).whenComplete((r, e) -> {
                if (e != null) {
                    notifyFailed(e);
                } else {
                    notifyStopped();
                }
            });
        } else {
            notifyStopped();
        }
    }

    @Override
    public void notify(StreamNotification notification) {
        notifications.add(notification);
    }

    @VisibleForTesting
    int getBucketId() {
        return bucketId;
    }

    @VisibleForTesting
    Map<Stream, CompletableFuture<Void>> getRetentionFutureMap() {
        return Collections.unmodifiableMap(retentionFutureMap);
    }
}
