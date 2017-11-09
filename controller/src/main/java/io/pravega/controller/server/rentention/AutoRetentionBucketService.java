/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rentention;

import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.OperationContext;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.Config;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

@Slf4j
public class AutoRetentionBucketService extends AbstractService implements BucketChangeListener {

    private final int bucketId;
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;
    private final ConcurrentMap<Stream, CompletableFuture> retentionFutureMap;
    private final LinkedBlockingQueue<BucketChangeListener.StreamNotification> notifications;
    private final AtomicBoolean stop;

    public AutoRetentionBucketService(int bucketId, StreamMetadataStore streamMetadataStore,
                                      StreamMetadataTasks streamMetadataTasks, ScheduledExecutorService executor) {
        this.bucketId = bucketId;
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;

        this.notifications = new LinkedBlockingQueue<>();
        this.stop = new AtomicBoolean(false);
        this.retentionFutureMap = new ConcurrentHashMap<>();
    }

    @Override
    protected void doStart() {
        RetryHelper.withIndefiniteRetriesAsync(() -> streamMetadataStore.getStreamsForBucket(bucketId, executor)
                .thenAccept(streams -> retentionFutureMap.putAll(streams.stream()
                        .map(s -> {
                            String[] splits = s.split("/");
                            return new StreamImpl(splits[0], splits[1]);
                        })
                        .collect(Collectors.toMap(s -> s, this::getStreamRetentionFuture))
                )),
                e -> log.warn("exception thrown getting streams for bucket {}, e = {}", bucketId, e), executor)
                .thenAccept(x -> {
                    log.info("streams collected for the bucket, registering for change notification and starting loop for processing notifications");
                    streamMetadataStore.registerBucketChangeListener(bucketId, this);
                    Futures.loop(stop::get, this::processNotification, executor);
                })
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                });
    }

    private CompletableFuture<Void> processNotification() {
        return CompletableFuture.runAsync(() -> {
            StreamNotification notification = Exceptions.handleInterrupted(notifications::take);
            StreamImpl stream = new StreamImpl(notification.getScope(), notification.getStream());

            switch (notification.getType()) {
                case StreamAdded:
                    retentionFutureMap.putIfAbsent(stream, getStreamRetentionFuture(stream));
                    break;
                case StreamRemoved:
                    retentionFutureMap.remove(stream).cancel(true);
                    break;
                case StreamUpdated:
                    // if retention has been disabled then cancel the future.
                    // if retention has been enabled then add the future if it does not exist.
                    // For now we will do nothing.
                    break;
                case ConnectivityError:
                    // TODO: refresh retention future map as this means we can miss events
                    break;
            }
        });
    }

    private CompletableFuture<Void> getStreamRetentionFuture(StreamImpl stream) {
        return Futures.loopWithDelay(stop::get, () -> performRetention(stream),
                Duration.ofMinutes(Config.MINIMUM_RETENTION_FREQUENCY_IN_MINUTES).toMillis(), executor);
    }

    private CompletableFuture<Void> performRetention(StreamImpl stream) {
        OperationContext context = streamMetadataStore.createContext(stream.getScope(), stream.getStreamName());
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getConfiguration(stream.getScope(), stream.getStreamName(), context, executor)
                .thenCompose(config -> streamMetadataTasks.autoRetention(stream.getScope(), stream.getStreamName(),
                        config.getRetentionPolicy(), System.currentTimeMillis(), context))
                .exceptionally(e -> {
                    log.warn("Exception thrown while performing auto retention for stream {}, {}", stream, e);
                    throw new CompletionException(e);
                }), RetryHelper.UNCONDITIONAL_PREDICATE, 5, executor)
                .exceptionally(e -> {
                    log.warn("Unable to perform retention for stream {}, {}. Ignoring, retention will be attempted in next cycle.", stream, e);
                    return null;
                });
    }

    @Override
    protected void doStop() {
        this.stop.set(true);
        CompletableFuture.runAsync(() -> {
            // cancel all futures
            retentionFutureMap.forEach((key, value) -> value.cancel(true));
            streamMetadataStore.unregisterBucketListener(bucketId);
        }).whenComplete((r, e) -> {
            if (e != null) {
                notifyFailed(e);
            } else {
                notifyStopped();
            }
        });
    }

    @Override
    public void notify(StreamNotification notification) {
        notifications.add(notification);
    }
}
