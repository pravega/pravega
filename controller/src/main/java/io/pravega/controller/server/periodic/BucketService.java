/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.periodic;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamImpl;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.util.RetryHelper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.LoggerFactory;

public abstract class BucketService extends AbstractService implements BucketChangeListener {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(BucketService.class));

    private final int bucketId;
    private final BucketStore bucketStore;
    protected final ScheduledExecutorService executor;
    private final ConcurrentMap<Stream, CompletableFuture<Void>> workFutureMap;
    private final LinkedBlockingQueue<BucketChangeListener.StreamNotification> notifications;
    private final CompletableFuture<Void> latch;
    private CompletableFuture<Void> notificationLoop;

    BucketService(int bucketId, BucketStore bucketStore, ScheduledExecutorService executor) {
        this.bucketId = bucketId;
        this.bucketStore = bucketStore;
        this.executor = executor;

        this.notifications = new LinkedBlockingQueue<>();
        this.workFutureMap = new ConcurrentHashMap<>();
        this.latch = new CompletableFuture<>();
    }

    @Override
    protected void doStart() {
        RetryHelper.withIndefiniteRetriesAsync(() -> getStreamsForBucket(bucketId)
                .thenAccept(streams -> workFutureMap.putAll(streams.stream()
                                                                   .map(s -> {
                            String[] splits = s.split("/");
                            log.info("Adding new stream {}/{} to bucket {} during bootstrap", splits[0], splits[1], bucketId);
                            return new StreamImpl(splits[0], splits[1]);
                        })
                                                                   .collect(Collectors.toMap(s -> s, this::startWork))
                )),
                e -> log.warn("exception thrown getting streams for bucket {}, e = {}", bucketId, e), executor)
                .thenAccept(x -> {
                    log.info("streams collected for the bucket {}, registering for change notification and starting loop for processing notifications", bucketId);
                    registerBucketChangeListener(bucketId, this);
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
                        workFutureMap.computeIfAbsent(stream, x -> startWork(stream));
                        break;
                    case StreamRemoved:
                        log.info("Stream {}/{} removed from bucket {} ", notification.getScope(), notification.getStream(), bucketId);
                        stream = new StreamImpl(notification.getScope(), notification.getStream());
                        workFutureMap.remove(stream).cancel(true);
                        break;
                    case StreamUpdated:
                        break;
                    case ConnectivityError:
                        log.info("Retention.StreamNotification for connectivity error");
                        break;
                }
            }
        }, executor);
    }

    abstract CompletableFuture<List<String>> getStreamsForBucket(int bucketId);
    
    abstract CompletableFuture<Void> startWork(StreamImpl stream);

    @Override
    protected void doStop() {
        Futures.await(latch);
        if (notificationLoop != null) {
            notificationLoop.thenAccept(x -> {
                // cancel all retention futures
                workFutureMap.forEach((key, value) -> value.cancel(true));
                bucketStore.unregisterBucketChangeListenerForRetention(bucketId);
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
    Map<Stream, CompletableFuture<Void>> getWorkFutureMap() {
        return Collections.unmodifiableMap(workFutureMap);
    }
}
