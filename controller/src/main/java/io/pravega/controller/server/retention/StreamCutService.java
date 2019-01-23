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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamCutService extends AbstractService implements BucketOwnershipListener {
    private final int bucketCount;
    private final String processId;
    private final ConcurrentSkipListSet<StreamCutBucketService> buckets;
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;
    private final RequestTracker requestTracker;

    public StreamCutService(final int bucketCount, String processId, final StreamMetadataStore streamMetadataStore,
                            final StreamMetadataTasks streamMetadataTasks, final ScheduledExecutorService executor,
                            final RequestTracker requestTracker) {
        this.bucketCount = bucketCount;
        this.processId = processId;
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.buckets = new ConcurrentSkipListSet<>();
        this.requestTracker = requestTracker;
    }

    @Override
    protected void doStart() {
        streamMetadataStore.createBucketsRoot()
                .thenCompose(v -> Futures.allOf(IntStream.range(0, bucketCount).boxed().map(this::tryTakeOwnership).collect(Collectors.toList())))
                .thenAccept(x -> streamMetadataStore.registerBucketOwnershipListener(this))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                });
    }

    private CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return RetryHelper.withIndefiniteRetriesAsync(() -> streamMetadataStore.takeBucketOwnership(bucket, processId, executor),
                e -> log.warn("exception while attempting to take ownership"), executor)
                .thenCompose(isOwner -> {
                    if (isOwner && buckets.stream().noneMatch(x -> x.getBucketId() == bucket)) {
                        log.info("Taken ownership for bucket {}", bucket);
                        StreamCutBucketService bucketService = new StreamCutBucketService(bucket, streamMetadataStore,
                                streamMetadataTasks, executor, requestTracker);
                        buckets.add(bucketService);
                        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
                        bucketService.addListener(new Listener() {
                            @Override
                            public void running() {
                                super.running();
                                bucketFuture.complete(null);
                            }

                            @Override
                            public void failed(State from, Throwable failure) {
                                super.failed(from, failure);
                                bucketFuture.completeExceptionally(failure);
                            }
                        }, executor);
                        bucketService.startAsync();

                        return bucketFuture;
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    protected void doStop() {
        Futures.allOf(buckets.stream().map(bucketService -> {
            CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
            bucketService.addListener(new Listener() {
                @Override
                public void terminated(State from) {
                    super.terminated(from);
                    bucketFuture.complete(null);
                }

                @Override
                public void failed(State from, Throwable failure) {
                    super.failed(from, failure);
                    bucketFuture.completeExceptionally(failure);
                }
            }, executor);
            bucketService.stopAsync();

            return bucketFuture;
        }).collect(Collectors.toList()))
                .whenComplete((r, e) -> {
                    streamMetadataStore.unregisterBucketOwnershipListener();
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStopped();
                    }
                });
    }

    @Override
    public void notify(BucketNotification notification) {
        switch (notification.getType()) {
            case BucketAvailable:
                tryTakeOwnership(notification.getBucketId());
                break;
            case ConnectivityError:
                log.info("Bucket notification for connectivity error");
                break;
        }
    }

    @VisibleForTesting
    Set<StreamCutBucketService> getBuckets() {
        return Collections.unmodifiableSet(buckets);
    }
}
