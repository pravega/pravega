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
import io.netty.util.internal.ConcurrentSet;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class AutoRetentionService extends AbstractService implements BucketOwnershipListener {
    private final int bucketCount;
    private final String processId;
    private final ConcurrentSet<AutoRetentionBucketService> buckets;
    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public AutoRetentionService(final int bucketCount, String processId, final StreamMetadataStore streamMetadataStore,
                                final StreamMetadataTasks streamMetadataTasks, final ScheduledExecutorService executor) {
        this.bucketCount = bucketCount;
        this.processId = processId;
        this.streamMetadataStore = streamMetadataStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.executor = executor;
        this.buckets = new ConcurrentSet<>();
    }

    @Override
    protected void doStart() {
        Futures.await(Futures.allOf(IntStream.range(0, bucketCount).boxed().map(this::tryTakeOwnership).collect(Collectors.toList()))
                .thenAccept(x -> streamMetadataStore.registerBucketOwnershipListener(this)));
        notifyStarted();
    }

    private CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return RetryHelper.withIndefiniteRetriesAsync(() -> streamMetadataStore.takeBucketOwnership(bucket, processId, executor),
                e -> log.warn("exception while attempting to take ownership"), executor)
                .thenAccept(isOwner -> {
                    if (isOwner) {
                        log.info("Taken ownership for bucket {}", bucket);
                        AutoRetentionBucketService bucketService = new AutoRetentionBucketService(bucket, streamMetadataStore,
                                streamMetadataTasks, executor);
                        buckets.add(bucketService);
                        bucketService.startAsync();
                        bucketService.awaitRunning();
                    }
                });
    }

    @Override
    protected void doStop() {
        buckets.forEach(AutoRetentionBucketService::doStop);
        streamMetadataStore.unregisterBucketOwnershipListener();
        notifyStopped();
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
}
