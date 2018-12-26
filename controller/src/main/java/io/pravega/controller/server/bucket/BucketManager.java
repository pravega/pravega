/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class BucketManager extends AbstractService implements BucketOwnershipListener {
    private final String processId;
    private final String serviceId;
    private final BucketStore bucketStore;
    private final Function<Integer, AbstractBucketService> bucketServiceSupplier;
    private final ConcurrentMap<Integer, AbstractBucketService> buckets;
    private final ScheduledExecutorService executor;
    private final Object bucketOwnershipLock = new Object();

    public BucketManager(final String processId, final BucketStore bucketStore,
                         final String serviceId, final ScheduledExecutorService executor,
                         final Function<Integer, AbstractBucketService> bucketServiceSupplier) {
        this.processId = processId;
        this.bucketStore = bucketStore;
        this.serviceId = serviceId;
        this.executor = executor;
        this.buckets = new ConcurrentHashMap<>();
        this.bucketServiceSupplier = bucketServiceSupplier;
    }

    @Override
    protected void doStart() {
        Futures.allOf(IntStream.range(0, bucketStore.getBucketCount()).boxed().map(this::tryTakeOwnership).collect(Collectors.toList()))
                .thenAccept(x -> bucketStore.registerBucketOwnershipListener(serviceId, this))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                });
    }

    private CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return RetryHelper.withIndefiniteRetriesAsync(() -> bucketStore.takeBucketOwnership(serviceId, bucket, 
                processId, executor),
                e -> log.warn("exception while attempting to take ownership"), executor)
                .thenCompose(isOwner -> {
                    if (isOwner) {
                        log.info("Taken ownership for bucket {}", bucket);

                        // Once we have taken ownership of the bucket, we will register listeners on the bucket. 
                        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();

                        buckets.computeIfAbsent(bucket, x -> bucketServiceSupplier.apply(bucket));

                        AbstractBucketService bucketService = buckets.get(bucket);
                        bucketService.addListener(new Listener() {
                            @Override
                            public void running() {
                                super.running();
                                log.info("successfully started bucket service for bucket: {} bucket: {} ", BucketManager.this.serviceId, bucket);
                                bucketFuture.complete(null);
                            }

                            @Override
                            public void failed(State from, Throwable failure) {
                                super.failed(from, failure);
                                log.error("Failed to start bucket service: {} bucket: {} ", BucketManager.this.serviceId, bucket);
                                buckets.remove(serviceId);
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
        Futures.allOf(buckets.values().stream().map(bucketService -> {
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
                    bucketStore.unregisterBucketOwnershipListener(serviceId);
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
    Map<Integer, AbstractBucketService> getBucketServices() {
        return Collections.unmodifiableMap(buckets);
    }
}
