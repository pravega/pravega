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
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This class is central to bucket management. This is instantiated once per service type. It is responsible for acquiring
 * ownership of buckets for the given service and then starting one bucket service instance for each bucket it owns. 
 * It manages the lifecycle of bucket service.
 * The implementation of this class implements store specific monitoring for bucket ownership changes.
 * This service does not have any dedicated threads. Upon receiving any new notification, it submits a "tryTakeOwnership" 
 * work in the executor's queue. 
 */
@Slf4j
public abstract class BucketManager extends AbstractService {
    private final String processId;
    @Getter(AccessLevel.PROTECTED)
    private final BucketStore.ServiceType serviceType;
    private final Function<Integer, BucketService> bucketServiceSupplier;
    private final ConcurrentMap<Integer, BucketService> buckets;
    private final ScheduledExecutorService executor;

    BucketManager(final String processId, final BucketStore.ServiceType serviceType, final ScheduledExecutorService executor,
                  final Function<Integer, BucketService> bucketServiceSupplier) {
        this.processId = processId;
        this.serviceType = serviceType;
        this.executor = executor;
        this.buckets = new ConcurrentHashMap<>();
        this.bucketServiceSupplier = bucketServiceSupplier;
    }

    @Override
    protected void doStart() {
        Futures.allOf(IntStream.range(0, getBucketCount()).boxed().map(this::initializeBucket).collect(Collectors.toList()))
                .thenAccept(x -> startBucketOwnershipListener())
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                });
    }

    protected abstract int getBucketCount();

    private CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return takeBucketOwnership(bucket, processId, executor)
                         .thenCompose(isOwner -> {
                    if (isOwner) {
                        log.info("{}: Taken ownership for bucket {}", serviceType, bucket);

                        // Once we have taken ownership of the bucket, we will register listeners on the bucket. 
                        CompletableFuture<Void> bucketFuture = new CompletableFuture<>();
                        
                        buckets.compute(bucket, (x, y) -> {
                            if (y == null) {
                                BucketService bucketService = bucketServiceSupplier.apply(bucket);

                                bucketService.addListener(new Listener() {
                                    @Override
                                    public void running() {
                                        super.running();
                                        log.info("{}: successfully started bucket service bucket: {} ", BucketManager.this.serviceType, bucket);
                                        bucketFuture.complete(null);
                                    }

                                    @Override
                                    public void failed(State from, Throwable failure) {
                                        super.failed(from, failure);
                                        log.error("{}: Failed to start bucket: {} ", BucketManager.this.serviceType, bucket);
                                        buckets.remove(bucket);
                                        bucketFuture.completeExceptionally(failure);
                                    }
                                }, executor);

                                bucketService.startAsync();
                                return bucketService;
                            } else {
                                bucketFuture.complete(null);
                                return y;
                            }
                        });

                        return bucketFuture;
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    @Override
    protected void doStop() {
        log.info("{}: Stop request received for bucket manager", serviceType);
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
                    stopBucketOwnershipListener();
                    if (e != null) {
                        log.error("{}: bucket service shutdown failed with exception", serviceType, e);
                        notifyFailed(e);
                    } else {
                        log.info("{}: bucket service stopped", serviceType);
                        notifyStopped();
                    }
                });
    }

    public void notify(BucketNotification notification) {
        switch (notification.getType()) {
            case BucketAvailable:
                RetryHelper.withIndefiniteRetriesAsync(() -> tryTakeOwnership(notification.getBucketId()),
                        e -> log.warn("{}: exception while attempting to take ownership for bucket {} ", serviceType,
                                notification.getBucketId(), e.getMessage()), executor);
                break;
            case ConnectivityError:
                log.warn("{}: Bucket notification for connectivity error", serviceType);
                break;
        }
    }

    abstract void startBucketOwnershipListener();

    abstract void stopBucketOwnershipListener();

    abstract CompletableFuture<Void> initializeBucket(int bucket);

    /**
     * Method to take ownership of a bucket.
     *
     * @param bucket      bucket id
     * @param processId   process id
     * @param executor    executor
     * @return future, which when completed, will contain a boolean which tells if ownership attempt succeeded or failed.
     */
    abstract CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor);

    @VisibleForTesting
    Map<Integer, BucketService> getBucketServices() {
        return Collections.unmodifiableMap(buckets);
    }

    @Data
    static class BucketNotification {
        private final int bucketId;
        private final NotificationType type;
    }

    protected enum NotificationType {
        BucketAvailable,
        ConnectivityError
    }
}
