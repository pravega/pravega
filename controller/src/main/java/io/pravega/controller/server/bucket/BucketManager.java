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
import io.netty.util.internal.ConcurrentSet;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class BucketManager extends AbstractService implements BucketOwnershipListener {
    private final int bucketCount;
    private final String processId;
    private final ConcurrentSet<AbstractBucketService> buckets;
    private final BucketStore bucketStore;
    private final ScheduledExecutorService executor;
    private final Function<Integer, AbstractBucketService> bucketServiceSupplier;

    public BucketManager(final int bucketCount, final String processId, final BucketStore bucketStore, 
                         final ScheduledExecutorService executor,
                         final Function<Integer, AbstractBucketService> bucketServiceSupplier) {
        this.bucketCount = bucketCount;
        this.processId = processId;
        this.bucketStore = bucketStore;
        this.executor = executor;
        this.buckets = new ConcurrentSet<>();
        this.bucketServiceSupplier = bucketServiceSupplier;
    }

    @Override
    protected void doStart() {
        Futures.allOf(IntStream.range(0, bucketCount).boxed().map(this::tryTakeOwnership).collect(Collectors.toList()))
                .thenAccept(x -> bucketStore.registerBucketOwnershipListener(this))
                .whenComplete((r, e) -> {
                    if (e != null) {
                        notifyFailed(e);
                    } else {
                        notifyStarted();
                    }
                });
    }

    private CompletableFuture<Void> tryTakeOwnership(int bucket) {
        return RetryHelper.withIndefiniteRetriesAsync(() -> bucketStore.takeBucketOwnership(bucket, processId, executor),
                e -> log.warn("exception while attempting to take ownership"), executor)
                .thenCompose(isOwner -> {
                    if (isOwner && buckets.stream().noneMatch(x -> x.getBucketId() == bucket)) {
                        log.info("Taken ownership for bucket {}", bucket);
                        
                        AbstractBucketService bucketService = bucketServiceSupplier.apply(bucket);
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
                                buckets.remove(bucketService);
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
                    bucketStore.unregisterBucketOwnershipListener();
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
    Set<AbstractBucketService> getBucketServices() {
        return Collections.unmodifiableSet(buckets);
    }
}
