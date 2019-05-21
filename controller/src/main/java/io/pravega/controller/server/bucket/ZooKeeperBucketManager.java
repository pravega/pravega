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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import io.pravega.controller.util.RetryHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

@Slf4j
public class ZooKeeperBucketManager extends BucketManager {
    private final ZookeeperBucketStore bucketStore;
    private final ConcurrentMap<BucketStore.ServiceType, PathChildrenCache> bucketOwnershipCacheMap;

    ZooKeeperBucketManager(String processId, ZookeeperBucketStore bucketStore, BucketStore.ServiceType serviceType, ScheduledExecutorService executor,
                           Function<Integer, BucketService> bucketServiceSupplier) {
        super(processId, serviceType, executor, bucketServiceSupplier);
        bucketOwnershipCacheMap = new ConcurrentHashMap<>();
        this.bucketStore = bucketStore;
    }

    @Override
    protected int getBucketCount() {
        return bucketStore.getBucketCount();
    }

    @Override
    public void startBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.computeIfAbsent(getServiceType(),
                x -> bucketStore.getServiceOwnershipPathChildrenCache(getServiceType()));

        PathChildrenCacheListener bucketListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // no action required
                    break;
                case CHILD_REMOVED:
                    int bucketId = Integer.parseInt(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    // if we are greater than our fair share --> introduce a delay
                    // TODO: shivesh
                    Futures.delayedFuture();
                    RetryHelper.withIndefiniteRetriesAsync(() -> tryTakeOwnership(bucketId),
                            e -> log.warn("{}: exception while attempting to take ownership for bucket {} ", getServiceType(),
                                    bucketId, e.getMessage()), getExecutor());
                    break;
                case CONNECTION_LOST:
                    log.warn("{}: Received connectivity error", getServiceType());
                    break;
                default:
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), getServiceType());
            }
        };

        pathChildrenCache.getListenable().addListener(bucketListener);
        log.info("bucket ownership listener registered on bucket root {}", getServiceType());

        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        } catch (Exception e) {
            log.error("Starting ownership listener for service {} threw exception", getServiceType(), e);
            throw Exceptions.sneakyThrow(e);
        }

    }

    @Override
    public void stopBucketOwnershipListener() {
        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.remove(getServiceType());
        if (pathChildrenCache != null) {
            try {
                pathChildrenCache.clear();
                pathChildrenCache.close();
            } catch (IOException e) {
                log.warn("unable to close listener for bucket ownership", e);
            }
        }
    }

    @Override
    public CompletableFuture<Void> initializeService() {
        return bucketStore.createBucketsRoot(getServiceType());
    }

    @Override
    public CompletableFuture<Void> initializeBucket(int bucket) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount());
        
        return bucketStore.createBucket(getServiceType(), bucket);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount());
        return bucketStore.takeBucketOwnership(getServiceType(), bucket, processId);
    }
}
