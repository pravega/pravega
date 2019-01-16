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
import io.pravega.controller.store.stream.BucketStore;
import io.pravega.controller.store.stream.ZookeeperBucketStore;
import lombok.Lombok;
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
        PathChildrenCacheListener bucketListener = (client, event) -> {
            switch (event.getType()) {
                case CHILD_ADDED:
                    // no action required
                    break;
                case CHILD_REMOVED:
                    int bucketId = Integer.parseInt(ZKPaths.getNodeFromPath(event.getData().getPath()));
                    notify(new BucketNotification(bucketId, NotificationType.BucketAvailable));
                    break;
                case CONNECTION_LOST:
                    notify(new BucketNotification(Integer.MIN_VALUE, NotificationType.ConnectivityError));
                    break;
                default:
                    log.warn("Received unknown event {} on bucket root {} ", event.getType(), getServiceType());
            }
        };

        PathChildrenCache pathChildrenCache = bucketOwnershipCacheMap.computeIfAbsent(getServiceType(),
                x -> {
                    PathChildrenCache cache = bucketStore.getServiceOwnershipPathChildrenCache(getServiceType());
                    cache.getListenable().addListener(bucketListener);
                    log.info("bucket ownership listener registered on bucket root {}", getServiceType());

                    return cache;
                });

        try {
            pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
        } catch (Exception e) {
            log.error("Starting ownership listener for service {} threw exception", getServiceType(), e);
            throw Lombok.sneakyThrow(e);
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
    public CompletableFuture<Void> initializeBucket(int bucket) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount());
        
        return bucketStore.initializeBucket(getServiceType(), bucket);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketStore.getBucketCount());
        return bucketStore.takeBucketOwnership(getServiceType(), bucket, processId);
    }
}
