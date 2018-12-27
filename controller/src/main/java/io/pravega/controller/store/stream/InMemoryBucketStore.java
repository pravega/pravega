/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.controller.server.bucket.BucketChangeListener;
import io.pravega.controller.server.bucket.BucketOwnershipListener;
import lombok.Getter;
import lombok.Synchronized;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 * TODO: shivesh
 */
public class InMemoryBucketStore implements BucketStore {
    @Getter
    private final int bucketCount;
    
    private final ConcurrentMap<String, Set<String>> bucketedStreams;
    
    private final ConcurrentMap<String, BucketChangeListener> listeners;

    InMemoryBucketStore(int bucketCount) {
        this.bucketCount = bucketCount;
        listeners = new ConcurrentHashMap<>();
        bucketedStreams = new ConcurrentHashMap<>();
    }


    @Override
    public void registerBucketChangeListener(ServiceType serviceType, int bucket, BucketChangeListener listener) {
        listeners.put(getBucketName(serviceType, bucket), listener);
    }

    private String getBucketName(ServiceType serviceType, int bucket) {
        return serviceType.getName() + "/" + bucket;
    }

    @Override
    public void unregisterBucketChangeListener(ServiceType serviceType, int bucket) {
        listeners.remove(getBucketName(serviceType, bucket));
    }

    @Override
    public void registerBucketOwnershipListener(ServiceType serviceType, BucketOwnershipListener ownershipListener) {
    }

    @Override
    public void unregisterBucketOwnershipListener(ServiceType serviceType) {
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(ServiceType serviceType, int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketCount);
        return CompletableFuture.completedFuture(true);
    }

    @Synchronized
    @Override
    public CompletableFuture<List<String>> getStreamsForBucket(ServiceType serviceType, int bucket, Executor executor) {
        String bucketName = getBucketName(serviceType, bucket);
        if (bucketedStreams.containsKey(bucketName)) {
            return CompletableFuture.completedFuture(ImmutableList.copyOf(bucketedStreams.get(bucketName)));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addStreamToBucketStore(ServiceType serviceType, String scope, String stream, Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketName = getBucketName(serviceType, bucket);
        Set<String> set;
        if (bucketedStreams.containsKey(bucketName)) {
            set = bucketedStreams.get(bucketName);
        } else {
            set = new HashSet<>();
        }
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);
        boolean isUpdate = set.contains(bucketName);
        set.add(scopedStreamName);
        bucketedStreams.put(bucketName, set);
        
        return CompletableFuture.runAsync(() -> listeners.computeIfPresent(bucketName, (b, listener) -> {
            if (isUpdate) {
                listener.notify(new BucketChangeListener.StreamNotification(scope, stream,
                        BucketChangeListener.StreamNotification.NotificationType.StreamUpdated));
            } else {
                listener.notify(new BucketChangeListener.StreamNotification(scope, stream,
                        BucketChangeListener.StreamNotification.NotificationType.StreamAdded));
            }
            return listener;
        }), executor);
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> removeStreamFromBucketStore(ServiceType serviceType, String scope, String stream, Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String bucketName = getBucketName(serviceType, bucket);

        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);

        bucketedStreams.computeIfPresent(bucketName, (b, set) -> {
            set.remove(scopedStreamName);
            return set;
        });
        
        return CompletableFuture.runAsync(() -> listeners.computeIfPresent(getBucketName(serviceType, bucket), (b, listener) -> {
            listener.notify(new BucketChangeListener.StreamNotification(scope, stream,
                    BucketChangeListener.StreamNotification.NotificationType.StreamRemoved));
            return listener;
        }), executor);
    }
}
