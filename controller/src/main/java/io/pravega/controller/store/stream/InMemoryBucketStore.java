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
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.controller.server.periodic.BucketChangeListener;
import io.pravega.controller.server.periodic.BucketOwnershipListener;
import lombok.Synchronized;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TODO: shivesh
 */
public class InMemoryBucketStore implements BucketStore {
    private final int bucketCount;
    
    @GuardedBy("$lock")
    private final Map<Integer, List<String>> bucketedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, RetentionPolicy> streamPolicyMap = new HashMap<>();

    private final AtomicReference<BucketOwnershipListener> ownershipListenerRef;

    private final ConcurrentMap<Integer, BucketChangeListener> listeners;

    protected InMemoryBucketStore(int bucketCount) {
        this.bucketCount = bucketCount;
        ownershipListenerRef = new AtomicReference<>();
        listeners = new ConcurrentHashMap<>();
    }


    @Synchronized
    @Override
    public void registerBucketChangeListenerForRetention(int bucket, BucketChangeListener listener) {
        listeners.put(bucket, listener);
    }

    @Synchronized
    @Override
    public void unregisterBucketChangeListenerForRetention(int bucket) {
        listeners.remove(bucket);
    }

    @Override
    public void registerBucketOwnershipListener(BucketOwnershipListener ownershipListener) {
        this.ownershipListenerRef.set(ownershipListener);
    }

    @Override
    public void unregisterBucketOwnershipListener() {
        this.ownershipListenerRef.set(null);
    }

    @Override
    public CompletableFuture<Boolean> takeBucketOwnership(int bucket, String processId, Executor executor) {
        Preconditions.checkArgument(bucket < bucketCount);
        return CompletableFuture.completedFuture(true);
    }

    @Synchronized
    @Override
    public CompletableFuture<List<String>> getStreamsForRetention(int bucket, Executor executor) {
        if (bucketedStreams.containsKey(bucket)) {
            return CompletableFuture.completedFuture(Collections.unmodifiableList(bucketedStreams.get(bucket)));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addUpdateStreamForRetention(String scope, String stream, RetentionPolicy retentionPolicy,
                                                                   Executor executor) {
        Preconditions.checkNotNull(retentionPolicy);
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        List<String> list;
        if (bucketedStreams.containsKey(bucket)) {
            list = bucketedStreams.get(bucket);
        } else {
            list = new ArrayList<>();
        }
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);
        list.add(scopedStreamName);
        bucketedStreams.put(bucket, list);

        final boolean isUpdate = streamPolicyMap.containsKey(scopedStreamName);

        streamPolicyMap.put(scopedStreamName, retentionPolicy);

        return CompletableFuture.runAsync(() -> listeners.computeIfPresent(bucket, (b, listener) -> {
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
    public CompletableFuture<Void> removeStreamFromRetention(String scope, String stream, Executor executor) {
        int bucket = BucketStore.getBucket(scope, stream, bucketCount);
        String scopedStreamName = BucketStore.getScopedStreamName(scope, stream);

        bucketedStreams.computeIfPresent(bucket, (b, list) -> {
            list.remove(scopedStreamName);
            return list;
        });

        streamPolicyMap.remove(scopedStreamName);

        return CompletableFuture.runAsync(() -> listeners.computeIfPresent(bucket, (b, listener) -> {
            listener.notify(new BucketChangeListener.StreamNotification(scope, stream,
                    BucketChangeListener.StreamNotification.NotificationType.StreamRemoved));
            return listener;
        }), executor);
    }
}
