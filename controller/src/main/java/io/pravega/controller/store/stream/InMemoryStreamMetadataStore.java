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
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.controller.server.retention.BucketChangeListener;
import io.pravega.controller.server.retention.BucketOwnershipListener;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

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
import java.util.stream.Collectors;

/**
 * In-memory stream store.
 */
@Slf4j
class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<Integer, List<String>> bucketedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, RetentionPolicy> streamPolicyMap = new HashMap<>();

    private final AtomicReference<BucketOwnershipListener> ownershipListenerRef;

    private final ConcurrentMap<Integer, BucketChangeListener> listeners;
    private final AtomicInt96 counter;

    private final Executor executor;

    InMemoryStreamMetadataStore(int bucketCount, Executor executor) {
        super(new InMemoryHostIndex(), bucketCount);
        this.listeners = new ConcurrentHashMap<>();
        this.executor = executor;
        this.ownershipListenerRef = new AtomicReference<>();
        this.counter = new AtomicInt96();
    }

    @Override
    @Synchronized
    Stream newStream(String scope, String name) {
        if (streams.containsKey(scopedStreamName(scope, name))) {
            return streams.get(scopedStreamName(scope, name));
        } else {
            return new InMemoryStream(scope, name);
        }
    }

    @Override
    CompletableFuture<Int96> getNextCounter() {
        return CompletableFuture.completedFuture(counter.incrementAndGet());
    }

    @Override
    @Synchronized
    Scope newScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            return new InMemoryScope(scopeName);
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<CreateStreamResponse> createStream(final String scopeName, final String streamName,
                                                                final int startingSegmentNumber,
                                                                final StreamConfiguration configuration,
                                                                final long timeStamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            InMemoryStream stream = (InMemoryStream) getStream(scopeName, streamName, context);
            return stream.create(configuration, timeStamp, startingSegmentNumber)
                    .thenApply(x -> {
                        streams.put(scopedStreamName(scopeName, streamName), stream);
                        scopes.get(scopeName).addStreamToScope(streamName);
                        return x;
                    });
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> checkStreamExists(final String scopeName,
                                                        final String streamName) {
        return CompletableFuture.completedFuture(streams.containsKey(scopedStreamName(scopeName, streamName)));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteStream(final String scopeName, final String streamName,
                                                final OperationContext context,
                                                final Executor executor) {
        String scopedStreamName = scopedStreamName(scopeName, streamName);
        if (scopes.containsKey(scopeName) && streams.containsKey(scopedStreamName)) {
            streams.remove(scopedStreamName);
            scopes.get(scopeName).removeStreamFromScope(streamName);
            return super.deleteStream(scopeName, streamName, context, executor);
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, streamName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> startUpdateConfiguration(final String scopeName,
                                                            final String streamName,
                                                            final StreamConfiguration configuration,
                                                            final OperationContext context,
                                                            final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            String scopeStreamName = scopedStreamName(scopeName, streamName);
            if (!streams.containsKey(scopeStreamName)) {
                return Futures.
                        failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeStreamName));
            }
            return streams.get(scopeStreamName).startUpdateConfiguration(configuration);
        } else {
            return Futures.
                    failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Synchronized
    @Override
    public void registerBucketChangeListener(int bucket, BucketChangeListener listener) {
        listeners.put(bucket, listener);
    }

    @Synchronized
    @Override
    public void unregisterBucketListener(int bucket) {
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
    public CompletableFuture<List<String>> getStreamsForBucket(int bucket, Executor executor) {
        if (bucketedStreams.containsKey(bucket)) {
            return CompletableFuture.completedFuture(Collections.unmodifiableList(bucketedStreams.get(bucket)));
        } else {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
    }

    @Synchronized
    @Override
    public CompletableFuture<Void> addUpdateStreamForAutoStreamCut(String scope, String stream, RetentionPolicy retentionPolicy,
                                                                   OperationContext context, Executor executor) {
        Preconditions.checkNotNull(retentionPolicy);
        int bucket = getBucket(scope, stream);
        List<String> list;
        if (bucketedStreams.containsKey(bucket)) {
            list = bucketedStreams.get(bucket);
        } else {
            list = new ArrayList<>();
        }
        String scopedStreamName = getScopedStreamName(scope, stream);
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
    public CompletableFuture<Void> removeStreamFromAutoStreamCut(String scope, String stream, OperationContext context, Executor executor) {
        int bucket = getBucket(scope, stream);
        String scopedStreamName = getScopedStreamName(scope, stream);

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

    @Override
    @Synchronized
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!scopes.containsKey(scopeName)) {
            InMemoryScope scope = new InMemoryScope(scopeName);
            scope.createScope();
            scopes.put(scopeName, scope);
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.SUCCESS).build());
        } else {
            return CompletableFuture.completedFuture(CreateScopeStatus.newBuilder().setStatus(
                    CreateScopeStatus.Status.SCOPE_EXISTS).build());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName).listStreamsInScope().thenApply(streams -> {
                if (streams.isEmpty()) {
                    scopes.get(scopeName).deleteScope();
                    scopes.remove(scopeName);
                    return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SUCCESS).build();
                } else {
                    return DeleteScopeStatus.newBuilder().setStatus(DeleteScopeStatus.Status.SCOPE_NOT_EMPTY).build();
                }
            });
        } else {
            return CompletableFuture.completedFuture(DeleteScopeStatus.newBuilder().setStatus(
                    DeleteScopeStatus.Status.SCOPE_NOT_FOUND).build());
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<String> getScopeConfiguration(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return CompletableFuture.completedFuture(scopeName);
        } else {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<List<String>> listScopes() {
        return CompletableFuture.completedFuture(new ArrayList<>(scopes.keySet()));
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return List of streams in scope
     */
    @Override
    @Synchronized
    public CompletableFuture<List<StreamConfiguration>> listStreamsInScope(final String scopeName) {
        InMemoryScope inMemoryScope = scopes.get(scopeName);
        if (inMemoryScope != null) {
            return inMemoryScope.listStreamsInScope()
                    .thenApply(streams -> streams.stream().map(
                            stream -> this.getConfiguration(scopeName, stream, null, executor).join()).collect(Collectors.toList()));
        } else {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
