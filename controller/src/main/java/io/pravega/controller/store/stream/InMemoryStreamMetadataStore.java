/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.internal.LinkedTreeMap;
import io.pravega.client.stream.RetentionPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.common.lang.Int96;
import io.pravega.controller.store.InMemoryScope;
import io.pravega.controller.store.Scope;
import io.pravega.controller.store.Version;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, Integer> deletedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final HashMap<String, InMemoryScope> scopes = new HashMap<>();
    private final AtomicInteger position = new AtomicInteger();
    @GuardedBy("$lock")
    private final LinkedTreeMap<String, Integer> orderedScopes = new LinkedTreeMap<>();
    
    @GuardedBy("$lock")
    private final Map<Integer, List<String>> bucketedStreams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, RetentionPolicy> streamPolicyMap = new HashMap<>();

    private final AtomicInt96 counter;

    private final Executor executor;

    public InMemoryStreamMetadataStore(Executor executor) {
        super(new InMemoryHostIndex(), new InMemoryHostIndex());
        this.executor = executor;
        this.counter = new AtomicInt96();
    }

    public Map<String, InMemoryScope> getScopes() {
        return scopes;
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
    CompletableFuture<Boolean> checkScopeExists(String scope) {
        return CompletableFuture.completedFuture(scopes.containsKey(scope));
    }

    @Override
    Version getEmptyVersion() {
        return Version.IntVersion.EMPTY;
    }

    @Override
    Version parseVersionData(byte[] data) {
        return Version.IntVersion.fromBytes(data);
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
                                                                final StreamConfiguration configuration,
                                                                final long timeStamp,
                                                                final OperationContext context,
                                                                final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            InMemoryStream stream = (InMemoryStream) getStream(scopeName, streamName, context);
            return getSafeStartingSegmentNumberFor(scopeName, streamName)
                    .thenCompose(startingSegmentNumber -> stream.create(configuration, timeStamp, startingSegmentNumber)
                    .thenCompose(status -> {
                        streams.put(scopedStreamName(scopeName, streamName), stream);
                        return scopes.get(scopeName).addStreamToScope(streamName).thenApply(v -> status);
                    }));
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
    public CompletableFuture<Integer> getSafeStartingSegmentNumberFor(final String scopeName, final String streamName) {
        final Integer safeStartingSegmentNumber = deletedStreams.get(scopedStreamName(scopeName, streamName));
        return CompletableFuture.completedFuture((safeStartingSegmentNumber != null) ? safeStartingSegmentNumber + 1 : 0);
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> deleteStream(final String scopeName, final String streamName,
                                                final OperationContext context,
                                                final Executor executor) {
        String scopedStreamName = scopedStreamName(scopeName, streamName);
        if (scopes.containsKey(scopeName) && streams.containsKey(scopedStreamName)) {
            streams.remove(scopedStreamName);
            return getCreationTime(scopeName, streamName, context, executor)
                    .thenCompose(time -> scopes.get(scopeName).removeStreamFromScope(streamName))
                    .thenCompose(v -> super.deleteStream(scopeName, streamName, context, executor));
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
    
    @Override
    @Synchronized
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!scopes.containsKey(scopeName)) {
            InMemoryScope scope = new InMemoryScope(scopeName);
            scope.createScope();
            scopes.put(scopeName, scope);
            orderedScopes.put(scopeName, position.incrementAndGet());
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
                    orderedScopes.remove(scopeName);
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

    @Override
    public CompletableFuture<Pair<List<String>, String>> listScopes(String continuationToken, int limit, Executor executor) {
        List<String> result = new ArrayList<>();
        String nextToken = continuationToken;
        int start = Strings.isNullOrEmpty(continuationToken) ? 0 : Integer.parseInt(continuationToken);
        for (Map.Entry<String, Integer> x : orderedScopes.entrySet()) {
            if (x.getValue() > start) {
                result.add(x.getKey());
                nextToken = x.getValue().toString();
            }
            if (result.size() == limit) {
                break;
            }
        }
        return CompletableFuture.completedFuture(new ImmutablePair<>(result, nextToken));
    }

    /**
     * List the streams in scope.
     *
     * @param scopeName Name of scope
     * @return List of streams in scope
     */
    @Override
    @Synchronized
    public CompletableFuture<Map<String, StreamConfiguration>> listStreamsInScope(final String scopeName) {
        InMemoryScope inMemoryScope = scopes.get(scopeName);
        if (inMemoryScope != null) {
            return inMemoryScope.listStreamsInScope()
                    .thenApply(streams -> {
                        HashMap<String, StreamConfiguration> result = new HashMap<>();
                        for (String stream : streams) {
                            State state = getState(scopeName, stream, true, null, executor).join();
                            StreamConfiguration configuration = Futures.exceptionallyExpecting(
                                    getConfiguration(scopeName, stream, null, executor),
                                    e -> e instanceof StoreException.DataNotFoundException, null).join();
                            if (configuration != null && !state.equals(State.CREATING) && !state.equals(State.UNKNOWN)) {
                                result.put(stream, configuration);
                            }
                        }
                        return result;
                    });
        } else {
            return Futures.failedFuture(StoreException.create(StoreException.Type.DATA_NOT_FOUND, scopeName));
        }
    }

    @Override
    @Synchronized
    CompletableFuture<Void> recordLastStreamSegment(final String scope, final String stream, int lastActiveSegment,
                                                    OperationContext context, final Executor executor) {
        Integer oldLastActiveSegment = deletedStreams.put(getScopedStreamName(scope, stream), lastActiveSegment);
        Preconditions.checkArgument(oldLastActiveSegment == null || lastActiveSegment >= oldLastActiveSegment);
        log.debug("Recording last segment {} for stream {}/{} on deletion.", lastActiveSegment, scope, stream);
        return CompletableFuture.completedFuture(null);
    }
    
    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }

    @Override
    public void close() throws IOException {
        
    }
    
    @VisibleForTesting
    void addStreamObjToScope(String scopeName, String streamName) {
        InMemoryStream stream = (InMemoryStream) getStream(scopeName, streamName, null);

        streams.put(scopedStreamName(scopeName, streamName), stream);
        scopes.get(scopeName).addStreamToScope(streamName).join();
    }
}
