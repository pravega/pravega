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

import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.concurrent.Futures;
import io.pravega.controller.store.index.InMemoryHostIndex;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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

    private final Executor executor;

    InMemoryStreamMetadataStore(Executor executor) {
        super(new InMemoryHostIndex());
        this.executor = executor;
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
            return stream.create(configuration, timeStamp)
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
