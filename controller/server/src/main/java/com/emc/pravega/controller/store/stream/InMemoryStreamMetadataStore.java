/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    @GuardedBy("$lock")
    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @GuardedBy("$lock")
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    @Override
    @Synchronized
    Stream newStream(String scope, String name) {
        Stream stream = streams.get(scopedStreamName(scope, name));
        if (stream != null) {
            return stream;
        } else {
            throw new DataNotFoundException(name);
        }
    }

    @Override
    @Synchronized
    Scope newScope(final String scopeName) {
        return scopes.get(scopeName);
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> createStream(final String scopeName, final String streamName,
                                                   final StreamConfiguration configuration,
                                                   final long timeStamp,
                                                   final OperationContext context,
                                                   final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            if (!streams.containsKey(scopedStreamName(scopeName, streamName))) {
                InMemoryStream stream = new InMemoryStream(scopeName, streamName);
                stream.create(configuration, timeStamp);
                streams.put(scopedStreamName(scopeName, streamName), stream);
                scopes.get(scopeName).addStreamToScope(streamName);
                return CompletableFuture.completedFuture(true);
            } else {
                return FutureHelpers.
                        failedFuture(new StoreException(StoreException.Type.NODE_EXISTS, "Stream already exists."));
            }
        } else {
            return FutureHelpers.
                    failedFuture(new StoreException(StoreException.Type.NODE_NOT_FOUND, "Scope not found."));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<Boolean> updateConfiguration(final String scopeName,
                                                          final String streamName,
                                                          final StreamConfiguration configuration,
                                                          final OperationContext context,
                                                          final Executor executor) {
        if (scopes.containsKey(scopeName)) {
            return streams.get(scopedStreamName(scopeName, streamName)).updateConfiguration(configuration);
        } else {
            return FutureHelpers.
                    failedFuture(new StoreException(StoreException.Type.NODE_NOT_FOUND, "Scope not found."));
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!validateName(scopeName)) {
            log.error("Create scope failed due to invalid scope name {}", scopeName);
            return CompletableFuture.completedFuture(CreateScopeStatus.INVALID_SCOPE_NAME);
        } else {
            if (!scopes.containsKey(scopeName)) {
                InMemoryScope scope = new InMemoryScope(scopeName);
                scope.createScope();
                scopes.put(scopeName, scope);
                return CompletableFuture.completedFuture(CreateScopeStatus.SUCCESS);
            } else {
                return CompletableFuture.completedFuture(CreateScopeStatus.SCOPE_EXISTS);
            }
        }
    }

    @Override
    @Synchronized
    public CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName).listStreamsInScope().thenApply((streams) -> {
                if (streams.isEmpty()) {
                    scopes.get(scopeName).deleteScope();
                    scopes.remove(scopeName);
                    return DeleteScopeStatus.SUCCESS;
                } else {
                    return DeleteScopeStatus.SCOPE_NOT_EMPTY;
                }
            });
        } else {
            return CompletableFuture.completedFuture(DeleteScopeStatus.SCOPE_NOT_FOUND);
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
    public CompletableFuture<List<String>> listStreamsInScope(final String scopeName) {
        InMemoryScope inMemoryScope = scopes.get(scopeName);
        if (inMemoryScope != null) {
            return inMemoryScope.listStreamsInScope();
        } else {
            return FutureHelpers.failedFuture(StoreException.create(StoreException.Type.NODE_NOT_FOUND));
        }
    }

    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
