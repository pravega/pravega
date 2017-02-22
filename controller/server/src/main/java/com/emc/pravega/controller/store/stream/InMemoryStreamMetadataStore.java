/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.controller.store.stream.tables.ActiveTxRecordWithStream;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.DeleteScopeStatus;
import com.emc.pravega.stream.StreamConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * In-memory stream store.
 */
@Slf4j
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    private final Map<String, InMemoryStream> streams = new HashMap<>();
    private final Map<String, InMemoryScope> scopes = new HashMap<>();

    @Override
    synchronized Stream newStream(final String streamName) {
        Stream stream = streams.get(streamName);
        if (stream != null) {
            return stream;
        } else {
            throw new StreamNotFoundException(streamName);
        }
    }

    @Override
    synchronized Scope newScope(final String scopeName) {
        return scopes.get(scopeName);
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(final String scopeName, final String streamName,
                                                                final StreamConfiguration configuration,
                                                                final long timeStamp) {
        if (!validateName(streamName)) {
            log.error("Create stream failed due to invalid stream name {}", scopeName);
            return CompletableFuture.completedFuture(false);
        } else {
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
    }

    @Override
    public synchronized CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        if (!validateName(scopeName)) {
            log.error("Create scope failed due to invalid scope name {}", scopeName);
            return CompletableFuture.completedFuture(CreateScopeStatus.FAILURE);
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
    public synchronized CompletableFuture<DeleteScopeStatus> deleteScope(final String scopeName) {
        if (scopes.containsKey(scopeName)) {
            if (scopes.get(scopeName).getStreamsInScope().size() == 0) {
                scopes.get(scopeName).deleteScope();
                scopes.remove(scopeName);
                return CompletableFuture.completedFuture(DeleteScopeStatus.SUCCESS);
            } else {
                return CompletableFuture.completedFuture(DeleteScopeStatus.SCOPE_NOT_EMPTY);
            }
        } else {
            return CompletableFuture.completedFuture(DeleteScopeStatus.SCOPE_NOT_FOUND);
        }
    }

    @Override
    public CompletableFuture<List<String>> listScopes() {
        return CompletableFuture.completedFuture(new ArrayList<>(scopes.keySet()));
    }

    @Override
    public CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        throw new NotImplementedException();
    }

    private String scopedStreamName(final String scopeName, final String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
