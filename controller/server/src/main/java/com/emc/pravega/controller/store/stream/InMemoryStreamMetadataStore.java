/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
    synchronized Stream newStream(String streamName) {
        if (streams.containsKey(streamName)) {
            return streams.get(streamName);
        } else {
            throw new StreamNotFoundException(streamName);
        }
    }

    @Override
    synchronized Scope newScope(String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName);
        } else {
            throw new StoreException(StoreException.Type.NODE_NOT_FOUND, "Scope not found.");
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(String scopeName, String streamName,
                                                                StreamConfiguration configuration, long timeStamp) {
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
    public synchronized CompletableFuture<CreateScopeStatus> createScope(String scopeName) {
        if (!validateZNodeName(scopeName)) {
            log.debug("Create scope failed due to invalid scope name {}", scopeName);
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
    public synchronized CompletableFuture<DeleteScopeStatus> deleteScope(String scopeName) {
        if (scopes.containsKey(scopeName)) {
            return scopes.get(scopeName).listStreamsInScope().thenApply(streams -> {
                if (streams.size() == 0) {
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
    public CompletableFuture<List<String>> listScopes() {
        return CompletableFuture.completedFuture(new ArrayList<>(scopes.keySet()));
    }

    @Override
    public CompletableFuture<List<ActiveTxRecordWithStream>> getAllActiveTx() {
        throw new NotImplementedException();
    }

    private String scopedStreamName(String scopeName, String streamName) {
        return new StringBuilder(scopeName).append("/").append(streamName).toString();
    }
}
