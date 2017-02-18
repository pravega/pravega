/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.StreamConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * In-memory stream store.
 */
public class InMemoryStreamMetadataStore extends AbstractStreamMetadataStore {

    private final Map<String, InMemoryStream> streams = new HashMap<>();

    @Override
    synchronized Stream newStream(String scope, String name) {
        if (streams.containsKey(name)) {
            return streams.get(name);
        } else {
            throw new DataNotFoundException(name);
        }
    }

    @Override
    public synchronized CompletableFuture<Boolean> createStream(String scope, String name, StreamConfiguration configuration, long timeStamp, OperationContext context, Executor executor) {
        if (!streams.containsKey(name)) {
            InMemoryStream stream = new InMemoryStream(name, scope);
            stream.create(configuration, timeStamp);
            streams.put(name, stream);
            return CompletableFuture.completedFuture(true);
        } else {
            CompletableFuture<Boolean> result = new CompletableFuture<>();
            result.completeExceptionally(new StreamAlreadyExistsException(name));
            return result;
        }
    }
}
