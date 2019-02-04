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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Properties of a Scope and operations that can be performed on it.
 * Identifier for a Scope is its name.
 */
public interface Scope {

    /**
     * Retrieve name of the scope.
     *
     * @return Name of the scope
     */
    String getName();

    /**
     * Create the scope.
     *
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> createScope();

    /**
     * Delete the scope.
     *
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> deleteScope();

    /**
     * Api to add stream under a scope for listing streams. 
     * 
     * @param name name of stream
     * @param creationTime  creation time of stream.
     * @return Future, which upon completion will indicate that the stream has been added to the scope. 
     */
    CompletableFuture<Void> addStreamToScope(final String name, final long creationTime);

    /**
     * Api to remove stream under a scope for listing streams. 
     *
     * @param name name of stream
     * @param creationTime  creation time of stream.
     * @return Future, which upon completion will indicate that the stream has been removed from the scope. 
     */
    CompletableFuture<Void> removeStreamFromScope(final String name, final long creationTime);

    /**
     * A paginated api on the scope to get requested number of streams from under the scope starting from the continuation token. 
     * 
     * @param limit maximum number of streams to return
     * @param continuationToken continuation token from where to start.
     * @param executor executor
     * @return A future, which upon completion, will hold a pair of list of stream names and a new continuation token. 
     */
    CompletableFuture<Pair<List<String>, String>> listStreamsInScope(final int limit, final String continuationToken, 
                                                                     final Executor executor);

    /**
     * List existing streams in scopes.
     *
     * @return List of streams in scope
     */
    CompletableFuture<List<String>> listStreamsInScope();
    
    /**
     * Refresh the scope object. Typically to be used to invalidate any caches.
     * This allows us reuse of scope object without having to recreate a new scope object for each new operation
     */
    void refresh();

    static String encodeStreamInScope(String name, long creationTime) {
        String streamWithCreationTime = name + "/" + creationTime;
        return Base64.getEncoder().encodeToString(streamWithCreationTime.getBytes());
    }

    static Pair<String, Long> decodeStreamInScope(String encodedName) {
        String[] decoded = new String(Base64.getDecoder().decode(encodedName)).split("/");
        return new ImmutablePair<>(decoded[0], Long.parseLong(decoded[1]));
    }
    
    static int compareStreamInScope(String encodedToken1, String encodedToken2) {
        Pair<String, Long> decodedToken1 = decodeStreamInScope(encodedToken1);
        Pair<String, Long> decodedToken2 = decodeStreamInScope(encodedToken2);
        
        if (decodedToken1.getValue() < decodedToken2.getValue()) {
            return -1;
        } else if (decodedToken1.getValue().longValue() == decodedToken2.getValue().longValue()) {
            return decodedToken1.getKey().compareTo(decodedToken2.getKey());
        } else {
            return 1;
        }
    }
}
