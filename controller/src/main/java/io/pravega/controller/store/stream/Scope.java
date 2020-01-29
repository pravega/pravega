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

import org.apache.commons.lang3.tuple.Pair;

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
     * A paginated api on the scope to get requested number of streams from under the scope starting from the continuation token. 
     * 
     * @param limit maximum number of streams to return
     * @param continuationToken continuation token from where to start.
     * @param executor executor
     * @return A future, which upon completion, will hold a pair of list of stream names and a new continuation token. 
     */
    CompletableFuture<Pair<List<String>, String>> listStreams(final int limit, final String continuationToken,
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
}
