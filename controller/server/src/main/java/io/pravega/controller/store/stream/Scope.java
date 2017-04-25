/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import java.util.List;
import java.util.concurrent.CompletableFuture;

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
