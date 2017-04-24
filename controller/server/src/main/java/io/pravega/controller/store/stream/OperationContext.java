/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

/**
 * Interface for defining an operation context.
 * A context caches metadata fetches so within a context if for the same entity, multiple
 * read operations against the store are requested, the values are served from the context's cache.
 */
public interface OperationContext {

    Stream getStream();
}
