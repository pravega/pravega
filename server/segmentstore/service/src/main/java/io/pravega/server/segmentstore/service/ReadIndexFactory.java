/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service;

import io.pravega.server.segmentstore.storage.ReadOnlyStorage;

/**
 * Defines a Factory for ReadIndex objects.
 */
public interface ReadIndexFactory extends AutoCloseable {
    /**
     * Creates an instance of a ReadIndex class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this ReadIndex.
     * @param storage           A ReadOnlyStorage to use for reading data that is not in the cache.
     */
    ReadIndex createReadIndex(ContainerMetadata containerMetadata, ReadOnlyStorage storage);

    @Override
    void close();
}
