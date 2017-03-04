/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.storage.Cache;

/**
 * Defines a Factory for ReadIndex objects.
 */
public interface ReadIndexFactory extends AutoCloseable {
    /**
     * Creates an instance of a ReadIndex class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this ReadIndex.
     * @param cache             The cache to use for the ReadIndex.
     * @throws NullPointerException If any of the arguments are null.
     */
    ReadIndex createReadIndex(ContainerMetadata containerMetadata, Cache cache);

    @Override
    void close();
}
