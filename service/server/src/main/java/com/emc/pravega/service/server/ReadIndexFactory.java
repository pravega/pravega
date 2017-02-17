/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

/**
 * Defines a Factory for ReadIndex objects.
 */
public interface ReadIndexFactory extends AutoCloseable {
    /**
     * Creates an instance of a ReadIndex class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this ReadIndex.
     * @throws NullPointerException If any of the arguments are null.
     */
    ReadIndex createReadIndex(ContainerMetadata containerMetadata);

    @Override
    void close();
}
