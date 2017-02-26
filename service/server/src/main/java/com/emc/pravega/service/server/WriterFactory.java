/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

/**
 * Defines a Factory for Writers.
 */
public interface WriterFactory {
    /**
     * Creates a new Writer with given arguments.
     *
     * @param containerMetadata Metadata for the container that this Writer will be for.
     * @param operationLog      The OperationLog to attach to.
     * @param readIndex         The ReadIndex to attach to (to provide feedback for mergers).
     * @return An instance of a class that implements the Writer interface.
     */
    Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex);
}
