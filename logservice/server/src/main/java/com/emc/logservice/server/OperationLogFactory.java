package com.emc.logservice.server;

import com.emc.logservice.server.logs.OperationLog;

/**
 * Defines a Factory for DurableLog Components.
 */
public interface OperationLogFactory {

    /**
     * Creates a new instance of an OperationLog class.
     *
     * @param containerMetadata The Metadata for the create the DurableLog for.
     * @param cache             A ReadIndex to store new data in.
     * @return The result.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the metadata is already in recovery mode.
     */
    OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, Cache cache);
}
