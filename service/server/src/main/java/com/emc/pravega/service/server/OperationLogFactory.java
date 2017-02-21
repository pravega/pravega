/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.service.server.logs.CacheUpdater;

/**
 * Defines a Factory for DurableLog Components.
 */
public interface OperationLogFactory {

    /**
     * Creates a new instance of an OperationLog class.
     *
     * @param containerMetadata The Metadata for the create the DurableLog for.
     * @param cacheUpdater      A CacheUpdater that can be used to store new appends in.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the metadata is already in recovery mode.
     */
    OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, CacheUpdater cacheUpdater);
}
