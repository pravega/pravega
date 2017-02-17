/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

/**
 * Defines a Metadata Repository; helps manage Metadata for Containers.
 */
public interface MetadataRepository {
    /**
     * Gets a reference to the UpdateableContainerMetadata with given id.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     */
    UpdateableContainerMetadata getMetadata(int streamSegmentContainerId);
}
