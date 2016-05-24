package com.emc.logservice.server;

/**
 * Defines a Metadata Repository; helps manage Metadata for Containers.
 */
public interface MetadataRepository {
    /**
     * Gets a reference to the UpdateableContainerMetadata with given id.
     * TODO: figure out arguments.
     * TODO: Consider returning an interface.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @return The result.
     */
    UpdateableContainerMetadata getMetadata(String streamSegmentContainerId);
}
