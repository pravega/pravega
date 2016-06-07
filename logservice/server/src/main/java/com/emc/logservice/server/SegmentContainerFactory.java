package com.emc.logservice.server;

/**
 * Defiones a Factory for SegmentContainers.
 */
public interface SegmentContainerFactory {
    /**
     * Creates a new instance of a SegmentContainer.
     * @param containerId The Id of the container to create.
     * @return
     */
    SegmentContainer createStreamSegmentContainer(String containerId);
}
