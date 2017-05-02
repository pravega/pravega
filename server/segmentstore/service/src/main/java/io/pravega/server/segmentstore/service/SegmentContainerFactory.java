/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.service;

/**
 * Defines a Factory for SegmentContainers.
 */
public interface SegmentContainerFactory {
    /**
     * Creates a new instance of a SegmentContainer.
     *
     * @param containerId The Id of the container to create.
     */
    SegmentContainer createStreamSegmentContainer(int containerId);
}
