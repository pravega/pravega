/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.contracts;

/**
 * Indicates that the maximum number of Active Segments per Container has been reached and that no more segments can be
 * registered.
 */
public class TooManyActiveSegmentsException extends ContainerException {
    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId     The Id of the ContainerException.
     * @param maxSegmentCount The maximum number of active Segments per container.
     */
    public TooManyActiveSegmentsException(int containerId, int maxSegmentCount) {
        super(containerId, getMessage(maxSegmentCount));
    }

    private static String getMessage(int maxSegmentCount) {
        return String.format("The maximum number of active Segments (%d) has been reached.", maxSegmentCount);
    }
}
