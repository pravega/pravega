/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

/**
 * Indicates that the maximum number of Active Segments per Container has been reached and that no more segments can be
 * registered.
 */
public class TooManyActiveSegmentsException extends ContainerException {
    private static final long serialVersionUID = 1L;

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
