/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server;

import com.google.common.util.concurrent.Service;

/**
 * Exception thrown whenever a Container is in an invalid State.
 */
public class IllegalContainerStateException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public IllegalContainerStateException(String message) {
        super(message);
    }

    public IllegalContainerStateException(int containerId, Service.State expectedState, Service.State desiredState) {
        super(String.format("Container %d is in an invalid state for this operation. Expected: %s; Actual: %s.", containerId, desiredState, expectedState));
    }
}
