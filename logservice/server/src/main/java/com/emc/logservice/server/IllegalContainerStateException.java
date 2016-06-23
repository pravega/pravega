package com.emc.logservice.server;

import com.google.common.util.concurrent.Service;

/**
 * Exception thrown whenever a Container is in an invalid State
 */
public class IllegalContainerStateException extends RuntimeException { // TODO: this should extend ContainerException, but that is not a RuntimeException.
    public IllegalContainerStateException(String message) {
        super(message);
    }

    public IllegalContainerStateException(String containerId, Service.State expectedState, Service.State desiredState) {
        super(String.format("Container %s is in an invalid state for this operation. Expected: %s; Actual: %s.", containerId, desiredState, expectedState));
    }
}
