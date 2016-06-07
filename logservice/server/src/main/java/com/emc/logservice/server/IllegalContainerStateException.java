package com.emc.logservice.server;

/**
 * Created by andrei on 6/6/16.
 */
public class IllegalContainerStateException extends RuntimeException { // TODO: this should extend ContainerException, but that is not a RuntimeException.
    public IllegalContainerStateException(String message) {
        super(message);
    }

    public IllegalContainerStateException(String containerId, ContainerState expectedState, ContainerState desiredState) {
        super(String.format("Container %s is in an invalid state for this operation. Expected: %s; Actual: %s.", containerId, desiredState, expectedState));
    }
}
