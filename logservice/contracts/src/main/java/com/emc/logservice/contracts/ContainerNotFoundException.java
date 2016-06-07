package com.emc.logservice.contracts;

/**
 * An exception that is thrown whenever a Container cannot be found.
 */
public class ContainerNotFoundException extends ContainerException {
    /**
     * Creates a new instance of the ContainerNotFoundException.
     *
     * @param containerId The Id of the container.
     */
    public ContainerNotFoundException(String containerId) {
        this(containerId, "Container Id does not exist.");
    }

    /**
     * Creates a new instance of the ContainerNotFoundException.
     *
     * @param containerId The Id of the container.
     * @param message     The message for the exception.
     */
    public ContainerNotFoundException(String containerId, String message) {
        super(containerId, message);
    }
}
