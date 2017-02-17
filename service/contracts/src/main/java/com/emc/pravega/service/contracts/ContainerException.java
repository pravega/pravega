/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * An exception that is related to a particular Container.
 */
abstract class ContainerException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final int containerId;

    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId The Id of the ContainerException.
     * @param message     The message for this exception.
     */
    ContainerException(int containerId, String message) {
        this(containerId, message, null);
    }

    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId The Id of the Container.
     * @param message     The message for this exception.
     * @param cause       The causing exception.
     */
    private ContainerException(int containerId, String message, Throwable cause) {
        super(String.format("%s (%d).", message, containerId), cause);
        this.containerId = containerId;
    }

    /**
     * Gets a value indicating the Container Id.
     */
    public int getContainerId() {
        return this.containerId;
    }
}
