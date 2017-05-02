/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.contracts;

/**
 * An exception that is related to a particular Container.
 */
public abstract class ContainerException extends StreamingException {
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
    public ContainerException(int containerId, String message) {
        this(containerId, message, null);
    }

    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId The Id of the Container.
     * @param message     The message for this exception.
     * @param cause       The causing exception.
     */
    public ContainerException(int containerId, String message, Throwable cause) {
        super(String.format("[Container %d] %s.", containerId, message), cause);
        this.containerId = containerId;
    }

    /**
     * Gets a value indicating the Container Id.
     */
    public int getContainerId() {
        return this.containerId;
    }
}
