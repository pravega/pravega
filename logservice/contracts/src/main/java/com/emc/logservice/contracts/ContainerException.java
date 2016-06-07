package com.emc.logservice.contracts;

/**
 * An exception that is related to a particular Container.
 */
public abstract class ContainerException extends StreamingException {
    private final String containerId;

    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId The Id of the ContainerException.
     * @param message     The message for this exception.
     */
    public ContainerException(String containerId, String message) {
        this(containerId, message, null);
    }

    /**
     * Creates a new instance of the ContainerException class.
     *
     * @param containerId The Id of the Container.
     * @param message     The message for this exception.
     * @param cause       The causing exception.
     */
    public ContainerException(String containerId, String message, Throwable cause) {
        super(String.format("%s (%s).", message, containerId), cause);
        this.containerId = containerId;
    }

    /**
     * Gets a value indicating the Container Id.
     *
     * @return
     */
    public String getContainerId() {
        return this.containerId;
    }
}
