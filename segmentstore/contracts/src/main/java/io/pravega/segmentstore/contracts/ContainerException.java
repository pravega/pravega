/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

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
     * @return int that is the id of the container
     */
    public int getContainerId() {
        return this.containerId;
    }
}
