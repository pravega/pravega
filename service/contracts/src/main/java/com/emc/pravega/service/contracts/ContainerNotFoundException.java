/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * An exception that is thrown whenever a Container cannot be found.
 */
public class ContainerNotFoundException extends ContainerException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the ContainerNotFoundException.
     *
     * @param containerId The Id of the container.
     */
    public ContainerNotFoundException(int containerId) {
        super(containerId, "Container Id does not exist.");
    }
}
