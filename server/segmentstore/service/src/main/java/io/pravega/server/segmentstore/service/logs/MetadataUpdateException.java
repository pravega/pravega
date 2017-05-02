/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.service.logs;

import io.pravega.server.segmentstore.contracts.ContainerException;

/**
 * Exception that is thrown whenever the Metadata cannot be updated.
 */
public class MetadataUpdateException extends ContainerException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    MetadataUpdateException(int containerId, String message) {
        super(containerId, message);
    }

    MetadataUpdateException(int containerId, String message, Throwable cause) {
        super(containerId, message, cause);
    }
}
