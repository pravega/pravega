/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.contracts.ContainerException;

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
