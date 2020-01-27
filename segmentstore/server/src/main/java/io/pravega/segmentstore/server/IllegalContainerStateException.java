/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import com.google.common.util.concurrent.Service;

/**
 * Exception thrown whenever a Container is in an invalid State.
 */
public class IllegalContainerStateException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public IllegalContainerStateException(String message) {
        super(message);
    }

    public IllegalContainerStateException(int containerId, Service.State expectedState, Service.State desiredState) {
        super(String.format("Container %d is in an invalid state for this operation. Expected: %s; Actual: %s.", containerId, desiredState, expectedState));
    }
}
