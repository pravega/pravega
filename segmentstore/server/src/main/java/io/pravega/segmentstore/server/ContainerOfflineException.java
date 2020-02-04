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

/**
 * Exception thrown whenever a Container is offline.
 */
public class ContainerOfflineException extends IllegalContainerStateException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates an new instance of the ContainerOfflineException class.
     *
     * @param containerId The Id of the SegmentContainer that is offline.
     */
    public ContainerOfflineException(int containerId) {
        super(String.format("Container %d is offline and cannot execute any operation.", containerId));
    }
}
