/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
