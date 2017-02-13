/**
 *  Copyright (c) 2017 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.controller.store.host;

/**
 * This exception is thrown on errors from the HostControllerStore implementation.
 */
public class HostStoreException extends RuntimeException {

    /**
     * Create a HostStoreException using a text cause.
     *
     * @param message   The cause of the exception.
     */
    public HostStoreException(String message) {
        super(message);
    }

    /**
     * Create a HostStoreException using a text cause.
     *
     * @param message   The cause of the exception.
     * @param cause     Any existing exception that needs to be wrapped.
     */
    public HostStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
