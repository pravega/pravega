/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.storage;

/**
 * Exception that is thrown whenever the Durable Data Log (or one its components) is unavailable.
 * This generally indicates a temporary unavailability, which can go away if the operation is retried.
 */
public class DataLogNotAvailableException extends DurableDataLogException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     */
    public DataLogNotAvailableException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     * @param cause
     */
    public DataLogNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
