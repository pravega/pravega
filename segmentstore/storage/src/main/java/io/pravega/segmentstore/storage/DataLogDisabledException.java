/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

/**
 * Exception that is thrown whenever the DurableDataLog cannot be initialized because it is disabled.
 */
public class DataLogDisabledException extends DataLogInitializationException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataLogDisabledException class.
     *
     * @param message Message for the exception.
     */
    public DataLogDisabledException(String message) {
        super(message);
    }
}
