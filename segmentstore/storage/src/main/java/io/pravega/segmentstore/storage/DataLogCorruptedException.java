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
 * Exception that is thrown whenever it is detected that the {@link DurableDataLog} or its metadata is corrupted.
 */
public class DataLogCorruptedException extends DurableDataLogException {
    private static final long serialVersionUID = 1L;

    public DataLogCorruptedException(String message) {
        super(message);
    }
}
