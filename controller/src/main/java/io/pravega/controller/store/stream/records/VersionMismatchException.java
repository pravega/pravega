/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream.records;

import io.pravega.common.io.SerializationException;

public class VersionMismatchException extends SerializationException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Version mismatch during deserialization %s.";

    /**
     * Creates a new instance of VersionMismatchException class.
     *
     * @param className resource on which lock failed
     */
    public VersionMismatchException(final String className) {
        super(String.format(FORMAT_STRING, className));
    }
}
