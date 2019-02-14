/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.StreamSegmentException;

/**
 * Indicates that there is a possibility of data corruption.
 *
 */
public class DataCorruptionException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is no longer primary.
     */
    public DataCorruptionException(String streamSegmentName) {
        this(streamSegmentName, null, null);
    }

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param streamSegmentName The name of the segment for which there is a possibility of data corruption.
     * @param cause             The causing exception.
     */
    public DataCorruptionException(String streamSegmentName, Throwable cause) {
        this(streamSegmentName, null, cause);
    }

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param streamSegmentName The name of the segment for which there is a possibility of data corruption.
     * @param message           The exception message.
     */
    public DataCorruptionException(String streamSegmentName, String message) {
        this(streamSegmentName, message, null);
    }

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param streamSegmentName The name of the segment for which there is a possibility of data corruption.
     * @param cause             The causing exception.
     * @param message           The exception message.
     */

    public DataCorruptionException(String streamSegmentName, String message, Throwable cause) {
        super(streamSegmentName, "The non primary writer detected for this StreamSegment. There is a possibility of data corruption." + (message == null ? "" : " " + message),
                cause);
    }
}

