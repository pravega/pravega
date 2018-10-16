/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import io.pravega.segmentstore.contracts.StreamSegmentException;

/**
 * Exception that is thrown whenever an operation is conditioned on its target Table Segment to be empty but the
 * Table Segment is not.
 */
public class TableSegmentNotEmptyException extends StreamSegmentException {
    /**
     * Creates a new instance of the {@link TableSegmentNotEmptyException} class.
     *
     * @param streamSegmentName The Table Segment that is not empty.
     */
    public TableSegmentNotEmptyException(String streamSegmentName) {
        super(streamSegmentName, "Table Segment is not empty.");
    }
}
