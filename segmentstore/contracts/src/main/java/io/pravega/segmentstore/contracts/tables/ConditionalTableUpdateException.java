/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
 * Exception that is thrown whenever a Conditional Update to a Table (based on versions) failed.
 */
public abstract class ConditionalTableUpdateException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the ConditionalTableUpdateException class.
     *
     * @param segmentName The name of the affected Table Segment.
     */
    ConditionalTableUpdateException(String segmentName, String message) {
        super(segmentName, message);
    }
}
