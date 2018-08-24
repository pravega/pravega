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
 * Exception that is thrown whenever a Conditional Update to a Table (based on versions) failed.
 */
public class ConditionalTableUpdateException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the ConditionalTableUpdateException class.
     *
     * @param segmentName     The name of the affected Table Segment.
     * @param expectedVersion The expected version for the update. A null value indicates that no previous version exists.
     * @param actualVersion   The version that was passed in via the update.
     */
    public ConditionalTableUpdateException(String segmentName, Long expectedVersion, long actualVersion) {
        super(segmentName, getMessage(expectedVersion, actualVersion));
    }

    private static String getMessage(Long expectedVersion, long actualVersion) {
        return String.format("Conditional update failed. Expected Version: %s, Actual Version: %s.",
                expectedVersion == null ? "NONE" : Long.toString(expectedVersion),
                actualVersion);
    }
}
