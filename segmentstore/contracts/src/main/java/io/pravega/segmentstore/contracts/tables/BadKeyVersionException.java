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

import io.pravega.common.util.ArrayView;
import lombok.Getter;

/**
 * Exception that is thrown whenever a Conditional Update to a Table failed due to an existing Key that has a different
 * version than provided.
 */
public class BadKeyVersionException extends ConditionalTableUpdateException {
    private static final long serialVersionUID = 1L;
    @Getter
    private final long expectedVersion;
    @Getter
    private final long compareVersion;

    /**
     * Creates a new instance of the BadKeyVersionException class.
     *
     * @param segmentName     The name of the affected Table Segment.
     * @param key             The Key that does not exist.
     * @param expectedVersion The version of the Key in the Table.
     * @param compareVersion  The version of the Key provided by the request.
     */
    public BadKeyVersionException(String segmentName, ArrayView key, long expectedVersion, long compareVersion) {
        super(segmentName, key, String.format("Version mismatch (expected %s, given %s).", expectedVersion, compareVersion));
        this.expectedVersion = expectedVersion;
        this.compareVersion = compareVersion;
    }
}
