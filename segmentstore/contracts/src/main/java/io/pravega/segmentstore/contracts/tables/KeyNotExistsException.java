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

import io.pravega.common.util.ArrayView;
import lombok.Getter;

/**
 * Exception that is thrown whenever a Conditional Update to a Table failed due to a Key not being present.
 */
public class KeyNotExistsException extends ConditionalTableUpdateException {
    private static final long serialVersionUID = 1L;
    @Getter
    private final ArrayView key;

    /**
     * Creates a new instance of the KeyNotExistsException class.
     *
     * @param segmentName The name of the affected Table Segment.
     * @param key         The Key that does not exist.
     */
    public KeyNotExistsException(String segmentName, ArrayView key) {
        super(segmentName, "Key does not exist.");
        this.key = key;
    }
}
