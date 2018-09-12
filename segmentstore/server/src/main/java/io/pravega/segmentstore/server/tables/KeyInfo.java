/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.HashedArray;
import lombok.Data;

/**
 * Represents an update to a particular Key.
 */
@Data
class KeyInfo {
    /**
     * The Key.
     */
    private final HashedArray key;

    /**
     * The offset at which the key exists in the Segment.
     */
    private final long offset;

    @Override
    public String toString() {
        return String.format("Offset=%s, Key={%s}", this.offset, key);
    }
}
