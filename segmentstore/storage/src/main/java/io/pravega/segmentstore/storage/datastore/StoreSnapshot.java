/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.datastore;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class StoreSnapshot {
    private final long storedBytes;
    private final long usedBytes;
    private final long reservedBytes;
    private final long allocatedBytes;
    private final long maxBytes;

    @Override
    public String toString() {
        return String.format("Stored = %d, Used = %d, Reserved = %d, Allocated = %d, Max = %d",
                this.storedBytes, this.usedBytes, this.reservedBytes, this.allocatedBytes, this.maxBytes);
    }
}