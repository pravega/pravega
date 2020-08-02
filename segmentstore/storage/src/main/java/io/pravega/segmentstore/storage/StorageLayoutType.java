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
 * Type of Storage layout to use.
 */
public enum StorageLayoutType {
    /**
     * Uses {@link io.pravega.segmentstore.storage.rolling.RollingStorage}.
     */
    ROLLING_STORAGE,

    /**
     * Uses {@link io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage} .
     */
    CHUNKED_STORAGE,
}
