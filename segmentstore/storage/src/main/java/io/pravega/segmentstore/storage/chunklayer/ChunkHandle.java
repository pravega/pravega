/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Data;
import lombok.NonNull;

/**
 * Handle to a chunk.
 */
@Data
final public class ChunkHandle {
    /**
     * Name of the chunk.
     */
    @NonNull
    private final String chunkName;

    /**
     * Whether the segment is read only or not.
     */
    private final boolean isReadOnly;

    /**
     * Creates a read only handle for a given chunk name.
     *
     * @param chunkName Name of the chunk.
     * @return A readonly handle.
     */
    public static ChunkHandle readHandle(String chunkName) {
        return new ChunkHandle(chunkName, true);
    }

    /**
     * Creates a writable/updatable handle for a given chunk name.
     *
     * @param chunkName Name of the chunk.
     * @return A writable/updatable handle.
     */
    public static ChunkHandle writeHandle(String chunkName) {
        return new ChunkHandle(chunkName, false);
    }
}
