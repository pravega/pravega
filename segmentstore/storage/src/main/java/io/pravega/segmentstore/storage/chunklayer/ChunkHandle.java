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

import com.google.common.base.Preconditions;

/**
 * Handle to a chunk.
 */
public class ChunkHandle {
    private final String chunkName;
    private final boolean isReadOnly;

    /**
     * Creates a new instance of ChunkHandle.
     * @param chunkName Name of the segment.
     * @param isReadOnly  Whether the segment is read only or not.
     */
    public ChunkHandle(String chunkName, boolean isReadOnly) {
        this.chunkName = Preconditions.checkNotNull(chunkName, "chunkName");
        this.isReadOnly = isReadOnly;
    }

    /**
     * Gets name of the chunk.
     * @return Name of the chunk.
     */
    public String getChunkName() {
        return chunkName;
    }

    /**
     * Returns whether the handle is read only.
     * @return True if the handle is read only.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Creates a read only handle for a given chunk name.
     * @param chunkName Name of the chunk.
     * @return A readonly handle.
     */
    public static ChunkHandle readHandle(String chunkName) {
        return new ChunkHandle(chunkName, true);
    }

    /**
     * Creates a writable/updatable handle for a given chunk name.
     * @param chunkName Name of the chunk.
     * @return A writable/updatable handle.
     */
    public static ChunkHandle writeHandle(String chunkName) {
        return new ChunkHandle(chunkName, false);
    }
}
