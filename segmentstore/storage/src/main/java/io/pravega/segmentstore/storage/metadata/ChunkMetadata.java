/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import lombok.Builder;
import lombok.Data;

/**
 * Represents chunk metadata.
 * Following metadata is stored.
 * <ul>
 *     <li>Name of the chunk.</li>
 *     <li>Length of the chunk.</li>
 *     <li>Name of the next chunk in list.</li>
 * </ul>
 */
@Builder(toBuilder = true)
@Data
public class ChunkMetadata implements StorageMetadata {
    /**
     * Name of this chunk.
     */
    private final String name;

    /**
     * Length of the chunk.
     */
    private long length;

    /**
     * Name of the next chunk.
     */
    private String nextChunk;

    /**
     * Retrieves the key associated with the metadata, which is the name of the chunk.
     * @return Name of the chunk.
     */
    @Override
    public String getKey() {
        return name;
    }

    /**
     * Creates a deep copy of this instance.
     * @return
     */
    @Override
    public StorageMetadata deepCopy() {
        return toBuilder().build();
    }
}
