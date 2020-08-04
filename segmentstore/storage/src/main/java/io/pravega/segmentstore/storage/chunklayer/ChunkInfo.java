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

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Chunk Information.
 */
@Builder
@Data
final public class ChunkInfo {
    /**
     * Length of the chunk.
     */
    private final long length;

    /**
     * Name of the chunk.
     */
    @NonNull
    private final String name;
}
