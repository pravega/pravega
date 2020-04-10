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

import java.io.Serializable;

/**
 * Storage Metadata.
 * All storage related metadata is stored in {@link ChunkMetadataStore} using set of key-value pairs.
 * The String value returned by {@link StorageMetadata#getKey()} is used as key.
 * Notable derived classes are {@link SegmentMetadata} and {@link ChunkMetadata} which form the core of metadata related
 * to {@link io.pravega.segmentstore.storage.chunklayer.ChunkStorageManager} functionality.
 */
public interface StorageMetadata extends Serializable {

    /**
     * Retrieves the key associated with the metadata.
     * @return key.
     */
    String getKey();

    /**
     * Creates a deep copy of this instance.
     * @return
     */
    StorageMetadata deepCopy();
}
