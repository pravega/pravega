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
 */
public interface StorageMetadata extends Serializable {

    /**
     * Retrieves the key associated with the metadata.
     * @return key.
     */
    String getKey();

    StorageMetadata copy();
}
