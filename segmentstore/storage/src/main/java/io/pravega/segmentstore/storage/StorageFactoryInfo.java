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

import lombok.Builder;
import lombok.Data;

/**
 * Information about the capabilities supported by a {@link StorageFactory}.
 */
@Data
@Builder
public class StorageFactoryInfo {
    /**
     * Name of storage binding.
     */
    private final String name;

    /**
     * Type of storage layout supported.
     */
    private final StorageLayoutType storageLayoutType;

}
