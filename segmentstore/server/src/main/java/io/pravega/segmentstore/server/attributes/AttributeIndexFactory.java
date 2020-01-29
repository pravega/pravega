/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.storage.Storage;

/**
 * Defines a Factory for an ContainerAttributeIndex.
 */
public interface AttributeIndexFactory {
    /**
     * Creates a new ContainerAttributeIndex for a specific Segment Container.
     *
     * @param containerMetadata The Segment Container's Metadata.
     * @param storage           The Storage to read from and write to.
     * @return A new instance of a class implementing ContainerAttributeIndex.
     */
    ContainerAttributeIndex createContainerAttributeIndex(ContainerMetadata containerMetadata, Storage storage);
}
