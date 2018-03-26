/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import io.pravega.segmentstore.server.AttributeIndex;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Collection of Attribute Index objects.
 */
public interface ContainerAttributeIndex {
    /**
     * Creates a new AttributeIndex instance and initializes it.
     *
     * @param streamSegmentId The Id of the Segment to create the AttributeIndex for.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the requested AttributeIndex.
     */
    CompletableFuture<AttributeIndex> forSegment(long streamSegmentId, Duration timeout);
}
