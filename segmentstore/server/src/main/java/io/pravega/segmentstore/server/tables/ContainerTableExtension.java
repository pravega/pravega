/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a {@link SegmentContainerExtension} that implements Tables on top of Segment Containers.
 */
public interface ContainerTableExtension extends TableStore, SegmentContainerExtension {
    /**
     * Caches the unindexed Tail Section of the given Table Segment. This scans the tail portion of the segment (beyond
     * {@link TableAttributes#INDEX_OFFSET}) and updates any internal memory data structures to reflect the updates
     * recorded in it. This method can be used to speed up recovery of certain Table Segments and should be used sparingly
     * as it may consume significant CPU resources while processing.
     *
     * @param segmentName The name of the Segment to process..
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate whether the operation succeeded or not.
     */
    CompletableFuture<Void> cacheTailIndex(String segmentName, Duration timeout);
}