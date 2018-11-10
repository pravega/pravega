/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an index for a Segment's Extended Attributes.
 */
public interface AttributeIndex {
    /**
     * Performs an atomic update of a collection of Attributes. The updates can be a mix of insertions, updates or removals.
     * An Attribute is:
     * - Inserted when its ID is not already present.
     * - Updated when its ID is present and the associated value is non-null.
     * - Removed when its ID is present and the associated value is null.
     *
     * @param values  The Attributes to insert.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> update(Map<UUID, Long> values, Duration timeout);

    /**
     * Bulk-fetches a set of Attributes. This is preferred to calling get(UUID, Duration) repeatedly over a set of Attributes
     * as it only executes a single search for all, instead of one search for each.
     *
     * @param keys    A Collection of Attribute Ids whose values to fetch.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the desired result, but only for those Attribute Ids
     * that do have values. If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Map<UUID, Long>> get(Collection<UUID> keys, Duration timeout);

    /**
     * Compacts the Attribute Index into a final Snapshot and seals it, which means it will not accept any further changes.
     * This operation is idempotent, which means it will have no effect on an already sealed index.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the operation completed successfully.
     */
    CompletableFuture<Void> seal(Duration timeout);
}
