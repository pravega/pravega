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

import io.pravega.common.util.AsyncIterator;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an index for a Segment's Extended Attributes.
 */
public interface AttributeIndex {
    /**
     * Atomically inserts a collection of attributes into the index (either all Attributes are inserted or none are).
     *
     * @param values  The Attributes to insert.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> put(Map<UUID, Long> values, Duration timeout);

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
     * Atomically deletes a collection of Attributes from the index (either all Attributes are deleted or none are).
     *
     * @param keys    A Collection of Attribute Ids to remove.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> remove(Collection<UUID> keys, Duration timeout);

    /**
     * Compacts the Attribute Index into a final Snapshot and seals it, which means it will not accept any further changes.
     * This operation is idempotent, which means it will have no effect on an already sealed index.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the operation completed successfully.
     */
    CompletableFuture<Void> seal(Duration timeout);

    /**
     * Returns an {@link AsyncIterator} that will iterate through all Attributes between the given ranges. The Attributes
     * will be returned in ascending order, based on the {@link UUID#compareTo} ordering.
     *
     * @param fromId       A UUID representing the Attribute Id to begin the iteration at. This is an inclusive value.
     * @param toId         A UUID representing the Attribute Id to end the iteration at. This is an inclusive value.
     * @param fetchTimeout Timeout for every index fetch.
     * @return A new {@link AsyncIterator} that will iterate through the given Attribute range.
     */
    AsyncIterator<Iterator<Map.Entry<UUID, Long>>> iterator(UUID fromId, UUID toId, Duration fetchTimeout);
}
