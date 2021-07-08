/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
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
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted and
     * will contain the value that should be set as {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER} for the segment.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Long> update(Map<AttributeId, Long> values, Duration timeout);

    /**
     * Bulk-fetches a set of Attributes. This is preferred to calling get(AttributeId, Duration) repeatedly over a set of Attributes
     * as it only executes a single search for all, instead of one search for each.
     *
     * @param keys    A Collection of Attribute Ids whose values to fetch.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the desired result, but only for those Attribute Ids
     * that do have values. If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Map<AttributeId, Long>> get(Collection<AttributeId> keys, Duration timeout);

    /**
     * Compacts the Attribute Index into a final Snapshot and seals it, which means it will not accept any further changes.
     * This operation is idempotent, which means it will have no effect on an already sealed index.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the operation completed successfully.
     */
    CompletableFuture<Void> seal(Duration timeout);

    /**
     * Returns an {@link AttributeIterator} that will iterate through all Attributes between the given ranges. The
     * Attributes will be returned in ascending order, based on the {@link AttributeId#compareTo} ordering.
     *
     * @param fromId       An AttributeId representing the Attribute Id to begin the iteration at. This is an inclusive value.
     * @param toId         An AttributeId representing the Attribute Id to end the iteration at. This is an inclusive value.
     * @param fetchTimeout Timeout for every index fetch.
     * @return A new {@link AttributeIterator} that will iterate through the given Attribute range.
     */
    AttributeIterator iterator(AttributeId fromId, AttributeId toId, Duration fetchTimeout);

    /**
     * Gets the number of Attributes stored in this index.
     *
     * @return The number of Attributes stored, or -1 if index statistics are not enabled.
     */
    long getCount();
}
