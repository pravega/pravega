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
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     * @return The StreamSegment Id.
     */
    long getId();

    /**
     * Gets a value indicating the id of the Container this StreamSegment belongs to.
     * @return The Container Id.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether this StreamSegment has been merged into another.
     * @return true if StreamSegment is merged, false otherwise.
     */
    boolean isMerged();

    /**
     * Gets a value indicating whether this StreamSegment has been sealed in Storage.
     * This is different from isSealed(), which returns true if the StreamSegment has been sealed in the Metadata or in Storage.
     * @return true if this StreamSegment has been sealed in Storage, false otherwise.
     */
    boolean isSealedInStorage();

    /**
     * Gets a value indicating whether this StreamSegment has been deleted in Storage.
     * This is different from isDeleted(), which returns true if the StreamSegment has been deleted in the Metadata or in Storage.
     * @return true if this StreamSegment has been deleted in Storage, false otherwise.
     */
    boolean isDeletedInStorage();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     * @return The length of the StreamSegment.
     */
    long getStorageLength();

    /**
     * Gets a value representing the when the Segment was last used. The meaning of this value is implementation specific,
     * however higher values should indicate it was used more recently.
     * @return The value representing when the Segment last used.
     */
    long getLastUsed();

    /**
     * Gets a value indicating whether this instance of the SegmentMetadata is still active. As long as the Segment is
     * registered in the ContainerMetadata, this will return true. If this returns false, it means this SegmentMetadata
     * instance has been evicted from the ContainerMetadata (for whatever reasons) and should be considered stale.
     *
     * @return Whether this instance of the SegmentMetadata is active or not.
     */
    boolean isActive();

    /**
     * Creates a new SegmentProperties instance with current information from this SegmentMetadata object.
     *
     * @return The new SegmentProperties instance. This object is completely detached from the SegmentMetadata from which
     * it was created (changes to the base object will not be reflected in the result). The returned object will have a copy
     * of this instance's Attributes which would make it safe for iterating over without holding any locks.
     */
    SegmentProperties getSnapshot();

    /**
     * Gets a value indicating whether this {@link SegmentMetadata} instance is pinned to memory. If pinned, this metadata
     * will never be evicted by the owning metadata (even if there is eviction pressure and this Segment meets all other
     * eviction criteria).
     *
     * Notes:
     * - This will still be cleared out of the metadata if {@link UpdateableContainerMetadata#reset()} is invoked.
     * - This has no bearing on the eviction of the Segment's Extended Attributes.
     *
     * @return True if pinned, false otherwise.
     */
    boolean isPinned();

    /**
     * Gets a read-only Map of AttributeId-Values for this Segment. The returned object is a View combining both Core
     * and Extended attributes and provides direct read-only access to all this Segment's Attributes.
     *
     * Notes on concurrency:
     * - All retrieval methods are thread-safe and can be invoked from any context.
     * - Iterating over this Map's elements ({@link Map#keySet()}, {@link Map#values()}, {@link Map#entrySet()}) is not
     * thread safe and should only be done in an (external) synchronized context (while holding the same lock that is
     * used to update this {@link SegmentMetadata} instance).
     * - Consider using {@link #getSnapshot()} if you need a thread-safe way of iterating over the current set of Attributes.
     * - Consider using {@link #getAttributes(BiPredicate)} if you need to get a subset of the attributes and can provide
     * a filter.
     *
     * @return The map.
     */
    @Override
    Map<AttributeId, Long> getAttributes();

    /**
     * Gets new Map containing all the Attributes for this Segment that match the given filter.
     *
     * @param filter The Key-Value filter that will be used.
     * @return A new Map containing the result. This is a new object, detached from this {@link SegmentMetadata} instance
     * and can be safely accessed and iterated over from any thread.
     */
    Map<AttributeId, Long> getAttributes(BiPredicate<AttributeId, Long> filter);

    /**
     * Gets the type of this Segment, which was set at the time of Segment creation. This value cannot be modified
     * afterwards.
     *
     * @return A {@link SegmentType} representing the type of the Segment.
     */
    SegmentType getType();

    /**
     * The length of all Extended Attributes for this Segment, as defined by the
     * {@link io.pravega.segmentstore.contracts.Attributes#ATTRIBUTE_ID_LENGTH} attribute. Possible values:
     * * -1 or 0: This Segment's Extended Attributes' Ids are of type {@link AttributeId.UUID}.
     * * Larger than 0: This Segment's Extended Attributes' Ids are of type {@link AttributeId.Variable} and should all have
     * this length.
     *
     * @return The length of all Extended Attributes for this Segment.
     */
    int getAttributeIdLength();
}
