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
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.SegmentMetadata;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.val;

/**
 * Implements a {@link AttributeIterator} for a Segment, using both the Segment Attribute Index and Segment Metadata
 * as sources.
 *
 * While the Segment Attribute Index is the final storage for all the Segment's Extended Attributes, the Segment Metadata
 * also contains the Core Attributes and those Extended Attributes that have recently been changed. As such, if an
 * Attribute exists in the Segment Metadata, it's value is the most recent and must be used (eventually that value will
 * trickle down to the Attribute Index).
 *
 * Instances of this class iterate over the Attribute Index (within the specified bounds), and also include Attributes
 * from the Segment Metadata where appropriate.
 */
@ThreadSafe
class SegmentAttributeIterator implements AttributeIterator {
    //region Members

    private final AttributeIterator indexIterator;
    @GuardedBy("metadataAttributes")
    private final ArrayDeque<Map.Entry<AttributeId, Long>> metadataAttributes;
    private final AttributeId fromId;
    private final AttributeId toId;
    private final AtomicReference<AttributeId> lastIndexAttribute;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentAttributeIterator class.
     *
     * @param metadata The {@link SegmentMetadata} for the Segment.
     * @param fromId   The smallest Attribute Id to include.
     * @param toId     The largest Attribute Id to include.
     */
    SegmentAttributeIterator(@NonNull AttributeIterator indexIterator, @NonNull SegmentMetadata metadata, @NonNull AttributeId fromId, @NonNull AttributeId toId) {
        this.indexIterator = indexIterator;

        // Collect eligible attributes from the Metadata into a Dequeue (we need to be able to peek).
        // We need to use SegmentMetadata.getAttributes(BiPredicate) since that will perform the filtering while holding
        // the SegmentMetadata's lock, thus ensuring a correct iteration. SegmentMetadata.getAttributes() provides a simple
        // read-only view that cannot be used for safe iteration in this case.
        this.metadataAttributes = metadata
                .getAttributes((key, value) -> !Attributes.isCoreAttribute(key) && fromId.compareTo(key) <= 0 && toId.compareTo(key) >= 0)
                .entrySet().stream()
                .sorted(Map.Entry.comparingByKey(AttributeId::compareTo))
                .collect(Collectors.toCollection(ArrayDeque::new));
        this.fromId = fromId;
        this.toId = toId;
        this.lastIndexAttribute = new AtomicReference<>();
    }

    //endregion

    //region AttributeIterator Implementation

    @Override
    public CompletableFuture<List<Map.Entry<AttributeId, Long>>> getNext() {
        return this.indexIterator
                .getNext()
                .thenApply(this::mix);
    }

    /**
     * Mixes the Attributes from the given iterator with the ones from the {@link SegmentMetadata} passed to this class'
     * constructor.
     *
     * The mixing algorithm works along these lines:
     * - Each entry (Attribute) in the given Iterator is considered.
     * - All non-deleted Attributes from the {@link SegmentMetadata} that are smaller than the one from the current entry
     * will be added.
     * - The entry will be added only if there is no corresponding updated value for its Attribute Id in the {@link SegmentMetadata},
     * If there is, then the updated value is used.
     *
     * @param indexAttributes A List containing pairs of AttributeId to Long representing the base Attributes (i.e., from the
     *                        SegmentAttributeIndex. This iterator must return the pairs in their natural order based on
     *                        the {@link AttributeId#compareTo} comparer.
     * @return A List of Map Entries (AttributeId to Long) containing all the Attributes from the index, mixed with the appropriate
     * Attributes from the {@link SegmentMetadata} passed to this class' constructor. This will return null if both
     * indexAttributes is null and there are no more Attributes to process from the {@link SegmentMetadata}.
     */
    private List<Map.Entry<AttributeId, Long>> mix(List<Map.Entry<AttributeId, Long>> indexAttributes) {
        val result = new ArrayList<Map.Entry<AttributeId, Long>>();
        if (indexAttributes == null) {
            // Nothing more in the base iterator. Add whatever is in the metadata attributes.
            synchronized (this.metadataAttributes) {
                while (!this.metadataAttributes.isEmpty()) {
                    include(this.metadataAttributes.removeFirst(), result);
                }
            }

            return result.isEmpty() ? null : result;
        } else {
            // For each element in the index, add all those in the metadata iterator that are smaller than it, then add it.
            // Exclude those that are deleted.
            for (val idxAttribute : indexAttributes) {
                checkIndexAttribute(idxAttribute.getKey());

                // Find all metadata attributes that are smaller or the same as the base attribute and include them all.
                // This also handles value overrides (metadata attributes, if present, always have the latest value).
                AttributeId lastMetadataAttribute = null;
                synchronized (this.metadataAttributes) {
                    while (!this.metadataAttributes.isEmpty()
                            && this.metadataAttributes.peekFirst().getKey().compareTo(idxAttribute.getKey()) <= 0) {
                        lastMetadataAttribute = include(this.metadataAttributes.removeFirst(), result);
                    }
                }

                // Only add our element if it hasn't already been processed.
                if (lastMetadataAttribute == null || !lastMetadataAttribute.equals(idxAttribute.getKey())) {
                    include(idxAttribute, result);
                }
            }

            return result;
        }
    }

    private AttributeId include(Map.Entry<AttributeId, Long> e, List<Map.Entry<AttributeId, Long>> result) {
        if (e.getValue() != Attributes.NULL_ATTRIBUTE_VALUE) {
            // Only add non-deletion values.
            result.add(e);
        }

        return e.getKey();
    }

    private void checkIndexAttribute(AttributeId attributeId) {
        AttributeId prevId = this.lastIndexAttribute.get();
        if (prevId != null) {
            Preconditions.checkArgument(prevId.compareTo(attributeId) < 0,
                    "baseIterator did not return Attributes in order. Expected at greater than {%s}, found {%s}.", prevId, attributeId);
        }

        Preconditions.checkArgument(this.fromId.compareTo(attributeId) <= 0 && this.toId.compareTo(attributeId) >= 0,
                "baseIterator returned an Attribute Id that was out of range. Expected between {%s} and {%s}, found {%s}.",
                this.fromId, this.toId, attributeId);

        this.lastIndexAttribute.set(attributeId);
    }

    //endregion
}
