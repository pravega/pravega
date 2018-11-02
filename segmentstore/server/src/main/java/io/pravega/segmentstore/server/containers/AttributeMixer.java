/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.SegmentMetadata;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * Combined Segment Attributes from multiple sources.
 */
class AttributeMixer {
    private final ArrayDeque<Map.Entry<UUID, Long>> metadataAttributes;
    private final UUID fromId;
    private final UUID toId;
    private final AtomicReference<UUID> lastBaseAttributeId;

    /**
     * Creates a new instance of the AttributeMixer class.
     *
     * @param metadata The {@link SegmentMetadata} for the Segment.
     * @param fromId   The smallest Attribute Id to include.
     * @param toId     The largest Attribute Id to include.
     */
    AttributeMixer(@NonNull SegmentMetadata metadata, @NonNull UUID fromId, @NonNull UUID toId) {
        // Collect eligible attributes from the Metadata into a Dequeue (we need to be able to peek).
        this.metadataAttributes = metadata
                .getAttributes().entrySet().stream()
                .filter(e -> fromId.compareTo(e.getKey()) <= 0 && toId.compareTo(e.getKey()) >= 0)
                .sorted(Comparator.comparing(Map.Entry::getKey, UUID::compareTo))
                .collect(Collectors.toCollection(ArrayDeque::new));
        this.fromId = fromId;
        this.toId = toId;
        this.lastBaseAttributeId = new AtomicReference<>();
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
     * @param baseIterator An Iterator returning pairs of UUID to Long representing the base Attributes (i.e., from the
     *                     SegmentAttributeIndex. This iterator must return the pairs in their natural order based on the
     *                     {@link UUID#compareTo} comparer.
     * @return A Map of UUID to Long containing all the Attributes from the base iterator, mixed with the appropriate
     * Attributes from the {@link SegmentMetadata} passed to this class' constructor. This will return null if both
     * baseIterator is null and there are no more Attributes to process from the {@link SegmentMetadata}.
     */
    Map<UUID, Long> mix(Iterator<Map.Entry<UUID, Long>> baseIterator) {
        val result = new HashMap<UUID, Long>();
        if (baseIterator == null) {
            // Nothing more in the base iterator. Add whatever is in the metadata attributes.
            while (!this.metadataAttributes.isEmpty()) {
                include(this.metadataAttributes.removeFirst(), result);
            }

            return result.isEmpty() ? null : result;
        } else {
            // For each element in the base iterator, add all those in the metadata iterator that are smaller than it,
            // then add it. Exclude those that are deleted.
            while (baseIterator.hasNext()) {
                val baseAttribute = baseIterator.next();
                checkBaseAttribute(baseAttribute.getKey());
                include(baseAttribute, result);

                // Find all metadata attributes that are smaller or the same as the base attribute and include them all.
                // This also handles value overrides (metadata attributes, if present, always have the latest value).
                while (!this.metadataAttributes.isEmpty()
                        && this.metadataAttributes.peekFirst().getKey().compareTo(baseAttribute.getKey()) <= 0) {
                    include(this.metadataAttributes.removeFirst(), result);
                }
            }

            return result;
        }
    }

    private void include(Map.Entry<UUID, Long> e, Map<UUID, Long> result) {
        if (e.getValue() != Attributes.NULL_ATTRIBUTE_VALUE) {
            // Only add non-deletion values.
            result.put(e.getKey(), e.getValue());
        } else {
            // If we encounter a deleted value, make sure we remove it from the result.
            result.remove(e.getKey());
        }
    }

    private void checkBaseAttribute(UUID attributeId) {
        UUID prevId = this.lastBaseAttributeId.get();
        if (prevId != null) {
            Preconditions.checkArgument(prevId.compareTo(attributeId) < 0,
                    "baseIterator did not return Attributes in order. Expected at greater than {%s}, found {%s}.", prevId, attributeId);
        }

        Preconditions.checkArgument(this.fromId.compareTo(attributeId) <= 0 && this.toId.compareTo(attributeId) >= 0,
                "baseIterator returned an Attribute Id that was out of range. Expected between {%s} and {%s}, found {%s}.",
                this.fromId, this.toId, attributeId);

        this.lastBaseAttributeId.set(attributeId);
    }
}
