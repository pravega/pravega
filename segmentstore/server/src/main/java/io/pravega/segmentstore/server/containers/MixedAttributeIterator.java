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

import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.Attributes;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * TODO javadoc, cleanup, unit tests.
 */
@RequiredArgsConstructor
public class MixedAttributeIterator implements AsyncIterator<Map<UUID, Long>> {
    @NonNull
    private final AsyncIterator<Iterator<Map.Entry<UUID, Long>>> attributeIndexIterator;
    @NonNull
    private final Iterator<Map.Entry<UUID, Long>> metadataAttributesIterator;

    @Override
    public CompletableFuture<Map<UUID, Long>> getNext() {
        return this.attributeIndexIterator.getNext()
                .thenApply(this::generateResult);
    }

    private Map<UUID, Long> generateResult(Iterator<Map.Entry<UUID, Long>> indexResult) {
        val result = new HashMap<UUID, Long>();
        if (indexResult == null) {
            // Nothing more in the Attribute Index. Add whatever is in the metadata attributes.
            this.metadataAttributesIterator.forEachRemaining(e -> include(e, result));
        } else {
            // For each element in the base result, add all those in the metadata iterator that are smaller than it, then add it.
            // Exclude those that have the metadata iterator indicates are deleted.
            while (indexResult.hasNext()) {
                val indexAttribute = indexResult.next();
                while (this.metadataAttributesIterator.hasNext()) {
                    val metadataAttribute = this.metadataAttributesIterator.next();
                    int cmp = metadataAttribute.getKey().compareTo(indexAttribute.getKey());
                    if (cmp <= 0) {
                        // The metadata attribute is smaller than the index attribute or the same: use the metadata value.
                        include(metadataAttribute, result);
                    } else {
                        // The metadata attribute is after the index attribute. Add the index attribute and this one.
                        include(indexAttribute, result);
                        include(metadataAttribute, result);
                    }

                    if (cmp >= 0) {
                        // Need to stop.
                        break;
                    }
                }
            }
        }

        if (result.isEmpty()) {
            // AsyncIterator should return null when there are no more elements to iterate over.
            assert result.isEmpty() || indexResult == null && !this.metadataAttributesIterator.hasNext() : "Empty result but more items to return";
            return null;
        }

        return result;

    }

    private void include(Map.Entry<UUID, Long> e, Map<UUID, Long> result) {
        if (!isDeleted(e)) {
            result.put(e.getKey(), e.getValue());
        }
    }

    private boolean isDeleted(Map.Entry<UUID, Long> e) {
        return e.getValue() == Attributes.NULL_ATTRIBUTE_VALUE;
    }
}
