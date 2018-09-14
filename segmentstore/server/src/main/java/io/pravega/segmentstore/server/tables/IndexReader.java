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

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;
import lombok.val;

/**
 * Provides read-only access to a Hash Array Mapped Tree implementation over Extended Attributes.
 */
class IndexReader {
    //region Members

    protected final ScheduledExecutorService executor;
    protected final AttributeCalculator attributeCalculator;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the IndexReader class.
     *
     * @param executor An Executor to use for async tasks.
     */
    IndexReader(@NonNull ScheduledExecutorService executor) {
        this.executor = executor;
        this.attributeCalculator = new AttributeCalculator();
    }

    //endregion

    //region Index Access Operations

    /**
     * Gets the offset (from the given {@link SegmentProperties}'s Attributes up to which all Table Entries have been indexed.
     *
     * @param segmentInfo A {@link SegmentProperties} from which to extract the information.
     * @return The offset.
     */
    long getLastIndexedOffset(SegmentProperties segmentInfo) {
        return segmentInfo.getAttributes().getOrDefault(Attributes.TABLE_INDEX_OFFSET, 0L);
    }

    /**
     * Locates a Bucket in the given Segment's Extended Attribute Index.
     *
     * @param keyHash The the Hash pertaining to the lookup key.
     * @param segment A DirectSegmentAccess providing access to the Segment to look into.
     * @param timer   Timer for the operation.
     * @return A Future that, when completed, will contain the requested Bucket information.
     */
    CompletableFuture<TableBucket> locateBucket(KeyHash keyHash, DirectSegmentAccess segment, TimeoutTimer timer) {
        Iterator<ArrayView> hashIterator = keyHash.iterator();
        Preconditions.checkArgument(hashIterator.hasNext(), "No hashes given");

        // Fetch Primary Hash, then fetch Secondary Hashes EAs until: 1) we reach a data node OR 2) the EA entry is missing.
        UUID phKey = this.attributeCalculator.getPrimaryHashAttributeKey(hashIterator.next());
        val builder = TableBucket.builder();
        return fetchNode(phKey, builder, segment, timer)
                .thenCompose(v -> Futures.loop(
                        () -> canContinueLookup(builder.getLastNode(), hashIterator),
                        () -> {
                            // Calculate EA Key for this secondary hash.
                            UUID shKey = this.attributeCalculator.getSecondaryHashAttributeKey(
                                    hashIterator.next(), (int) builder.getLastNode().getValue());

                            // Fetch the value of the EA Key.
                            return fetchNode(shKey, builder, segment, timer);
                        },
                        this.executor))
                .thenApply(v -> builder.build());
    }

    /**
     * Looks up a Backpointer offset.
     *
     * @param offset  The offset to find a backpointer from.
     * @param segment A DirectSegmentAccess providing access to the Segment to search in.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the backpointer offset, or -1 if no such pointer exists.
     */
    CompletableFuture<Long> getBackpointerOffset(long offset, DirectSegmentAccess segment, Duration timeout) {
        UUID key = this.attributeCalculator.getBackpointerAttributeKey(offset);
        return segment.getAttributes(Collections.singleton(key), false, timeout)
                      .thenApply(attributes -> {
                          long result = attributes.getOrDefault(key, Attributes.NULL_ATTRIBUTE_VALUE);
                          return result == Attributes.NULL_ATTRIBUTE_VALUE ? -1 : result;
                      });
    }

    /**
     * Extracts the SegmentOffset from the given non-index Node.
     *
     * @param node The Node to extract from.
     * @return The SegmentOffset.
     */
    long getOffset(TableBucket.Node node) {
        Preconditions.checkArgument(!node.isIndexNode(), "Cannot extract offset from an Index Node");
        return this.attributeCalculator.extractValue(node.getValue());
    }

    private CompletableFuture<Void> fetchNode(UUID key, TableBucket.TableBucketBuilder builder, DirectSegmentAccess segment, TimeoutTimer timer) {
        // Fetch the node from the Segment's attributes, but we really shouldn't be caching the value, as it will only
        // slow the retrieval down (it needs to be queued up in the Segment Container's processing queue, etc.) and it
        // provides little value. For pure retrievals, the Key Hash itself will be cached alongside with its offset,
        // while for updates, this value may be changed anyway, at which point it will be cached.
        return segment
                .getAttributes(Collections.singleton(key), false, timer.getRemaining())
                .thenAcceptAsync(attributes -> {
                    long nodeValue = attributes.getOrDefault(key, Attributes.NULL_ATTRIBUTE_VALUE);
                    if (nodeValue == Attributes.NULL_ATTRIBUTE_VALUE) {
                        // Found an index node that does not have any sub-branches for this hash.
                        builder.node(null);
                    } else {
                        // We found a sub-node, record it.
                        builder.node(new TableBucket.Node(
                                this.attributeCalculator.isIndexNodePointer(nodeValue),
                                key,
                                this.attributeCalculator.extractValue(nodeValue)));
                    }
                }, this.executor);
    }

    private boolean canContinueLookup(TableBucket.Node lastNode, Iterator<?> hashIterator) {
        return lastNode != null && lastNode.isIndexNode() && hashIterator.hasNext();
    }

    //endregion
}