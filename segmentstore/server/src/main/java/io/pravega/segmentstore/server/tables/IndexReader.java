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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
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
     * Gets the current value of the {@link Attributes#TABLE_NODE_ID} attribute from the given {@link SegmentProperties}.
     *
     * @param segmentInfo The {@link SegmentProperties} to examine.
     * @return The value.
     * @throws IllegalStateException If the stored value is missing, negative, or exceeds {@link AttributeCalculator#MAX_NODE_ID}.
     */
    int getTableNodeId(SegmentProperties segmentInfo) {
        long value = segmentInfo.getAttributes().getOrDefault(Attributes.TABLE_NODE_ID, -1L);
        Preconditions.checkState(value >= 0 && value <= AttributeCalculator.MAX_NODE_ID,
                "Illegal TABLE_NODE_ID. Must be non-negative and at most %s; found %s", AttributeCalculator.MAX_NODE_ID, value);
        return (int) value;
    }

    /**
     * Locates the {@link TableBucket}s for the given {@link KeyHash}es in the given Segment's Extended Attribute Index.
     *
     * @param keyHashes A Collection of {@link KeyHash}es to look up {@link TableBucket}s for.
     * @param segment   A {@link DirectSegmentAccess} providing access to the Segment to look into.
     * @param timer     Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the requested Bucket information.
     */
    CompletableFuture<Map<KeyHash, TableBucket>> locateBuckets(Collection<KeyHash> keyHashes, DirectSegmentAccess segment, TimeoutTimer timer) {
        val toFetch = keyHashes.stream()
                               .map(HashBucketBuilderPair::new)
                               .collect(Collectors.toList());
        val result = new HashMap<KeyHash, TableBucket>();
        return Futures.loop(
                () -> !toFetch.isEmpty(),
                () -> fetchNextNodes(toFetch, segment, timer)
                        .thenRun(() -> {
                            val newToFetch = new ArrayList<HashBucketBuilderPair>();
                            toFetch.forEach(b -> {
                                val last = b.builder.getLastNode();
                                if (last != null && last.isIndexNode() && b.moveNext()) {
                                    newToFetch.add(b);
                                } else {
                                    result.put(b.keyHash, b.builder.build());
                                }
                            });

                            toFetch.clear();
                            toFetch.addAll(newToFetch);
                        }),
                this.executor)
                      .thenApply(v -> result);
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
     * Gets the offsets for all the Table Entries in the given {@link TableBucket}.
     *
     * @param bucket  The {@link TableBucket} to get offsets for.
     * @param segment A {@link DirectSegmentAccess}
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a List of offsets, with the first offset being the
     * {@link TableBucket}'s offset itself, then descending down in the order of backpointers. If the {@link TableBucket}
     * is partial, then the list will be empty.
     */
    @VisibleForTesting
    CompletableFuture<List<Long>> getBucketOffsets(TableBucket bucket, DirectSegmentAccess segment, TimeoutTimer timer) {
        if (bucket.isPartial()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        val result = new ArrayList<Long>();
        AtomicLong offset = new AtomicLong(getOffset(bucket.getLastNode()));
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    result.add(offset.get());
                    return getBackpointerOffset(offset.get(), segment, timer.getRemaining());
                },
                offset::set,
                this.executor)
                      .thenApply(v -> result);
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

    private CompletableFuture<Void> fetchNextNodes(Collection<HashBucketBuilderPair> builders, DirectSegmentAccess segment, TimeoutTimer timer) {
        // Fetch the node from the Segment's attributes, but we really shouldn't be caching the value, as it will only
        // slow the retrieval down (it needs to be queued up in the Segment Container's processing queue, etc.) and it
        // provides little value. For pure retrievals, the Key Hash itself will be cached alongside with its offset,
        // while for updates, this value may be changed anyway, at which point it will be cached.
        val toFetch = builders.stream().collect(Collectors.groupingBy(HashBucketBuilderPair::getCurrentKey));
        return segment
                .getAttributes(toFetch.keySet(), false, timer.getRemaining())
                .thenAcceptAsync(attributes -> builders.forEach(b -> {
                    long nodeValue = attributes.getOrDefault(b.getCurrentKey(), Attributes.NULL_ATTRIBUTE_VALUE);
                    if (nodeValue == Attributes.NULL_ATTRIBUTE_VALUE) {
                        // Found an index node that does not have any sub-branches for this hash.
                        b.builder.node(null);
                    } else {
                        // We found a sub-node, record it.
                        b.builder.node(new TableBucket.Node(
                                this.attributeCalculator.isIndexNodePointer(nodeValue),
                                b.getCurrentKey(),
                                this.attributeCalculator.extractValue(nodeValue)));
                    }
                }), this.executor);
    }

    //endregion

    @RequiredArgsConstructor
    private class HashBucketBuilderPair {
        final KeyHash keyHash;
        final Iterator<ArrayView> hashIterator;
        final TableBucket.TableBucketBuilder builder;
        final AtomicReference<UUID> currentKey;

        HashBucketBuilderPair(KeyHash keyHash) {
            this.keyHash = keyHash;
            this.hashIterator = keyHash.iterator();
            this.builder = TableBucket.builder();
            this.currentKey = new AtomicReference<>();
            moveNext();
        }

        UUID getCurrentKey() {
            return this.currentKey.get();
        }

        boolean moveNext() {
            if (!this.hashIterator.hasNext()) {
                this.currentKey.set(null);
                return false;
            }

            val lastNode = this.builder.getLastNode();
            this.currentKey.set(lastNode == null
                    ? attributeCalculator.getPrimaryHashAttributeKey(this.hashIterator.next())
                    : attributeCalculator.getSecondaryHashAttributeKey(this.hashIterator.next(), (int) lastNode.getValue()));
            return true;
        }
    }
}