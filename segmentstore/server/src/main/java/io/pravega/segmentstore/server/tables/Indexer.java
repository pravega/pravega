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
import io.pravega.segmentstore.contracts.AttributeReference;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateByReference;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import lombok.NonNull;
import lombok.val;

/**
 * Implements a Hash Array Mapped Trie Index using Extended Segment Attributes.
 */
class Indexer {
    //region Members

    private final ScheduledExecutorService executor;
    private final AttributeCalculator attributeCalculator;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Indexer class.
     *
     * @param executor An Executor to use for async tasks.
     */
    Indexer(@NonNull ScheduledExecutorService executor) {
        this.executor = executor;
        this.attributeCalculator = new AttributeCalculator();
    }

    //endregion

    //region General Table Attributes

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
     * Generates a conditional {@link AttributeUpdate} that sets a new value for the {@link Attributes#TABLE_INDEX_OFFSET}
     * attribute.
     *
     * @param currentOffset The current offset. This will be used for the conditional update.
     * @param newOffset     The new offset to set.
     * @return The generated {@link AttributeUpdate}.
     */
    AttributeUpdate generateUpdateLastIndexedOffset(long currentOffset, long newOffset) {
        Preconditions.checkArgument(currentOffset <= newOffset, "newOffset must be larger than existingOffset");
        return new AttributeUpdate(Attributes.TABLE_INDEX_OFFSET, AttributeUpdateType.ReplaceIfEquals, newOffset, currentOffset);
    }

    /**
     * Generates a set of {@link AttributeUpdate}s that set the initial Attributes on a newly create Table Segment.
     *
     * Attributes:
     * * {@link Attributes#TABLE_NODE_ID} is initialized to 1.
     * * {@link Attributes#TABLE_INDEX_OFFSET} is initialized to 0.
     *
     * @return A Collection of {@link AttributeUpdate}s.
     */
    Collection<AttributeUpdate> generateInitialTableAttributes() {
        return Arrays.asList(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.None, 1L),
                new AttributeUpdate(Attributes.TABLE_INDEX_OFFSET, AttributeUpdateType.None, 0L));
    }

    //endregion

    //region Bucket Lookups

    /**
     * Locates a Bucket in the given Segment's Extended Attribute Index.
     *
     * @param segment A DirectSegmentAccess providing access to the Segment to look into.
     * @param keyHash The the Hash pertaining to the lookup key.
     * @param timer   Timer for the operation.
     * @return A Future that, when completed, will contain the requested Bucket information.
     */
    CompletableFuture<TableBucket> locateBucket(DirectSegmentAccess segment, KeyHash keyHash, TimeoutTimer timer) {
        Iterator<ArrayView> hashIterator = keyHash.iterator();
        Preconditions.checkArgument(hashIterator.hasNext(), "No hashes given");

        // Fetch Primary Hash, then fetch Secondary Hashes EAs until: 1) we reach a data node OR 2) the EA entry is missing.
        UUID phKey = this.attributeCalculator.getPrimaryHashAttributeKey(hashIterator.next());
        val builder = TableBucket.builder();
        return fetchNode(segment, phKey, builder, timer)
                .thenCompose(v -> Futures.loop(
                        () -> canContinueLookup(builder.getLastNode(), hashIterator),
                        () -> {
                            // Calculate EA Key for this secondary hash.
                            UUID shKey = this.attributeCalculator.getSecondaryHashAttributeKey(
                                    hashIterator.next(), (int) builder.getLastNode().getValue());

                            // Fetch the value of the EA Key.
                            return fetchNode(segment, shKey, builder, timer);
                        },
                        this.executor))
                .thenApply(v -> builder.build());
    }

    private CompletableFuture<Void> fetchNode(DirectSegmentAccess segment, UUID key, TableBucket.TableBucketBuilder builder, TimeoutTimer timer) {
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

    /**
     * Looks up a Backpointer offset.
     *
     * @param segment A DirectSegmentAccess providing access to the Segment to search in.
     * @param offset  The offset to find a backpointer from.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the backpointer offset, or -1 if no such pointer exists.
     */
    CompletableFuture<Long> getBackpointerOffset(DirectSegmentAccess segment, long offset, Duration timeout) {
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

    //endregion

    //region Bucket Updates

    /**
     * Generates an AttributeUpdate that creates a new or updates an existing Backpointer.
     *
     * @param fromOffset The offset at which the Backpointer originates.
     * @param toOffset   The offset at which the Backpointer ends.
     */
    AttributeUpdate generateBackpointerUpdate(long fromOffset, long toOffset) {
        return new AttributeUpdate(this.attributeCalculator.getBackpointerAttributeKey(fromOffset), AttributeUpdateType.Replace, toOffset);
    }

    /**
     * Generates an AttributeUpdate that removes a Backpointer, whether it exists or not.
     *
     * @param fromOffset The offset at which the Backpointer originates.
     */
    AttributeUpdate generateBackpointerRemoval(long fromOffset) {
        return new AttributeUpdate(this.attributeCalculator.getBackpointerAttributeKey(fromOffset), AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE);
    }

    /**
     * Generates a sequence of AttributeUpdates that will create a TableBucket or update an existing one.
     *
     * @param bucket              The Bucket to create.
     * @param keyHashes           A Map of {@link KeyHash} to Offsets representing the Hashes that currently mapped to
     *                            the given bucket, and for which we need to generate updates (to either alter the given
     *                            bucket or create sub-buckets).
     * @param attributeUpdates    A List of AttributeUpdates to collect updates in.
     */
    void generateBucketUpdate(TableBucket bucket, Map<KeyHash, Long> keyHashes, List<AttributeUpdate> attributeUpdates) {
        // TODO: implement.
    }

    /**
     * Generates one or more {@link AttributeUpdate}s that will delete a {@link TableBucket}.
     * <p>
     * NOTE: currently this will only fully delete a {@link TableBucket} if it is made of a single node. Multi-node buckets
     * cannot be deleted via this path since we do not have any information if they are used by other keys.
     *
     * @param bucket           The {@link TableBucket} to delete.
     * @param attributeUpdates A List of AttributeUpdates to collect updates in.
     */
    void generateBucketDelete(TableBucket bucket, List<AttributeUpdate> attributeUpdates) {
        // Be careful with deletes: we cannot delete the whole path (since it may be partially shared), so just delete
        // the data node. We'll delete the whole path upon compaction.
// TODO implement
    }

    /**
     * Generates a sequence of AttributeUpdates that will create a TableBucket or update an existing one.
     *
     * @param bucket              The Bucket to create.
     * @param keyHash             The Hash pertaining to the inserted data Key.
     * @param bucketSegmentOffset The offset of the last Key in the TableBucket. The AttributeUpdates generated by this
     *                            method will have this TableBucket pointing to this offset.
     * @param attributeUpdates    A List of AttributeUpdates to collect updates in.
     */
    private void generateBucketUpdate(TableBucket bucket, KeyHash keyHash, long bucketSegmentOffset, List<AttributeUpdate> attributeUpdates) {
        // TODO DELETE ME
        TableBucket.Node last = bucket.getLastNode();
        int hashIndex = last == null ? 0 : bucket.getNodes().size() - 1;
        Preconditions.checkArgument(hashIndex < keyHash.hashCount(),
                "Unable to update TableBucket since existing node count (%s) exceeds the available hash count (%s).", hashIndex, keyHash.hashCount());

        if (last != null && !last.isIndexNode()) {
            // We are simply updating a data node.
            attributeUpdates.add(generateAttributeUpdate(last.getKey(), false, 0, bucketSegmentOffset));
            return;
        }

        int newNodeCount = 0;
        while (hashIndex < keyHash.hashCount()) {
            // Every node except the last one is an Index Node.
            boolean indexNode = hashIndex < keyHash.hashCount() - 1;
            if (newNodeCount == 0) {
                // We haven't allocated anything. Figure out if we need to update the Primary Hash node or a Secondary Hash node.
                UUID key = hashIndex == 0
                        ? this.attributeCalculator.getPrimaryHashAttributeKey(keyHash.get(hashIndex))
                        : this.attributeCalculator.getSecondaryHashAttributeKey(keyHash.get(hashIndex), (int) last.getValue());

                // Set the node value.
                attributeUpdates.add(generateAttributeUpdate(key, indexNode, newNodeCount, bucketSegmentOffset));
            } else {
                // We've allocated at least something. Since we don't know the Node Ids we have previously allocated in this
                // run, we now have to use TABLE_NODE_ID arithmetic to allocate new ones.
                final int currentHashIndex = hashIndex;
                final int currentCount = newNodeCount;
                attributeUpdates.add(generateAttributeUpdate(
                        new AttributeReference<>(Attributes.TABLE_NODE_ID,
                                nodeId -> this.attributeCalculator.getSecondaryHashAttributeKey(keyHash.get(currentHashIndex), (int) (long) nodeId + currentCount - 1)),
                        indexNode, currentCount, bucketSegmentOffset));
            }

            newNodeCount++;
            hashIndex++;
        }

        if (newNodeCount > 0) {
            // We need to update the TABLE_NODE_ID to account for all the new index nodes we allocated.
            // The last one is a data node, account for that by subtracting 1 from count.
            attributeUpdates.add(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.Accumulate, newNodeCount - 1));
        }
    }

    private AttributeUpdate generateAttributeUpdate(UUID key, boolean indexNode, int updateCount, long bucketSegmentOffset) {
        if (indexNode) {
            // Index node: need to insert pointer to new node.
            return new AttributeUpdateByReference(
                    key,
                    AttributeUpdateType.Replace,
                    new AttributeReference<>(Attributes.TABLE_NODE_ID, value -> this.attributeCalculator.getIndexNodeAttributeValue(value + updateCount)));
        } else {
            // Data node: need to insert pointer to the current segment length.
            return new AttributeUpdate(key, AttributeUpdateType.Replace, this.attributeCalculator.getSegmentOffsetAttributeValue(bucketSegmentOffset));
        }
    }

    private AttributeUpdate generateAttributeUpdate(AttributeReference<UUID> attributeReference, boolean indexNode, int updateCount, long bucketSegmentOffset) {
        if (indexNode) {
            // Index node: need to insert pointer to new node.
            return new AttributeUpdateByReference(
                    attributeReference,
                    AttributeUpdateType.Replace,
                    new AttributeReference<>(Attributes.TABLE_NODE_ID, value -> this.attributeCalculator.getIndexNodeAttributeValue(value + updateCount)));
        } else {
            // Data node: need to insert pointer to the current segment length.
            return new AttributeUpdateByReference(attributeReference, AttributeUpdateType.Replace, this.attributeCalculator.getSegmentOffsetAttributeValue(bucketSegmentOffset));
        }
    }

    //endregion
}