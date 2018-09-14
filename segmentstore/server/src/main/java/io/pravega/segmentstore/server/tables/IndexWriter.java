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
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeReference;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateByReference;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

/**
 * Provides read-write access to a Hash Array Mapped Tree implementation over Extended Attributes.
 */
class IndexWriter extends IndexReader {
    //region Members

    private final KeyHasher hasher;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the IndexWriter class.
     *
     * @param keyHasher The {@link KeyHasher} to use for hashing keys.
     * @param executor  An Executor to use for async tasks.
     */
    IndexWriter(@NonNull KeyHasher keyHasher, ScheduledExecutorService executor) {
        super(executor);
        this.hasher = keyHasher;
    }

    //endregion

    //region Initial Table Attributes

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

    //region Updating Table Buckets

    /**
     * Groups the given {@link KeyUpdate} instances by their associated buckets. These buckets may be partial
     * (i.e., only part of the hash matched) or a full match.
     *
     * @param keyUpdates A Collection of {@link KeyUpdate} instances to index.
     * @param segment    The Segment to read from.
     * @param timer      Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the a collection of {@link BucketUpdate}s.
     */
    CompletableFuture<Collection<BucketUpdate>> groupByBucket(Collection<KeyUpdate> keyUpdates, DirectSegmentAccess segment, TimeoutTimer timer) {
        val result = new HashMap<TableBucket, BucketUpdate>();

        return Futures.loop(
                keyUpdates,
                item -> {
                    // Locate the Key's Bucket using the key's Hash.
                    KeyHash hash = this.hasher.hash(item.getKey());
                    return locateBucket(hash, segment, timer)
                            .thenApply(bucket -> {
                                // Add the bucket to the result and record this Key as a "new" key in it.
                                BucketUpdate bu = result.computeIfAbsent(bucket, BucketUpdate::new);
                                bu.withKeyUpdate(item);
                                return true;
                            });
                }, this.executor)
                      .thenApply(v -> result.values());
    }

    /**
     * Determines what Segment Attribute Updates are necessary to apply the given bucket updates and executes them
     * onto the given Segment.
     *
     * @param bucketUpdates      A Collection of {@link BucketUpdate} instances to apply. Each such instance refers to
     *                           a different {@link TableBucket} and contains the existing state and changes for it alone.
     * @param segment            A {@link DirectSegmentAccess} representing the Segment to apply the updates to.
     * @param firstIndexedOffset The first offset in the Segment that is indexed. This will be used as a conditional update
     *                           constraint (matched against the Segment's {@link Attributes#TABLE_INDEX_OFFSET}) to verify
     *                           the update will not corrupt the data (i.e., we do not overlap with another update).
     * @param lastIndexedOffset  The last offset in the Segment that is indexed. The Segment's {@link Attributes#TABLE_INDEX_OFFSET}
     *                           will be updated to this value (atomically) upon a successful completion of his call.
     * @param timeout            Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of Segment Attributes updated. If the
     * operation failed, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} if the update failed due to firstIndexOffset not matching the Segment's
     * {@link Attributes#TABLE_INDEX_OFFSET}) attribute value.
     * </ul>
     */
    CompletableFuture<Integer> updateBuckets(Collection<BucketUpdate> bucketUpdates, DirectSegmentAccess segment,
                                             long firstIndexedOffset, long lastIndexedOffset, Duration timeout) {
        List<AttributeUpdate> attributeUpdates = new ArrayList<>();

        // Process each Key in the given Map.
        // Locate the Key's Bucket, then generate necessary Attribute Updates to integrate new Keys into it.
        for (BucketUpdate bucketUpdate : bucketUpdates) {
            generateAttributeUpdates(bucketUpdate, attributeUpdates);
        }

        if (attributeUpdates.isEmpty()) {
            // We haven't made any updates.
            return CompletableFuture.completedFuture(0);
        } else {
            // Atomically update the Last Indexed Offset in the Segment's metadata, once we apply these changes.
            attributeUpdates.add(generateUpdateLastIndexedOffset(firstIndexedOffset, lastIndexedOffset));
            return segment.updateAttributes(attributeUpdates, timeout)
                          .thenApply(v -> attributeUpdates.size());
        }
    }

    /**
     * Generates the necessary {@link AttributeUpdate}s to index updates to the given {@link BucketUpdate}.
     *
     * Backpointer updates:
     * - Backpointers are used to resolve collisions when we have exhausted the full hash (so we cannot grow the tree
     * anymore). As such, we only calculate backpointers after we group the keys based on their full hashes.
     * - We need to handle overwritten Keys, as well as linking the new Keys.
     * - When an existing Key is overwritten, the Key after it needs to be linked to the Key before it (and its link to
     * the Key before it removed). No other links between existing Keys need to be changed.
     * - New Keys need to be linked between each other, and the first one linked to the last of existing Keys.
     *
     * TableBucket updates:
     * - We need to generate sub-buckets to resolve collisions (as much as we can grow the tree). In each such sub-bucket,
     * we simply need to have the {@link TableBucket} point to the last key in updatedKeys. Backpointers will complete
     * the data structure by providing collision resolution for those remaining cases.
     *
     * @param bucketUpdate     The {@link BucketUpdate} to generate updates for.
     * @param attributeUpdates A List of {@link AttributeUpdate}s to collect into.
     */
    private void generateAttributeUpdates(BucketUpdate bucketUpdate, List<AttributeUpdate> attributeUpdates) {
        if (!bucketUpdate.hasUpdates()) {
            // Nothing to do.
            return;
        }

        // Sanity check 1: If we get a full bucket (that points to a data node), then it must have at least one existing
        // key, otherwise (index buckets) must not have any.
        TableBucket bucket = bucketUpdate.getBucket();
        boolean indexBucket = bucket.getLastNode() == null || bucket.getLastNode().isIndexNode();
        Preconditions.checkArgument(indexBucket == bucketUpdate.getExistingKeys().isEmpty(),
                "Index Buckets must have no existing keys, while non-index Buckets must not have.");

        // Group all Keys by Key Hash. This will help us define the new buckets.
        val bucketsByHash = bucketUpdate.groupByHash(this.hasher::hash);

        // Sanity check 2: If we grouped in more than 1 hash, then the TableBucket must not have a full path. If it did,
        // then only one KeyHash can possibly map to it (and not two or more).
        Preconditions.checkState(bucketsByHash.size() == 1 || bucket.getNodes().size() < this.hasher.getHashPartCount(),
                "Exactly one KeyHash can map to a full-path TableBucket.");

        // Keep track of the new sub-bucket offsets.
        HashMap<KeyHash, Long> newBucketOffsets = new HashMap<>();

        // Keep track whether the bucket was deleted (if we get at least one update, it was not).
        boolean isDeleted = true;
        for (val entry : bucketsByHash.entrySet()) {
            BucketUpdate update = entry.getValue();

            // All Keys in this bucket have the same full hash. If there is more than one key per such hash, then we have
            // a collision, which must be resolved (this also handles removed keys).
            generateBackpointerUpdates(update, attributeUpdates);

            // Backpointers help us create a linked list (of keys) for those buckets which have collisions (in reverse
            // order, by offset). At this point, we only need to figure out the highest offset of a Key in each bucket,
            // which will then become the Bucket's offset.
            val bucketOffset = update.getBucketOffset();
            if (bucketOffset >= 0) {
                // We have an update.
                newBucketOffsets.put(entry.getKey(), bucketOffset);
                isDeleted = false;
            }
        }

        // 2. Bucket updates.
        if (isDeleted) {
            generateBucketDelete(bucket, attributeUpdates);
        } else {
            generateBucketUpdate(bucket, newBucketOffsets, attributeUpdates);
        }
    }

    /**
     * Generates the necessary Backpointer updates for the given {@link BucketUpdate}.
     *
     * @param update           The BucketUpdate to generate Backpointers for.
     * @param attributeUpdates A List of {@link AttributeUpdate} where the updates will be collected.
     */
    private void generateBackpointerUpdates(BucketUpdate update, List<AttributeUpdate> attributeUpdates) {
        // Keep track of the previous, non-deleted Key's offset. The first one points to nothing.
        AtomicLong previousOffset = new AtomicLong(Attributes.NULL_ATTRIBUTE_VALUE);

        // Keep track of whether the previous Key has been replaced.
        AtomicBoolean previousReplaced = new AtomicBoolean(false);

        // Process all existing Keys, in order of Offsets, and either unlink them (if replaced) or update pointers as needed.
        update.getExistingKeys().stream()
              .sorted(Comparator.comparingLong(KeyInfo::getOffset))
              .forEach(keyInfo -> {
                  boolean replaced = update.isKeyUpdated(keyInfo.getKey());
                  if (replaced) {
                      // This one has been replaced or removed; delete any backpointer originating from it.
                      attributeUpdates.add(generateBackpointerRemoval(keyInfo.getOffset()));
                      previousReplaced.set(true);
                  } else if (previousReplaced.get()) {
                      // This one hasn't been replaced or removed, however its previous one has been.
                      // Repoint it to whatever key is now ahead of it, or remove it (if previousOffset is nothing).
                      attributeUpdates.add(generateBackpointerUpdate(keyInfo.getOffset(), previousOffset.get()));
                      previousReplaced.set(false);
                      previousOffset.set(keyInfo.getOffset());
                  }
              });

        // Process all the new Keys, in order of offsets, and add any backpointers as needed, making sure to also link them
        // to whatever surviving existing Keys we might still have.
        update.getKeyUpdates().stream()
              .filter(keyUpdate -> !keyUpdate.isDeleted())
              .sorted(Comparator.comparingLong(KeyUpdate::getOffset))
              .forEach(keyUpdate -> {
                  if (previousOffset.get() != Attributes.NULL_ATTRIBUTE_VALUE) {
                      // Only add a backpointer if we have another Key ahead of it.
                      attributeUpdates.add(generateBackpointerUpdate(keyUpdate.getOffset(), previousOffset.get()));
                  }

                  previousOffset.set(keyUpdate.getOffset());
              });
    }

    /**
     * Generates an AttributeUpdate that creates a new or updates an existing Backpointer.
     *
     * @param fromOffset The offset at which the Backpointer originates.
     * @param toOffset   The offset at which the Backpointer ends.
     */
    private AttributeUpdate generateBackpointerUpdate(long fromOffset, long toOffset) {
        return new AttributeUpdate(this.attributeCalculator.getBackpointerAttributeKey(fromOffset), AttributeUpdateType.Replace, toOffset);
    }

    /**
     * Generates an AttributeUpdate that removes a Backpointer, whether it exists or not.
     *
     * @param fromOffset The offset at which the Backpointer originates.
     */
    private AttributeUpdate generateBackpointerRemoval(long fromOffset) {
        return new AttributeUpdate(this.attributeCalculator.getBackpointerAttributeKey(fromOffset), AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE);
    }

    /**
     * Generates one or more {@link AttributeUpdate}s that will delete a {@link TableBucket}.
     *
     * NOTE: currently this will only fully delete a {@link TableBucket} if it is made of a single node. Multi-node buckets
     * cannot be deleted via this path since we do not have any information if they are used by other keys.
     *
     * @param bucket           The {@link TableBucket} to delete.
     * @param attributeUpdates A List of AttributeUpdates to collect updates in.
     */
    private void generateBucketDelete(TableBucket bucket, List<AttributeUpdate> attributeUpdates) {
        TableBucket.Node last = bucket.getLastNode();
        if (last == null || last.isIndexNode()) {
            // This bucket path does not lead to an actual data node. Nothing we can do here.
            return;
        }

        // We need to be careful with deletes: we cannot delete the whole path (since it may be partially shared), so
        // the only thing we can delete is the data node. We'll delete the whole path during a compaction.
        attributeUpdates.add(new AttributeUpdate(last.getKey(), AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE));
    }

    /**
     * Generates a sequence of AttributeUpdates that will create or update the necessary Table Buckets based on the given
     * KeyHashes.
     *
     * Notes:
     * - If keyHashes.size() == 1, then bucket will be updated to point to whatever value is stored in the keyHashes entry.
     * - if keyHashes.size() > 1, then the given bucket will be transformed into keyHashes.size() buckets, each having
     * the given bucket as a root. The original bucket will have its data node pointer deleted and replaced with one or
     * more index node pointers. In this case
     *
     * @param bucket           The Bucket to create.
     * @param keyHashes        A Map of {@link KeyHash} to Offsets representing the Hashes that currently mapped to
     *                         the given bucket, and for which we need to generate updates (to either alter the given
     *                         bucket or create sub-buckets).
     * @param attributeUpdates A List of AttributeUpdates to collect updates in.
     */
    private void generateBucketUpdate(TableBucket bucket, Map<KeyHash, Long> keyHashes, List<AttributeUpdate> attributeUpdates) {
        // Figure out where we need to start from:
        // - If the bucket is empty, we start at the beginning (hashIndex = 0)
        // - If the bucket points to a data node, that will need to be replaced (hashIndex = NodeCount - 1)
        // - If the bucket points to an index node, we will have to continue (hashIndex = NodeCount)
        TableBucket.Node last = bucket.getLastNode();
        int initialHashIndex = last == null || last.isIndexNode()
                ? bucket.getNodes().size()
                : bucket.getNodes().size() - 1;
        Preconditions.checkArgument(initialHashIndex <= this.hasher.getHashPartCount(), "Unable to update TableBucket since existing node count "
                + "(%s) exceeds the available hash count (%s).", initialHashIndex, this.hasher.getHashPartCount());

        // Generate any updates necessary, by doing a depth-first recursion down to each branch before moving on to adjacent ones.
        int newNodeCount = generateBucketUpdate(new KeyHashGroup(initialHashIndex - 1, -1, null, keyHashes), 0, last, attributeUpdates);

        if (newNodeCount > 0) {
            // We need to update the TABLE_NODE_ID to account for all the new index nodes we allocated.
            // We need to account for the new data nodes, so subtract the number of keys from the count.
            attributeUpdates.add(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.Accumulate, newNodeCount));
        }
    }

    // TODO: javadoc here, and below
    private int generateBucketUpdate(KeyHashGroup group, int nodeIdOffset, TableBucket.Node last, List<AttributeUpdate> attributeUpdates) {
        Preconditions.checkState(group.getHashIndex() < this.hasher.getHashPartCount(), "Unable to update TableBucket; " +
                "reached the maximum hash count (%s) but there are still hashes to index.", this.hasher.getHashPartCount());

        int newNodeCount = 0;
        val subGroups = generateNextGroup(group);
        for (val g : subGroups) {
            boolean dataNode = g.hashes.size() == 1;
            if (dataNode) {
                // Data node.
                long value = getDataNodeValue(g);
                if (g.isFirstLevel()) {
                    UUID attributeId = getFirstLevelAttributeKey(g, last);
                    attributeUpdates.add(new AttributeUpdate(attributeId, AttributeUpdateType.Replace, value));
                } else {
                    val idReference = getNonFirstLevelAttributeKey(g, nodeIdOffset);
                    attributeUpdates.add(new AttributeUpdateByReference(idReference, AttributeUpdateType.Replace, value));
                }
            } else {
                // Index node.
                val valueReference = getIndexNodeValue(nodeIdOffset);
                if (g.isFirstLevel()) {
                    UUID attributeId = getFirstLevelAttributeKey(g, last);
                    attributeUpdates.add(new AttributeUpdateByReference(attributeId, AttributeUpdateType.Replace, valueReference));
                } else {
                    val idReference = getNonFirstLevelAttributeKey(g, nodeIdOffset);
                    attributeUpdates.add(new AttributeUpdateByReference(idReference, AttributeUpdateType.Replace, valueReference));
                }

                // Move on to the next part of this KeyHash, while also recording that we have allocated one TableNode.
                newNodeCount += generateBucketUpdate(g, nodeIdOffset + 1, last, attributeUpdates) + 1;

                // Need to keep track of how many nodes we allocated; subsequent allocations must use new numbers.
                nodeIdOffset += newNodeCount;
            }
        }

        return newNodeCount;
    }

    private Collection<KeyHashGroup> generateNextGroup(KeyHashGroup group) {
        int hashIndex = group.getHashIndex() + 1;
        int depth = group.getDepth() + 1;
        val result = new HashMap<HashedArray, KeyHashGroup>();
        for (val e : group.getHashes().entrySet()) {
            KeyHash hash = e.getKey();
            val hashPart = new HashedArray(hash.getPart(hashIndex).getCopy()); // TODO: maybe getPart returns a HashedArray.
            KeyHashGroup newGroup = result.computeIfAbsent(hashPart, ignored -> new KeyHashGroup(hashIndex, depth, hashPart, new HashMap<>()));
            newGroup.hashes.put(e.getKey(), e.getValue());
        }

        return result.values();
    }

    private long getDataNodeValue(KeyHashGroup g) {
        long bucketSegmentOffset = g.hashes.values().stream().findFirst().orElse(-1L);
        assert bucketSegmentOffset >= 0;
        return this.attributeCalculator.getSegmentOffsetAttributeValue(bucketSegmentOffset);
    }

    private UUID getFirstLevelAttributeKey(KeyHashGroup g, TableBucket.Node last) {
        return g.getHashIndex() == 0
                ? this.attributeCalculator.getPrimaryHashAttributeKey(g.getHashPart())
                : this.attributeCalculator.getSecondaryHashAttributeKey(g.getHashPart(), (int) last.getValue());
    }

    private AttributeReference<UUID> getNonFirstLevelAttributeKey(KeyHashGroup g, int nodeIdOffset) {
        return new AttributeReference<>(Attributes.TABLE_NODE_ID,
                nodeId -> this.attributeCalculator.getSecondaryHashAttributeKey(g.getHashPart(), (int) (long) nodeId + nodeIdOffset - 1));
    }

    private AttributeReference<Long> getIndexNodeValue(int nodeIdOffset) {
        return new AttributeReference<>(Attributes.TABLE_NODE_ID,
                value -> this.attributeCalculator.getIndexNodeAttributeValue(value + nodeIdOffset));
    }

    /**
     * Generates a conditional {@link AttributeUpdate} that sets a new value for the {@link Attributes#TABLE_INDEX_OFFSET}
     * attribute.
     *
     * @param currentOffset The current offset. This will be used for the conditional update.
     * @param newOffset     The new offset to set.
     * @return The generated {@link AttributeUpdate}.
     */
    private AttributeUpdate generateUpdateLastIndexedOffset(long currentOffset, long newOffset) {
        Preconditions.checkArgument(currentOffset <= newOffset, "newOffset must be larger than existingOffset");
        return new AttributeUpdate(Attributes.TABLE_INDEX_OFFSET, AttributeUpdateType.ReplaceIfEquals, newOffset, currentOffset);
    }

    //region KeyHashGroup

    @Data
    private static class KeyHashGroup {
        private final int hashIndex;
        private final int depth;
        private final ArrayView hashPart; // The KeyHash part based on which the grouping has been done.
        private final Map<KeyHash, Long> hashes;

        boolean isFirstLevel() {
            return this.depth == 0;
        }
    }

    //endregion
}
