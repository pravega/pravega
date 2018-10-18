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
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeUpdate;
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
import java.util.stream.Collectors;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

/**
 * Provides read-write access to a Hash Array Mapped Tree implementation over Extended Attributes.
 * See {@link IndexReader} for a description of the Index Structure.
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
    static Collection<AttributeUpdate> generateInitialTableAttributes() {
        return Arrays.asList(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.None, 0L),
                new AttributeUpdate(Attributes.TABLE_INDEX_OFFSET, AttributeUpdateType.None, 0L));
    }

    //endregion

    //region Updating Table Buckets

    /**
     * Groups the given {@link BucketUpdate.KeyUpdate} instances by their associated buckets. These buckets may be partial
     * (i.e., only part of the hash matched) or a full match.
     *
     * @param keyUpdates A Collection of {@link BucketUpdate.KeyUpdate} instances to index.
     * @param segment    The Segment to read from.
     * @param timer      Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the a collection of {@link BucketUpdate}s.
     */
    CompletableFuture<Collection<BucketUpdate>> groupByBucket(Collection<BucketUpdate.KeyUpdate> keyUpdates,
                                                              DirectSegmentAccess segment, TimeoutTimer timer) {
        val updatesByHash = keyUpdates.stream()
                                      .collect(Collectors.groupingBy(k -> this.hasher.hash(k.getKey())));
        return locateBuckets(updatesByHash.keySet(), segment, timer)
                .thenApplyAsync(buckets -> {
                    val result = new HashMap<TableBucket, BucketUpdate>();
                    buckets.forEach((keyHash, bucket) -> {
                        // Add the bucket to the result and record this Key as a "new" key in it.
                        BucketUpdate bu = result.computeIfAbsent(bucket, BucketUpdate::new);
                        updatesByHash.get(keyHash).forEach(bu::withKeyUpdate);
                    });

                    return result.values();
                }, this.executor);
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
     * @return A CompletableFuture that, when completed, will contain the number of new Index Nodes added. If the
     * operation failed, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} if the update failed due to firstIndexOffset not matching the Segment's
     * {@link Attributes#TABLE_INDEX_OFFSET}) attribute value or if it failed due to a concurrent call to this method which
     * modified the {@link Attributes#TABLE_NODE_ID} attribute. Both cases are retryable, but if TABLE_INDEX_OFFSET mismatches,
     * the entire bucketUpdates argument must be reconstructed with the reconciled value (to prevent index corruption).
     * </ul>
     */
    CompletableFuture<Integer> updateBuckets(Collection<BucketUpdate> bucketUpdates, DirectSegmentAccess segment,
                                             long firstIndexedOffset, long lastIndexedOffset, Duration timeout) {
        List<AttributeUpdate> attributeUpdates = new ArrayList<>();

        // Process each Key in the given Map.
        // Locate the Key's Bucket, then generate necessary Attribute Updates to integrate new Keys into it.
        final int initialTableNodeId = getTableNodeId(segment.getInfo());
        int currentTableNodeId = initialTableNodeId;
        for (BucketUpdate bucketUpdate : bucketUpdates) {
            currentTableNodeId += generateAttributeUpdates(bucketUpdate, currentTableNodeId, attributeUpdates);
        }

        if (lastIndexedOffset > firstIndexedOffset) {
            // Atomically update the Table-related attributes in the Segment's metadata, once we apply these changes.
            generateTableAttributeUpdates(firstIndexedOffset, lastIndexedOffset, initialTableNodeId, currentTableNodeId, attributeUpdates);
        }

        if (attributeUpdates.isEmpty()) {
            // We haven't made any updates.
            return CompletableFuture.completedFuture(0);
        } else {
            final int result = currentTableNodeId - initialTableNodeId;
            return segment.updateAttributes(attributeUpdates, timeout)
                          .thenApply(v -> result);
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
     * @param currentNodeId    The next available Node Id that can be allocated.
     * @param attributeUpdates A List of {@link AttributeUpdate}s to collect into.
     * @return The number of new index nodes added.
     */
    private int generateAttributeUpdates(BucketUpdate bucketUpdate, int currentNodeId, List<AttributeUpdate> attributeUpdates) {
        if (!bucketUpdate.hasUpdates()) {
            // Nothing to do.
            return 0;
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
        int newIndexNodeCount = 0;
        if (isDeleted) {
            generateBucketDelete(bucket, attributeUpdates);
        } else {
            newIndexNodeCount = generateBucketUpdate(bucket, newBucketOffsets, currentNodeId, attributeUpdates);
        }

        return newIndexNodeCount;
    }

    /**
     * Generates a sequence of {@link AttributeUpdate}s that will create or update the necessary Table Buckets based on
     * the given KeyHashes.
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
     * @param currentNodeId    The next available Node Id that can be allocated.
     * @param attributeUpdates A List of {@link AttributeUpdate}s to collect updates in.
     * @return The number of new index nodes allocated.
     */
    private int generateBucketUpdate(TableBucket bucket, Map<KeyHash, Long> keyHashes, int currentNodeId, List<AttributeUpdate> attributeUpdates) {
        // Figure out where we need to start from:
        // - If the bucket is empty, we start at the beginning (hashIndex = 0)
        // - If the bucket points to a data node, that will need to be replaced (hashIndex = NodeCount - 1)
        // - If the bucket points to an index node, we will have to continue (hashIndex = NodeCount)
        int initialHashIndex = bucket.isPartial()
                ? bucket.getNodes().size()
                : bucket.getNodes().size() - 1;
        Preconditions.checkArgument(initialHashIndex <= this.hasher.getHashPartCount(), "Unable to update TableBucket since existing node count "
                + "(%s) exceeds the available hash count (%s).", initialHashIndex, this.hasher.getHashPartCount());

        // Generate any updates necessary, by doing a depth-first recursion down to each branch before moving on to adjacent ones.
        val rootGroup = new KeyHashGroup(initialHashIndex - 1, -1, null, keyHashes);
        return generateBucketUpdate(rootGroup, currentNodeId, bucket.getLastNode(), attributeUpdates);
    }

    /**
     * Generates a sequence of {@link AttributeUpdate}s that will create or or update the necessary Table Buckets for
     * the given {@link KeyHashGroup}.
     *
     * @param group            The {@link KeyHashGroup} to generate updates for.
     * @param parentNodeId     The Node Id for the parent index node.
     * @param last             The existing {@link TableBucket}'s last node. All updates will be linked from this node.
     * @param attributeUpdates A List of {@link AttributeUpdate}s to collect updates in.
     * @return The number of new index nodes allocated.
     */
    private int generateBucketUpdate(KeyHashGroup group, int parentNodeId, TableBucket.Node last, List<AttributeUpdate> attributeUpdates) {
        Preconditions.checkState(group.getHashIndex() < this.hasher.getHashPartCount(), "Unable to update TableBucket; " +
                "reached the maximum hash count (%s) but there are still hashes to index.", this.hasher.getHashPartCount());

        int newNodeCount = 0;
        val subGroups = group.generateNextGroup();
        for (val subGroup : subGroups) {
            boolean dataNode = subGroup.getHashes().size() == 1;
            if (dataNode) {
                attributeUpdates.add(generateDataNodeUpdate(subGroup, parentNodeId, last));
            } else {
                // Index node.
                // We need to allocate a new index node. Its offset will be based on the parent node id offset, plus how
                // many index nodes we've allocated here.
                int indexNodeId = parentNodeId + 1 + newNodeCount;
                attributeUpdates.add(generateIndexNodeUpdate(subGroup, parentNodeId, indexNodeId, last));

                // Move on to the next part of this KeyHash, and pass down our own Node Id Offset.
                newNodeCount += generateBucketUpdate(subGroup, indexNodeId, last, attributeUpdates) + 1;
            }
        }

        return newNodeCount;
    }

    /**
     * Generates an {@link AttributeUpdate} that will create (a new) or update (an existing) Index Node using the given information.
     *
     * @param group        The {@link KeyHashGroup} to generate the update for.
     * @param parentNodeId The Node Id for the parent index node.
     *                     This only applies if this Index Node is not a top-level node (i.e., if it has parents).
     * @param indexNodeId  The Node Id to assign to this Index Node.
     * @param last         The existing {@link TableBucket}'s last node. All updates will be linked from this node.
     * @return An {@link AttributeUpdate} .
     */
    private AttributeUpdate generateIndexNodeUpdate(KeyHashGroup group, int parentNodeId, int indexNodeId, TableBucket.Node last) {
        UUID attributeId;
        if (group.isFirstLevel()) {
            // One of the "roots" of the tree.
            attributeId = getFirstLevelAttributeKey(group, last);
        } else {
            // Intermediate index nodes. Need to refer to the parent's Node Id so we can properly link it.
            attributeId = getNonFirstLevelAttributeKey(group, parentNodeId);
        }

        long value = this.attributeCalculator.getIndexNodeAttributeValue(indexNodeId);
        return new AttributeUpdate(attributeId, AttributeUpdateType.Replace, value);
    }

    /**
     * Generates an {@link AttributeUpdate} that will create (a new) or update (an existing) Data Node using the given information.
     *
     * @param group        The {@link KeyHashGroup} to generate the update for.
     * @param parentNodeId The Node Id for the parent index node.
     *                     This only applies if this Data Node is not a top-level node (i.e., if it has parents).
     * @param last         The existing {@link TableBucket}'s last node. All updates will be linked from this node.
     * @return An {@link AttributeUpdate} .
     */
    private AttributeUpdate generateDataNodeUpdate(KeyHashGroup group, int parentNodeId, TableBucket.Node last) {
        long value = getDataNodeValue(group);
        UUID attributeId;
        if (group.isFirstLevel()) {
            // Top-level Data Node. No parents, so no need to reference the parentNodeId. Just generate the Key.
            attributeId = getFirstLevelAttributeKey(group, last);
        } else {
            // This data node has parents. Need to link it to the parent via ts parentNodeId.
            attributeId = getNonFirstLevelAttributeKey(group, parentNodeId);
        }

        return new AttributeUpdate(attributeId, AttributeUpdateType.Replace, value);
    }

    /**
     * Generates a {@link UUID} which represents an Attribute ID for a node (Index or Data) that is either top-level (no parents)
     * or for which the parent is already present in the Segment's attributes (i.e., it has an assigned Node Id).
     *
     * @param group          The {@link KeyHashGroup} to generate for.
     * @param lastBucketNode The last existing Node in the TableBucket we want to update.
     * @return The UUID.
     */
    private UUID getFirstLevelAttributeKey(KeyHashGroup group, TableBucket.Node lastBucketNode) {
        if (group.getHashIndex() == 0) {
            // Top-level. Generate Primary Hash.
            return this.attributeCalculator.getPrimaryHashAttributeKey(group.getHashPart());
        } else if (lastBucketNode.isIndexNode()) {
            // We have a parent that is an index node, so must not touch it, but link from it; generate a
            // Secondary hash to expand the branch based on our parent's Node Id.
            return this.attributeCalculator.getSecondaryHashAttributeKey(group.getHashPart(), (int) lastBucketNode.getValue());
        } else {
            // We have a "parent", but this is a data node. We need to replace it with whatever we have in store. If we
            // have multiple keys (and the hashes haven't been exhausted), we will replace this data node with one or more
            // index nodes. If we have a single key, then we'll just update this entry.
            return lastBucketNode.getKey();
        }
    }

    /**
     * Generates an Attribute Id that represents the Key for a node (Index or Data) that has parents, but whose parents
     * are not yet present in the Index (i.e., their Node Ids are generated as part of the same update batch as this node).
     *
     * @param group  The {@link KeyHashGroup} to generate for.
     * @param nodeId The Node Id to use.
     * @return An UUID representing the Attribute Key that was generated.
     */
    private UUID getNonFirstLevelAttributeKey(KeyHashGroup group, int nodeId) {
        return this.attributeCalculator.getSecondaryHashAttributeKey(group.getHashPart(), nodeId);
    }

    /**
     * Calculates the value to store in a Data Node.
     *
     * @param group The {@link KeyHashGroup} to calculate the value for.
     * @return The value.
     */
    private long getDataNodeValue(KeyHashGroup group) {
        long bucketSegmentOffset = group.getHashes().values().stream().findFirst().orElse(-1L);
        assert bucketSegmentOffset >= 0;
        return this.attributeCalculator.getSegmentOffsetAttributeValue(bucketSegmentOffset);
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
        if (bucket.isPartial()) {
            // This bucket path does not lead to an actual data node. Nothing we can do here.
            return;
        }

        // We need to be careful with deletes: we cannot delete the whole path (since it may be partially shared), so
        // the only thing we can delete is the data node. We'll delete the whole path during a compaction.
        attributeUpdates.add(new AttributeUpdate(bucket.getLastNode().getKey(), AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE));
    }

    /**
     * Generates conditional {@link AttributeUpdate}s that update the values for Core Attributes representing the indexing
     * state of the Table Segment.
     *
     * @param currentOffset      The offset from which this indexing batch began. This will be checked against {@link Attributes#TABLE_INDEX_OFFSET}.
     * @param newOffset          The new offset to set for {@link Attributes#TABLE_INDEX_OFFSET}.
     * @param initialTableNodeId The initial value for TableNodeId. This will be checked against {@link Attributes#TABLE_NODE_ID}.
     * @param newTableNodeId     The new value for the {@link Attributes#TABLE_NODE_ID} attribute.
     * @param attributeUpdates   A List of {@link AttributeUpdate} to add the updates to.
     */
    private void generateTableAttributeUpdates(long currentOffset, long newOffset, long initialTableNodeId, long newTableNodeId, List<AttributeUpdate> attributeUpdates) {
        // Add an Update for the TABLE_INDEX_OFFSET to indicate we have indexed everything up to this offset.
        Preconditions.checkArgument(currentOffset <= newOffset, "newOffset must be larger than existingOffset");
        attributeUpdates.add(new AttributeUpdate(Attributes.TABLE_INDEX_OFFSET, AttributeUpdateType.ReplaceIfEquals, newOffset, currentOffset));

        // Add an Update (if needed) for TABLE_NODE_ID to indicate the next available value for such a node.
        if (newTableNodeId > initialTableNodeId) {
            attributeUpdates.add(new AttributeUpdate(Attributes.TABLE_NODE_ID, AttributeUpdateType.ReplaceIfEquals, newTableNodeId, initialTableNodeId));
        }
    }

    //endregion

    //region Backpointers

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
        AtomicBoolean first = new AtomicBoolean(true);
        update.getExistingKeys().stream()
              .sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset))
              .forEach(keyInfo -> {
                    boolean replaced = update.isKeyUpdated(keyInfo.getKey());
                    if (replaced) {
                        // This one has been replaced or removed; delete any backpointer originating from it.
                        if (!first.get()) {
                            // ... except if this is the first one in the list, which means it doesn't have a backpointer.
                            attributeUpdates.add(generateBackpointerRemoval(keyInfo.getOffset()));
                        }

                        previousReplaced.set(true); // Record that this has been replaced.
                    } else {
                        if (previousReplaced.get()) {
                            // This one hasn't been replaced or removed, however its previous one has been.
                            // Repoint it to whatever key is now ahead of it, or remove it (if previousOffset is nothing).
                            attributeUpdates.add(generateBackpointerUpdate(keyInfo.getOffset(), previousOffset.get()));
                            previousReplaced.set(false);
                        }

                        previousOffset.set(keyInfo.getOffset()); // Record the last valid offset.
                    }

                    first.set(false);
                });

        // Process all the new Keys, in order of offsets, and add any backpointers as needed, making sure to also link them
        // to whatever surviving existing Keys we might still have.
        update.getKeyUpdates().stream()
              .filter(keyUpdate -> !keyUpdate.isDeleted())
              .sorted(Comparator.comparingLong(BucketUpdate.KeyUpdate::getOffset))
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

    //endregion

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

        /**
         * Generates a Collection of {@link KeyHashGroup} based on the {@link KeyHash}es present in this group, but
         * regrouped by the next available Hash Index (incremented from the given {@link KeyHashGroup#getHashIndex()}.
         *
         * @return A Collection of {@link KeyHashGroup}s.
         */
        Collection<KeyHashGroup> generateNextGroup() {
            int hashIndex = this.hashIndex + 1;
            int depth = this.depth + 1;
            val result = new HashMap<HashedArray, KeyHashGroup>();
            for (val e : this.hashes.entrySet()) {
                KeyHash hash = e.getKey();
                val hashPart = new HashedArray(hash.getPart(hashIndex));
                KeyHashGroup newGroup = result.computeIfAbsent(hashPart, ignored -> new KeyHashGroup(hashIndex, depth, hashPart, new HashMap<>()));
                newGroup.hashes.put(e.getKey(), e.getValue());
            }

            return result.values();
        }
    }

    //endregion
}
