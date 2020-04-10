/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArrayComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.common.util.btree.sets.BTreeSet;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.val;

/**
 * Implementation for {@link SegmentSortedKeyIndex}.
 */
@Beta
class SegmentSortedKeyIndexImpl implements SegmentSortedKeyIndex {
    //region Members

    @VisibleForTesting
    static final Comparator<ArrayView> KEY_COMPARATOR = BTreeSet.COMPARATOR;
    private final String segmentName;
    private final SortedKeyIndexDataSource dataSource;
    @GuardedBy("tailKeys")
    private final TreeMap<ArrayView, CacheBucketOffset> tailKeys;
    private final BTreeSet sortedKeys;
    private final String traceLogId;

    //endregion

    SegmentSortedKeyIndexImpl(@NonNull String segmentName, @NonNull SortedKeyIndexDataSource dataSource, @NonNull Executor executor) {
        this.segmentName = segmentName;
        this.dataSource = dataSource;
        this.tailKeys = new TreeMap<>(KEY_COMPARATOR);
        this.traceLogId = String.format("SortedKeyIndex[%s]", this.segmentName);
        this.sortedKeys = new BTreeSet(TableStore.MAXIMUM_VALUE_LENGTH, TableStore.MAXIMUM_KEY_LENGTH,
                this::getBTreeSetPage, this::persistBTreeSet, executor, traceLogId);
    }

    //region SegmentSortedKeyIndex Implementation

    @Override
    public CompletableFuture<Void> persistUpdate(Collection<BucketUpdate> bucketUpdates, Duration timeout) {
        val args = prepareUpdate(bucketUpdates);
        if (args.isEmpty()) {
            // No insertion or deletion. Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        return this.sortedKeys.update(args.insertions, args.deletions, args.getOffsetCounter(), timeout);
    }

    private UpdateArgs prepareUpdate(Collection<BucketUpdate> bucketUpdates) {
        val result = new UpdateArgs();
        for (val bucketUpdate : bucketUpdates) {
            for (val keyUpdate : bucketUpdate.getKeyUpdates()) {
                if (this.dataSource.isKeyExcluded(keyUpdate.getKey())) {
                    // We are not processing our own keys.
                    continue;
                }

                boolean exists = bucketUpdate.keyExists(keyUpdate.getKey());
                if (keyUpdate.isDeleted()) {
                    if (exists) {
                        result.keyDeleted(keyUpdate.getKey(), keyUpdate.getOffset());
                    }
                } else if (!exists) {
                    result.keyInserted(keyUpdate.getKey(), keyUpdate.getOffset());
                }
            }
        }
        return result;
    }

    @Override
    public void includeTailUpdate(TableKeyBatch batch, long batchSegmentOffset) {
        synchronized (this.tailKeys) {
            // Include only external keys.
            batch.getItems().stream()
                    .filter(item -> !this.dataSource.isKeyExcluded(item.getKey().getKey()))
                    .forEach(item -> this.tailKeys.put(
                            item.getKey().getKey(),
                            new CacheBucketOffset(batchSegmentOffset + item.getOffset(), batch.isRemoval())));
        }
    }

    @Override
    public void includeTailCache(Map<? extends ArrayView, CacheBucketOffset> tailUpdates) {
        synchronized (this.tailKeys) {
            tailUpdates.forEach((key, offset) -> {
                if (!this.dataSource.isKeyExcluded(key)) {
                    this.tailKeys.put(key, offset);
                }
            });
        }
    }

    @Override
    public void updateSegmentIndexOffset(long offset) {
        synchronized (this.tailKeys) {
            if (offset < 0) {
                // Segment evicted.
                this.tailKeys.clear();
            } else {
                // Indexed up to an offset. It's safe to remove any tail entries below the offset.
                val toRemove = this.tailKeys.entrySet().stream()
                        .filter(e -> e.getValue().getSegmentOffset() <= offset)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                toRemove.forEach(this.tailKeys::remove);
            }
        }
    }

    @Override
    public AsyncIterator<List<ArrayView>> iterator(IteratorRange range, Duration fetchTimeout) {
        val lastKey = new AtomicReference<ArrayView>(range.getFrom());
        val persistedIterator = this.sortedKeys.iterator(range.getFrom(), false, range.getTo(), false, fetchTimeout);
        return () -> persistedIterator.getNext().thenApply(keys -> {
            keys = mixWithTail(keys, lastKey.get(), range.getTo());
            if (keys != null && !keys.isEmpty()) {
                // Keep track of the last key; we'll need it for the next iteration.
                lastKey.set(keys.get(keys.size() - 1));
            }
            return keys;
        });
    }

    @Override
    public IteratorRange getIteratorRange(@Nullable ArrayView fromKeyExclusive, @Nullable ArrayView prefix) {
        if (fromKeyExclusive != null && prefix != null) {
            // Validate args.
            Preconditions.checkArgument(KEY_COMPARATOR.compare(fromKeyExclusive, prefix) >= 0,
                    "FromKey does not begin with given prefix.");
        }

        if (fromKeyExclusive == null) {
            fromKeyExclusive = prefix;
        }

        val lastKeyExclusive = prefix == null ? null : ByteArrayComparator.getNextItemOfSameLength(prefix);
        return new IteratorRange(fromKeyExclusive, lastKeyExclusive);
    }

    //endregion

    //region Helpers

    private List<ArrayView> mixWithTail(List<ArrayView> persistedKeys, ArrayView fromExclusive, ArrayView toExclusive) {
        val tailResult = new ArrayList<ArrayView>();
        val tailKeys = new HashSet<HashedArray>();
        synchronized (this.tailKeys) {
            NavigableMap<ArrayView, CacheBucketOffset> tailSection;
            if (persistedKeys == null || persistedKeys.isEmpty()) {
                // No (or no more) items from the persisted index. Return as much as we can from our tail index.
                tailSection = getTailSection(fromExclusive, toExclusive, false);
            } else {
                // Match the range returned by BTreeSet.
                tailSection = getTailSection(fromExclusive, persistedKeys.get(persistedKeys.size() - 1), true);
            }

            tailSection.forEach((key, offset) -> {
                tailKeys.add(new HashedArray(key));
                if (!offset.isRemoval()) {
                    tailResult.add(key);
                }
            });
        }

        if (persistedKeys == null || persistedKeys.isEmpty()) {
            // We have reached the end of the iteration.
            return tailResult.isEmpty() ? null : tailResult;
        } else if (tailKeys.isEmpty()) {
            // Nothing in the tail; return whatever we got from the persisted set.
            return persistedKeys;
        }

        // Generate an iterator through the persisted keys that excludes anything that was updated in the tail.
        val persistedIterator = persistedKeys.stream().filter(key -> !tailKeys.contains(new HashedArray(key))).iterator();
        val tailIterator = tailResult.iterator();
        val result = new ArrayList<ArrayView>(persistedKeys.size() + tailResult.size());
        Iterators.mergeSorted(Arrays.asList(persistedIterator, tailIterator), KEY_COMPARATOR).forEachRemaining(result::add);
        return result;
    }

    @GuardedBy("tailKeys")
    private NavigableMap<ArrayView, CacheBucketOffset> getTailSection(ArrayView fromExclusive, ArrayView to, boolean toInclusive) {
        if (fromExclusive == null && to == null) {
            // Full map.
            return this.tailKeys;
        } else if (fromExclusive == null) {
            // No beginning.
            return this.tailKeys.headMap(to, toInclusive);
        } else if (to == null) {
            // No end.
            return this.tailKeys.tailMap(fromExclusive, false);
        } else {
            // Beginning and end.
            return this.tailKeys.subMap(fromExclusive, false, to, toInclusive);
        }
    }

    //endregion

    //region Data Source Access

    private CompletableFuture<ArrayView> getBTreeSetPage(long pageId, Duration timeout) {
        return this.dataSource.getRead().apply(this.segmentName, Collections.singletonList(pageIdToKey(pageId)), timeout)
                .thenApply(result -> result.isEmpty() || result.get(0) == null ? null : result.get(0).getValue());
    }

    private CompletableFuture<Void> persistBTreeSet(List<Map.Entry<Long, ArrayView>> toUpdate, Collection<Long> toDelete, Duration timeout) {
        CompletableFuture<?> updateResult;
        if (toUpdate.isEmpty()) {
            updateResult = CompletableFuture.completedFuture(null);
        } else {
            // TODO: break down into batches?
            val updateEntries = toUpdate.stream()
                    .map(e -> TableEntry.unversioned(pageIdToKey(e.getKey()), e.getValue()))
                    .collect(Collectors.toList());
            updateResult = this.dataSource.getUpdate().apply(this.segmentName, updateEntries, timeout);
        }

        CompletableFuture<?> deleteResult;
        if (toDelete.isEmpty()) {
            deleteResult = CompletableFuture.completedFuture(null);
        } else {
            val removeKeys = toDelete.stream()
                    .map(id -> TableKey.unversioned(pageIdToKey(id)))
                    .collect(Collectors.toList());
            deleteResult = this.dataSource.getDelete().apply(this.segmentName, removeKeys, timeout);
        }

        return CompletableFuture.allOf(updateResult, deleteResult);
    }

    private ArrayView pageIdToKey(long pageId) {
        byte[] b = new byte[Long.BYTES];
        BitConverter.writeLong(b, 0, pageId);
        return new ByteArraySegment(b);
    }

    //endregion

    //region Helper Classes

    private static class UpdateArgs {
        final HashSet<HashedArray> insertions = new HashSet<>();
        final HashSet<HashedArray> deletions = new HashSet<>();
        private long highestKeyOffset = -1;
        private int keyWithHighestOffsetLength = -1;

        void keyInserted(HashedArray key, long offset) {
            this.insertions.add(key);
            this.deletions.remove(key);
            updateHighestKeyOffsets(offset, key.getLength());
        }

        void keyDeleted(HashedArray key, long offset) {
            this.deletions.add(key);
            this.insertions.remove(key);
            updateHighestKeyOffsets(offset, key.getLength());
        }

        boolean isEmpty() {
            return this.insertions.isEmpty() && this.deletions.isEmpty();
        }

        private void updateHighestKeyOffsets(long offset, int length) {
            if (offset > this.highestKeyOffset) {
                this.highestKeyOffset = offset;
                this.keyWithHighestOffsetLength = length;
            }
        }

        private Supplier<Long> getOffsetCounter() {
            Preconditions.checkState(!isEmpty(), "Cannot generate Offset Counter for empty UpdateArgs.");
            Preconditions.checkArgument(this.highestKeyOffset >= 0, "highestKeyOffset must be a non-negative number.");
            Preconditions.checkArgument(this.keyWithHighestOffsetLength > 0, "keyWithHighestOffsetLength must be a positive number.");
            val nextId = new AtomicLong(this.highestKeyOffset);
            val maxValue = this.highestKeyOffset + this.keyWithHighestOffsetLength;
            return () -> {
                val result = nextId.incrementAndGet();
                Preconditions.checkState(result <= maxValue, "Maximum number of requests exceeded (from=%s, count=%s).",
                        this.highestKeyOffset, this.keyWithHighestOffsetLength);
                return result;
            };
        }
    }

    //endregion
}
