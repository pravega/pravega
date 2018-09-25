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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ContainerKeyIndex} class.
 */
public class ContainerKeyIndexTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 0;
    private static final int BATCH_SIZE = 100;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final KeyHasher HASHER = KeyHashers.DEFAULT_HASHER;

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability of the {@link ContainerKeyIndex} to handle unconditional updates.
     */
    @Test
    public void testUnconditionalUpdates() {
        final int iterationCount = 20;
        @Cleanup
        val context = new TestContext();

        // Generate a set of keys.
        // Update them repeatedly, and each time block their execution.
        // Unblock all, then verify the index is in the correct state (i.e., the batch with highest offset wins).
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        val updates = new ArrayList<UpdateItem>();
        for (int i = 0; i < iterationCount; i++) {
            val batch = toUpdateBatch(keys);
            val persist = new CompletableFuture<Long>();
            val update = context.index.update(context.segment, batch, () -> persist, context.timer);
            updates.add(new UpdateItem(batch, persist, update));
        }

        // Complete the persist futures in some arbitrary order (assign them arbitrary values)
        completePersistArbitrarily(updates, context);

        // Wait for the updates to complete.
        updates.stream().map(u -> u.update).forEach(CompletableFuture::join);

        // Check result.
        checkPrevailingUpdate(updates, context);
    }

    /**
     * Tests the ability of the {@link ContainerKeyIndex} to perform multi-key (batch) conditional updates. The conditions
     * are based both on pre-existing Key's versions and non-existing keys.
     */
    @Test
    public void testConditionalUpdates() throws Exception {
        final int versionedKeysPerBatch = 20;
        final int iterationCount = 10;
        @Cleanup
        val context = new TestContext();

        // Generate a set of unversioned keys.
        // At each iteration, pick a set of them and condition them on the previous Key's values being there.
        // The versioned set should overlap with the previous update's versioned set.
        // Each iteration updates all the keys.
        val unversionedKeys = generateUnversionedKeys(BATCH_SIZE, context);
        val updates = new ArrayList<UpdateItem>();
        long nextOffset = 0;
        for (int i = 0; i < iterationCount; i++) {
            val versionedCandidates = unversionedKeys.subList(i, i + versionedKeysPerBatch);
            List<TableKey> versionedKeys;
            if (updates.isEmpty()) {
                // First update (insertion): condition on not existing.
                versionedKeys = versionedCandidates.stream().map(k -> TableKey.notExists(k.getKey())).collect(Collectors.toList());
            } else {
                // Subsequent update: condition on previous value.
                UpdateItem lastUpdate = updates.get(updates.size() - 1);
                versionedKeys = new ArrayList<>();
                for (int j = 0; j < versionedCandidates.size(); j++) {
                    // Calculate the expected version. That is the offset of this item in the previous update.
                    long version = lastUpdate.offset.get() + lastUpdate.batch.getItems().get(i + j).getOffset();
                    versionedKeys.add(TableKey.versioned(versionedCandidates.get(j).getKey(), version));
                }
            }

            val batch = toUpdateBatch(unversionedKeys.subList(0, i), versionedKeys, unversionedKeys.subList(i + versionedKeys.size(), unversionedKeys.size()));
            val persist = new CompletableFuture<Long>();
            val update = context.index.update(context.segment, batch, () -> persist, context.timer);
            val updateItem = new UpdateItem(batch, persist, update);
            updateItem.offset.set(nextOffset);
            updates.add(updateItem);
            nextOffset += batch.getLength();
        }

        // Complete the persists on each update, and verify no update has been completed.
        updates.stream().skip(1).forEach(u -> u.persist.complete(u.offset.get()));
        for (val u : updates) {
            Assert.assertFalse("Not expecting update to be done yet.", u.update.isDone());
        }

        // Complete the first persist and verify that the updates were released in order (no exceptions).
        updates.get(0).persist.complete(updates.get(0).offset.get());
        for (val u : updates) {
            val updateResult = u.update.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected number of buckets returned.", u.batch.getItems().size(), updateResult.size());
            for (int i = 0; i < updateResult.size(); i++) {
                long expectedOffset = u.persist.join() + u.batch.getItems().get(i).getOffset();
                long actualOffset = updateResult.get(i);
                Assert.assertEquals("Unexpected offset for result index " + i, expectedOffset, actualOffset);
            }
        }

        // Check final result.
        checkPrevailingUpdate(updates, context);
    }

    /**
     * Tests the ability of the {@link ContainerKeyIndex} to reject conditional updates if the condition does not match
     * the state of the Key (version mismatch or key not exists).
     */
    @Test
    public void testConditionalUpdateFailure() {
        @Cleanup
        val context = new TestContext();

        Supplier<CompletableFuture<Long>> noPersist = () -> Futures.failedFuture(new AssertionError("Not expecting persist to be invoked."));
        val keyData = generateUnversionedKeys(1, context).get(0).getKey();

        // Key not exists, but we conditioned on it existing.
        AssertExtensions.assertThrows(
                "update() allowed conditional update on inexistent key conditioned on existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, 0L)), noPersist, context.timer),
                ex -> ex instanceof KeyNotExistsException && keyMatches(((KeyNotExistsException) ex).getKey(), keyData));

        // Create the key.
        context.index.update(context.segment,
                toUpdateBatch(TableKey.versioned(keyData, TableKey.NOT_EXISTS)),
                () -> CompletableFuture.completedFuture(0L),
                context.timer).join();

        // Key exists, but we conditioned on it not existing.
        AssertExtensions.assertThrows(
                "update() allowed conditional update on existent key conditioned on not existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, TableKey.NOT_EXISTS)), noPersist, context.timer),
                ex -> ex instanceof BadKeyVersionException && keyMatches(((BadKeyVersionException) ex).getKey(), keyData));

        // Key exists, but we conditioned on the wrong value.
        AssertExtensions.assertThrows(
                "update() allowed conditional update on inexistent key conditioned on existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, 123L)), noPersist, context.timer),
                ex -> ex instanceof BadKeyVersionException && keyMatches(((BadKeyVersionException) ex).getKey(), keyData));
    }

    /**
     * Tests the ability of the {@link ContainerKeyIndex#getBucketOffsets} to retrieve the offsets of buckets that
     * were not previously cached.
     */
    @Test
    public void testGetBucketOffsetsNotCached() {
        @Cleanup
        val context = new TestContext();

        // Setup the segment with initial attributes.
        val iw = new IndexWriter(HASHER, executorService());
        context.segment.updateAttributes(IndexWriter.generateInitialTableAttributes(), TIMEOUT).join();

        // Generate keys and index them by Hashes and assign offsets. Only half the keys exist; the others do not.
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        val offset = new AtomicLong();
        val hashes = new ArrayList<KeyHash>();
        val keysWithOffsets = new HashMap<KeyHash, KeyWithOffset>();
        for (val k : keys) {
            val hash = HASHER.hash(k.getKey());
            hashes.add(hash);
            boolean exists = hashes.size() % 2 == 0;
            if (exists) {
                keysWithOffsets.put(hash, new KeyWithOffset(new HashedArray(k.getKey()), offset.getAndAdd(k.getKey().getLength())));
            } else {
                keysWithOffsets.put(hash, null);
            }
        }

        // Update the keys in the segment (via their buckets).
        val buckets = iw.locateBuckets(keysWithOffsets.keySet(), context.segment, context.timer).join();
        val bucketUpdates = buckets.entrySet().stream()
                .map(e -> {
                    BucketUpdate bu = new BucketUpdate(e.getValue());
                    val ko = keysWithOffsets.get(e.getKey());
                    if (ko != null) {
                        bu.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, false));
                    }

                    return bu;
                })
                .collect(Collectors.toList());

        iw.updateBuckets(bucketUpdates, context.segment, 0L, 1L, TIMEOUT).join();

        // First lookup should go directly to the index. The cache should be empty.
        val result1 = context.index.getBucketOffsets(context.segment, hashes, context.timer).join();
        checkKeyOffsets(hashes, keysWithOffsets, result1);

        // Second lookup should be from the cache (for previous hits) and the rest from the index.
        val result2 = context.index.getBucketOffsets(context.segment, hashes, context.timer).join();
        checkKeyOffsets(hashes, keysWithOffsets, result2);
    }

    @Test
    public void testGetBackpointerOffsets() {

    }

    @Test
    public void testRecovery() {

    }

    private void checkKeyOffsets(List<KeyHash> allHashes, HashMap<KeyHash, KeyWithOffset> offsets, List<Long> bucketOffsets) {
        Assert.assertEquals("Unexpected number of results found.", allHashes.size(), bucketOffsets.size());
        for (int i = 0; i < allHashes.size(); i++) {
            KeyWithOffset ko = offsets.get(allHashes.get(i));
            long expectedValue = ko == null ? TableKey.NOT_EXISTS : ko.offset;
            Assert.assertEquals("Unexpected offset at index " + i, expectedValue, (long) bucketOffsets.get(i));
        }
    }

    private void checkPrevailingUpdate(List<UpdateItem> updates, TestContext context) {
        // The "surviving" update should be the one with the highest offsets for their corresponding persist Future..
        val highestUpdate = updates.stream().max(Comparator.comparingLong(u -> u.persist.join())).get();
        val highestUpdateHashes = highestUpdate.batch.getItems().stream().map(TableKeyBatch.Item::getHash).collect(Collectors.toList());
        val bucketOffsets = context.index.getBucketOffsets(context.segment, highestUpdateHashes, context.timer).join();
        Assert.assertEquals("Unexpected number of buckets returned.", highestUpdate.batch.getItems().size(), bucketOffsets.size());
        for (int i = 0; i < bucketOffsets.size(); i++) {
            long expectedOffset = highestUpdate.persist.join() + highestUpdate.batch.getItems().get(i).getOffset();
            long actualOffset = bucketOffsets.get(i);
            Assert.assertEquals("Unexpected offset for result index " + i, expectedOffset, actualOffset);
        }
    }

    private void completePersistArbitrarily(List<UpdateItem> updates, TestContext context) {
        val maxOffset = updates.size() * updates.stream().mapToInt(u -> u.batch.getLength()).max().getAsInt();
        for (val update : updates) {
            long offset = context.random.nextInt(maxOffset);
            update.offset.set(offset);
            update.persist.complete(offset);
        }
    }

    private List<TableKey> generateUnversionedKeys(int count, TestContext context) {
        val result = new ArrayList<TableKey>(count);
        for (int i = 0; i < count; i++) {
            byte[] keyData = new byte[Math.max(1, context.random.nextInt(100))];
            context.random.nextBytes(keyData);
            result.add(TableKey.unversioned(new ByteArraySegment(keyData)));
        }

        return result;
    }

    private TableKeyBatch toUpdateBatch(TableKey... keyLists) {
        return toUpdateBatch(Arrays.asList(keyLists));
    }

    @SafeVarargs
    private final TableKeyBatch toUpdateBatch(List<TableKey>... keyLists) {
        val batch = TableKeyBatch.update();
        for (val keyList : keyLists) {
            for (val key : keyList) {
                batch.add(key, HASHER.hash(key.getKey()), key.getKey().getLength());
            }
        }

        return batch;
    }

    private boolean keyMatches(ArrayView k1, ArrayView k2) {
        return new HashedArray(k1).equals(new HashedArray(k2));
    }

    //region Helper Classes

    @RequiredArgsConstructor
    private static class KeyWithOffset {
        final HashedArray key;
        final long offset;
    }

    @RequiredArgsConstructor
    private static class UpdateItem {
        final TableKeyBatch batch;
        final CompletableFuture<Long> persist;
        final CompletableFuture<List<Long>> update;
        final AtomicLong offset = new AtomicLong(-1);
    }

    private class TestContext implements AutoCloseable {
        final InMemoryCacheFactory cacheFactory;
        final CacheManager cacheManager;
        final SegmentMock segment;
        final ContainerKeyIndex index;
        final TimeoutTimer timer;
        final Random random;

        TestContext() {
            this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            this.segment = new SegmentMock(executorService());
            this.index = new ContainerKeyIndex(CONTAINER_ID, this.cacheFactory, this.cacheManager, executorService());
            this.timer = new TimeoutTimer(TIMEOUT);
            this.random = new Random(0);
        }

        @Override
        public void close() {
            this.index.close();
            this.cacheManager.close();
            this.cacheFactory.close();
        }
    }

    //endregion
}
