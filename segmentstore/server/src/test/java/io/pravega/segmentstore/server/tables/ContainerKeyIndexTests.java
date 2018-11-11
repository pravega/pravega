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

import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link ContainerKeyIndex} class.
 */
public class ContainerKeyIndexTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 0;
    private static final int BATCH_SIZE = 100;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final long SHORT_TIMEOUT_MILLIS = TIMEOUT.toMillis() / 3;
    private static final KeyHasher HASHER = KeyHashers.DEFAULT_HASHER;
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

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

        // Check result. We can only check bucket offsets and not backpointers due to the unpredictable nature of completing
        // Futures and invoking their callbacks in executors - we can only do that with conditional updates.
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
            val updateResult = u.update.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected number of buckets returned.", u.batch.getItems().size(), updateResult.size());
            for (int i = 0; i < updateResult.size(); i++) {
                long expectedOffset = u.persist.join() + u.batch.getItems().get(i).getOffset();
                long actualOffset = updateResult.get(i);
                Assert.assertEquals("Unexpected offset for result index " + i, expectedOffset, actualOffset);
            }
        }

        // Check final result.
        checkPrevailingUpdate(updates, context);

        // We can safely check backpointers here.
        checkBackpointers(updates, context);
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

        // Create the key. We must actually write something to the segment here as this will be used in the subsequent
        // calls to validate the key.
        val s = new EntrySerializer();
        val toUpdate = TableEntry.versioned(keyData, new ByteArraySegment(new byte[100]), TableKey.NOT_EXISTS);
        byte[] toWrite = new byte[s.getUpdateLength(toUpdate)];
        s.serializeUpdate(Collections.singleton(toUpdate), toWrite);
        context.index.update(context.segment,
                toUpdateBatch(toUpdate.getKey()),
                () -> context.segment.append(toWrite, null, TIMEOUT),
                context.timer).join();

        // Key exists, but we conditioned on it not existing.
        AssertExtensions.assertThrows(
                "update() allowed conditional update on existent key conditioned on not existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, TableKey.NOT_EXISTS)), noPersist, context.timer),
                ex -> ex instanceof BadKeyVersionException && keyMatches(((BadKeyVersionException) ex).getExpectedVersions(), keyData));

        // Key exists, but we conditioned on the wrong value.
        AssertExtensions.assertThrows(
                "update() allowed conditional update on inexistent key conditioned on existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, 123L)), noPersist, context.timer),
                ex -> ex instanceof BadKeyVersionException && keyMatches(((BadKeyVersionException) ex).getExpectedVersions(), keyData));
    }


    /**
     * Tests the ability of the {@link ContainerKeyIndex} to reconcile conditional updates if the condition does not match
     * the given Key's Bucket offset, but it matches the Key's offset (this is a corner scenario in a situation with multiple
     * Keys sharing the same bucket).
     */
    @Test
    public void testConditionalUpdateFailureReconciliation() {
        val hasher = KeyHashers.CONSTANT_HASHER;
        @Cleanup
        val context = new TestContext();

        Supplier<CompletableFuture<Long>> noPersist = () -> Futures.failedFuture(new AssertionError("Not expecting persist to be invoked."));
        val keys = generateUnversionedKeys(2, context);

        // First, write two keys with the same hash (and serialize them to the Segment).
        val versions = new HashMap<ArrayView, Long>();
        for (val key : keys) {
            val s = new EntrySerializer();
            val toUpdate = TableEntry.unversioned(key.getKey(), new ByteArraySegment(new byte[100]));
            byte[] toWrite = new byte[s.getUpdateLength(toUpdate)];
            s.serializeUpdate(Collections.singleton(toUpdate), toWrite);
            val r = context.index.update(context.segment,
                    toUpdateBatch(hasher, Collections.singletonList(toUpdate.getKey())),
                    () -> context.segment.append(toWrite, null, TIMEOUT),
                    context.timer).join();
            versions.put(key.getKey(), r.get(0));
        }

        // We want to remove the first key.
        val keyToRemove = keys.get(0);
        val validVersion = versions.get(keyToRemove.getKey());
        val invalidVersion = validVersion + 1;

        // Verify that a bad version won't work.
        AssertExtensions.assertThrows(
                "update() allowed conditional update with invalid version.",
                () -> context.index.update(context.segment,
                        toUpdateBatch(hasher, Collections.singletonList(TableKey.versioned(keyToRemove.getKey(), invalidVersion))),
                        noPersist,
                        context.timer),
                ex -> ex instanceof BadKeyVersionException);

        // Verify that the key's version (which is different than the bucket's version), works.
        AtomicBoolean persisted = new AtomicBoolean();
        context.index.update(context.segment,
                toUpdateBatch(hasher, Collections.singletonList(TableKey.versioned(keyToRemove.getKey(), validVersion))),
                () -> {
                    persisted.set(true);
                    return CompletableFuture.completedFuture(context.segment.getInfo().getLength());
                },
                context.timer).join();
        Assert.assertTrue("Expected persisted to have been invoked after reconciled conditional update.", persisted.get());
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
        val hashes = new ArrayList<UUID>();
        val keysWithOffsets = new HashMap<UUID, KeyWithOffset>();
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

    /**
     * Checks the ability for the {@link ContainerKeyIndex} class to properly handle recovery situations where the Table
     * Segment may not have been fully indexed when the first request for it is received.
     */
    @Test
    public void testRecovery() throws Exception {
        final int segmentLength = 123456;
        @Cleanup
        val context = new TestContext();

        // Setup the segment with initial attributes.
        val iw = new IndexWriter(HASHER, executorService());
        context.segment.updateAttributes(IndexWriter.generateInitialTableAttributes(), TIMEOUT).join();

        // Generate keys and index them by Hashes and assign offsets. Only half the keys exist; the others do not.
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        val offset = new AtomicLong();
        val hashes = new ArrayList<UUID>();
        val keysWithOffsets = new HashMap<UUID, KeyWithOffset>();
        for (val k : keys) {
            val hash = HASHER.hash(k.getKey());
            hashes.add(hash);
            keysWithOffsets.put(hash, new KeyWithOffset(new HashedArray(k.getKey()), offset.getAndAdd(k.getKey().getLength())));
        }

        // Update the keys in the segment (via their buckets).
        val buckets = iw.locateBuckets(keysWithOffsets.keySet(), context.segment, context.timer).join();
        val bucketUpdates = buckets.entrySet().stream()
                                   .map(e -> {
                                       BucketUpdate bu = new BucketUpdate(e.getValue());
                                       val ko = keysWithOffsets.get(e.getKey());
                                       bu.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, false));
                                       return bu;
                                   })
                                   .collect(Collectors.toList());

        // We leave the IndexOffset unchanged for now.
        iw.updateBuckets(bucketUpdates, context.segment, 0L, 0L, TIMEOUT).join();

        // Simulate writing the data to the segment by increasing its length.
        context.segment.append(new byte[segmentLength], null, TIMEOUT).join();

        // So far we haven't touched the container index, so it has no knowledge of all the keys that were just indexed.

        // 1. Verify getBucketOffsets(), getBackpointers() and conditional updates block on IndexOffset being up-to-date.
        val getBucketOffsets = context.index.getBucketOffsets(context.segment, hashes, context.timer);
        val backpointerKey = keysWithOffsets.values().stream().findFirst().get();
        val getBackpointers = context.index.getBackpointerOffset(context.segment, backpointerKey.offset, context.timer.getRemaining());
        val conditionalUpdateKey = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val conditionalUpdate = context.index.update(
                context.segment,
                toUpdateBatch(conditionalUpdateKey),
                () -> CompletableFuture.completedFuture(segmentLength + 1L),
                context.timer);
        Assert.assertFalse("Expected getBucketOffsets() to block.", getBucketOffsets.isDone());
        Assert.assertFalse("Expected getBackpointerOffset() to block.", getBackpointers.isDone());
        Assert.assertFalse("Expecting conditional update to block.", conditionalUpdate.isDone());

        // 2. Verify unconditional updates go through.
        val unconditionalUpdateKey = generateUnversionedKeys(1, context).get(0);
        val unconditionalUpdateResult = context.index.update(
                context.segment,
                toUpdateBatch(unconditionalUpdateKey),
                () -> CompletableFuture.completedFuture(segmentLength + 2L),
                context.timer).get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from the non-blocked unconditional update.",
                segmentLength + 2L, (long) unconditionalUpdateResult.get(0));

        // 3. Verify that all operations are unblocked when we reached the expected IndexOffset.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), segmentLength - 1);
        Assert.assertFalse("Not expecting anything to be unblocked at this point",
                getBucketOffsets.isDone() || getBackpointers.isDone() || conditionalUpdate.isDone());
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), segmentLength);
        val getBucketOffsetsResult = getBucketOffsets.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val getBackpointersResult = getBackpointers.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val conditionalUpdateResult = conditionalUpdate.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        checkKeyOffsets(hashes, keysWithOffsets, getBucketOffsetsResult);
        Assert.assertEquals("Unexpected result from unblocked getBackpointerOffset().", -1L, (long) getBackpointersResult);
        Assert.assertEquals("Unexpected result from unblocked conditional update.", segmentLength + 1L, (long) conditionalUpdateResult.get(0));

        // 4. Verify no new requests are blocked now.
        getBucketOffsets.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS); // A timeout check will suffice

        // 5. Verify requests are cancelled if we notify the segment has been removed.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        val cancelledKey = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val cancelledRequest = context.index.update(
                context.segment,
                toUpdateBatch(cancelledKey),
                () -> CompletableFuture.completedFuture(segmentLength + 3L),
                context.timer);
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        AssertExtensions.assertThrows(
                "Blocked request was not cancelled when a segment remove notification was received.",
                cancelledRequest,
                ex -> ex instanceof CancellationException);

        // 6. Verify requests are cancelled (properly) when we close the index.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        val cancelledKey2 = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val cancelledRequest2 = context.index.update(
                context.segment,
                toUpdateBatch(cancelledKey2),
                () -> CompletableFuture.completedFuture(segmentLength + 4L),
                context.timer);
        context.index.close();
        AssertExtensions.assertThrows(
                "Blocked request was not cancelled when a the index was closed.",
                cancelledRequest2,
                ex -> ex instanceof ObjectClosedException);
    }

    private void checkKeyOffsets(List<UUID> allHashes, Map<UUID, KeyWithOffset> offsets, Map<UUID, Long> bucketOffsets) {
        Assert.assertEquals("Unexpected number of results found.", allHashes.size(), bucketOffsets.size());
        for (int i = 0; i < allHashes.size(); i++) {
            UUID hash = allHashes.get(i);
            KeyWithOffset ko = offsets.get(hash);
            long expectedValue = ko == null ? TableKey.NOT_EXISTS : ko.offset;
            Assert.assertEquals("Unexpected offset at index " + i, expectedValue, (long) bucketOffsets.get(hash));
        }
    }

    private void checkPrevailingUpdate(List<UpdateItem> updates, TestContext context) {
        // The "surviving" update should be the one with the highest offsets for their corresponding persist Future.
        val sortedUpdates = updates.stream().sorted(Comparator.comparingLong(u -> u.offset.get())).collect(Collectors.toList());
        val highestUpdate = sortedUpdates.get(sortedUpdates.size() - 1);
        val highestUpdateHashes = highestUpdate.batch.getItems().stream().map(TableKeyBatch.Item::getHash).collect(Collectors.toList());

        // Check Bucket offsets.
        val bucketOffsets = context.index.getBucketOffsets(context.segment, highestUpdateHashes, context.timer).join();
        Assert.assertEquals("Unexpected number of buckets returned.", highestUpdate.batch.getItems().size(), bucketOffsets.size());

        val expectedOffsetsByHash = highestUpdate.batch.getItems().stream()
                .collect(Collectors.toMap(TableKeyBatch.Item::getHash, TableKeyBatch.Item::getOffset));
        for (val e : bucketOffsets.entrySet()) {
            long expectedOffset = highestUpdate.offset.get() + expectedOffsetsByHash.get(e.getKey());
            long actualOffset = e.getValue();
            Assert.assertEquals("Unexpected offset.", expectedOffset, actualOffset);
        }
    }

    private void checkBackpointers(List<UpdateItem> updates, TestContext context) {
        val sortedUpdates = updates.stream().sorted(Comparator.comparingLong(u -> u.offset.get())).collect(Collectors.toList());
        val highestUpdateHashes = sortedUpdates.get(sortedUpdates.size() - 1).batch
                .getItems().stream().map(TableKeyBatch.Item::getHash).collect(Collectors.toList());

        Map<UUID, Long> backpointerSources = context.index.getBucketOffsets(context.segment, highestUpdateHashes, context.timer).join();
        for (int updateId = sortedUpdates.size() - 1; updateId >= 0; updateId--) {
            // Generate the expected backpointers.
            Map<UUID, Long> expectedBackpointers = new HashMap<>();
            if (updateId == 0) {
                // For the first update, we do not expect any.
                backpointerSources.keySet().forEach(k -> expectedBackpointers.put(k, -1L));
            } else {
                // For any other updates, it's whatever got executed before this one.
                val previousUpdate = sortedUpdates.get(updateId - 1);
                for (int i = 0; i < previousUpdate.batch.getItems().size(); i++) {
                    expectedBackpointers.put(
                            previousUpdate.batch.getItems().get(i).getHash(),
                            previousUpdate.update.join().get(i));
                }
            }

            // Fetch the actual values.
            Map<UUID, Long> actualBackpointers = backpointerSources
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, e -> context.index.getBackpointerOffset(context.segment, e.getValue(), TIMEOUT).join()));

            // Check and move on.
            Assert.assertEquals("Unexpected backpointer count for update " + updateId, expectedBackpointers.size(), actualBackpointers.size());
            for (val e : expectedBackpointers.entrySet()) {
                val a = actualBackpointers.get(e.getKey());
                Assert.assertNotNull("No backpointer for update "+updateId, a);
                Assert.assertEquals("Unexpected backpointer for update " + updateId, e.getValue(), a);
            }

            backpointerSources = expectedBackpointers;
        }

        // Check Backpointers.
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
        return toUpdateBatch(HASHER, keyLists);
    }

    @SafeVarargs
    private final TableKeyBatch toUpdateBatch(KeyHasher hasher, List<TableKey>... keyLists) {
        val batch = TableKeyBatch.update();
        for (val keyList : keyLists) {
            for (val key : keyList) {
                batch.add(key, hasher.hash(key.getKey()), key.getKey().getLength());
            }
        }

        return batch;
    }

    private boolean keyMatches(ArrayView k1, ArrayView k2) {
        return new HashedArray(k1).equals(new HashedArray(k2));
    }

    private boolean keyMatches(Map<TableKey, Long> expectedVersions, ArrayView k2) {
        return expectedVersions.size() == 1 && keyMatches(expectedVersions.keySet().stream().findFirst().get().getKey(), k2);
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
