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

import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.TableStoreMock;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.NonNull;
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
    private static final int TEST_MAX_TAIL_CACHE_PRE_INDEX_LENGTH = 128 * 1024;
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(2);
    private static final Comparator<BufferView> KEY_COMPARATOR = BufferViewComparator.create()::compare;
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
        AssertExtensions.assertSuppliedFutureThrows(
                "update() allowed conditional update on inexistent key conditioned on existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, 0L)), noPersist, context.timer),
                ex -> ex instanceof KeyNotExistsException && ((KeyNotExistsException) ex).getKey().equals(keyData));

        // Create the key. We must actually write something to the segment here as this will be used in the subsequent
        // calls to validate the key.
        val s = new EntrySerializer();
        val toUpdate = TableEntry.versioned(keyData, new ByteArraySegment(new byte[100]), TableKey.NOT_EXISTS);
        val toWrite = s.serializeUpdate(Collections.singleton(toUpdate));
        context.index.update(context.segment,
                toUpdateBatch(toUpdate.getKey()),
                () -> context.segment.append(toWrite, null, TIMEOUT),
                context.timer).join();

        // Key exists, but we conditioned on it not existing.
        AssertExtensions.assertSuppliedFutureThrows(
                "update() allowed conditional update on existent key conditioned on not existing.",
                () -> context.index.update(context.segment, toUpdateBatch(TableKey.versioned(keyData, TableKey.NOT_EXISTS)), noPersist, context.timer),
                ex -> ex instanceof BadKeyVersionException && keyMatches(((BadKeyVersionException) ex).getExpectedVersions(), keyData));

        // Key exists, but we conditioned on the wrong value.
        AssertExtensions.assertSuppliedFutureThrows(
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
        val versions = new HashMap<BufferView, Long>();
        for (val key : keys) {
            val s = new EntrySerializer();
            val toUpdate = TableEntry.unversioned(key.getKey(), new ByteArraySegment(new byte[100]));
            val toWrite = s.serializeUpdate(Collections.singleton(toUpdate));
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
        AssertExtensions.assertSuppliedFutureThrows(
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
                keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), offset.getAndAdd(k.getKey().getLength())));
            } else {
                keysWithOffsets.put(hash, null);
            }
        }

        // Update the keys in the segment (via their buckets).
        val buckets = iw.locateBuckets(context.segment, keysWithOffsets.keySet(), context.timer).join();
        Collection<BucketUpdate> bucketUpdates = buckets.entrySet().stream()
                .map(e -> {
                    val builder = BucketUpdate.forBucket(e.getValue());
                    val ko = keysWithOffsets.get(e.getKey());
                    if (ko != null) {
                        builder.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, ko.offset, false));
                    }

                    return builder.build();
                })
                .collect(Collectors.toList());

        iw.updateBuckets(context.segment, bucketUpdates, 0L, 1L, 0, TIMEOUT).join();

        // First lookup should go directly to the index. The cache should be empty.
        val result1 = context.index.getBucketOffsets(context.segment, hashes, context.timer).join();
        checkKeyOffsets(hashes, keysWithOffsets, result1);

        // Second lookup should be from the cache (for previous hits) and the rest from the index.
        val result2 = context.index.getBucketOffsets(context.segment, hashes, context.timer).join();
        checkKeyOffsets(hashes, keysWithOffsets, result2);
    }

    /**
     * Tests the {@link ContainerKeyIndex#getBucketOffsetDirect} method.
     */
    @Test
    public void testGetBucketOffsetDirect() {
        final long updateBatchLength = 100000;
        final long noCacheOffset = updateBatchLength;
        final long lowerCacheOffset = noCacheOffset + updateBatchLength;
        final long higherCacheOffset = lowerCacheOffset + updateBatchLength;
        @Cleanup
        val context = new TestContext();

        // Setup the segment with initial attributes.
        val iw = new IndexWriter(HASHER, executorService());

        // Generate keys.
        // First 1/3 of the keys do not exist in the cache.
        // Second 1/3 of the keys exist in the cache, but have an offset lower than in the Index.
        // Last 1/3 of the keys exist in the cache and have an offset higher than in the Index.
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        val keysWithOffsets = new HashMap<UUID, KeyWithOffset>();
        val noCacheKeys = new ArrayList<TableKey>();
        val lowerCacheOffsetKeys = new ArrayList<TableKey>();
        val higherCacheOffsetKeys = new ArrayList<TableKey>();
        for (int i = 0; i < keys.size(); i++) {
            val k = keys.get(i);
            val hash = HASHER.hash(k.getKey());
            if (i < keys.size() / 3) {
                // Does not exist in the cache.
                noCacheKeys.add(k);
                keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), noCacheOffset));
            } else if (i < keys.size() * 2 / 3) {
                // Exists in the cache, but with a lower offset than in the index.
                lowerCacheOffsetKeys.add(k);
                keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), lowerCacheOffset));
            } else {
                // Exists in the cache with a higher offset than in the index.
                higherCacheOffsetKeys.add(k);
                keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), higherCacheOffset));
            }
        }

        // Update everything in the underlying index.
        val buckets = iw.locateBuckets(context.segment, keysWithOffsets.keySet(), context.timer).join();
        Collection<BucketUpdate> bucketUpdates = buckets.entrySet().stream()
                                   .map(e -> {
                                       val builder = BucketUpdate.forBucket(e.getValue());
                                       val ko = keysWithOffsets.get(e.getKey());
                                       builder.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, ko.offset, false));
                                       return builder.build();
                                   })
                                   .collect(Collectors.toList());
        iw.updateBuckets(context.segment, bucketUpdates, 0L, 1L, 0, TIMEOUT).join();

        // Update cache, and immediately clear out the tail section as we want to simulate a case where the values are already
        // thought to be indexed already.
        context.index.update(context.segment, toUpdateBatch(lowerCacheOffsetKeys),
                () -> CompletableFuture.completedFuture(lowerCacheOffset - updateBatchLength), context.timer).join();
        context.index.update(context.segment, toUpdateBatch(higherCacheOffsetKeys),
                () -> CompletableFuture.completedFuture(higherCacheOffset + BATCH_SIZE), context.timer).join();
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), higherCacheOffset + updateBatchLength);

        // Check results. The expected offsets should already be stored in keysWithOffsets.
        for (val k : keys) {
            val hash = HASHER.hash(k.getKey());
            val actualOffset = context.index.getBucketOffsetDirect(context.segment, hash, context.timer).join();
            val expectedOffset = keysWithOffsets.get(hash).offset;
            Assert.assertEquals("Unexpected result from getBucketOffsetDirect.", expectedOffset, (long) actualOffset);

            val cachedOffset = context.index.getBucketOffsets(context.segment, Collections.singleton(hash), context.timer).join().get(hash);
            Assert.assertEquals("Unexpected result from getBucketOffsets.", expectedOffset, (long) cachedOffset);
        }
    }

    /**
     * Checks the ability for the {@link ContainerKeyIndex} class to properly handle recovery situations where the Table
     * Segment may not have been fully indexed when the first request for it is received.
     */
    @Test
    public void testRecovery() throws Exception {
        val s = new EntrySerializer();
        @Cleanup
        val context = new TestContext();

        // Setup the segment with initial attributes.
        val iw = new IndexWriter(HASHER, executorService());

        // 1. Generate initial set of keys and serialize them to the segment.
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        val entries1 = new ArrayList<TableEntry>(keys.size());
        val offset = new AtomicLong();
        val hashes = new ArrayList<UUID>();
        val keysWithOffsets = new HashMap<UUID, KeyWithOffset>();
        for (val k : keys) {
            val hash = HASHER.hash(k.getKey());
            hashes.add(hash);
            byte[] valueData = new byte[Math.max(1, context.random.nextInt(100))];
            context.random.nextBytes(valueData);
            val entry = TableEntry.unversioned(k.getKey(), new ByteArraySegment(valueData));
            keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), offset.getAndAdd(s.getUpdateLength(entry))));
            entries1.add(entry);
        }
        val update1 = s.serializeUpdate(entries1);
        Assert.assertEquals(offset.get(), update1.getLength());
        context.segment.append(update1, null, TIMEOUT).join();

        // 2. Initiate a recovery and verify pre-caching is triggered and requests are auto-unblocked.
        val get1 = context.index.getBucketOffsets(context.segment, hashes, context.timer);
        val result1 = get1.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val expected1 = new HashMap<UUID, Long>();
        keysWithOffsets.forEach((k, o) -> expected1.put(k, o.offset));
        AssertExtensions.assertMapEquals("Unexpected result from getBucketOffsets() after auto pre-caching.", expected1, result1);
        checkSortedKeys(keys.stream().map(TableKey::getKey).collect(Collectors.toList()), context);

        // 3. Set LastIdx to Length, and increase by TEST_MAX_TAIL_CACHE_PRE_INDEX_LENGTH + 1 (so we don't do pre-caching).
        val buckets = iw.locateBuckets(context.segment, keysWithOffsets.keySet(), context.timer).join();
        Collection<BucketUpdate> bucketUpdates = buckets.entrySet().stream()
                .map(e -> {
                    val builder = BucketUpdate.forBucket(e.getValue());
                    val ko = keysWithOffsets.get(e.getKey());
                    builder.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, ko.offset, false));
                    return builder.build();
                })
                .collect(Collectors.toList());
        iw.updateBuckets(context.segment, bucketUpdates, 0L, offset.get(), keysWithOffsets.size(), TIMEOUT).join();
        context.segment.append(new ByteArraySegment(new byte[TEST_MAX_TAIL_CACHE_PRE_INDEX_LENGTH + 1]), null, TIMEOUT).join();

        // 4. Verify pre-caching is disabled and that the requests are blocked.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1); // Force-evict it so we start clean.
        val getBucketOffsets = context.index.getBucketOffsets(context.segment, hashes, context.timer);
        val backpointerKey = keysWithOffsets.values().stream().findFirst().get();
        val getBackpointers = context.index.getBackpointerOffset(context.segment, backpointerKey.offset, context.timer.getRemaining());
        val getUnindexedKeys = context.index.getUnindexedKeyHashes(context.segment);
        val conditionalUpdateKey = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val conditionalUpdate = context.index.update(
                context.segment,
                toUpdateBatch(conditionalUpdateKey),
                () -> CompletableFuture.completedFuture(context.segment.getInfo().getLength() + 1L),
                context.timer);
        val sortedIndexRequest = context.index.getSortedKeyIndex(context.segment);
        Assert.assertFalse("Expected getBucketOffsets() to block.", getBucketOffsets.isDone());
        Assert.assertFalse("Expected getBackpointerOffset() to block.", getBackpointers.isDone());
        Assert.assertFalse("Expecting conditional update to block.", conditionalUpdate.isDone());
        Assert.assertFalse("Expecting getSortedKeyIndex() to block.", sortedIndexRequest.isDone());

        // 4.1. Verify unconditional updates go through.
        val unconditionalUpdateKey = generateUnversionedKeys(1, context).get(0);
        val unconditionalUpdateResult = context.index.update(
                context.segment,
                toUpdateBatch(unconditionalUpdateKey),
                () -> CompletableFuture.completedFuture(context.segment.getInfo().getLength() + 2L),
                context.timer).get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from the non-blocked unconditional update.",
                context.segment.getInfo().getLength() + 2L, (long) unconditionalUpdateResult.get(0));

        // 3. Verify that all operations are unblocked when we reached the expected IndexOffset.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), context.segment.getInfo().getLength() - 1);
        Assert.assertFalse("Not expecting anything to be unblocked at this point",
                getBucketOffsets.isDone() || getBackpointers.isDone() || conditionalUpdate.isDone() || getUnindexedKeys.isDone());
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), context.segment.getInfo().getLength());
        val getBucketOffsetsResult = getBucketOffsets.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val getBackpointersResult = getBackpointers.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val conditionalUpdateResult = conditionalUpdate.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val getUnindexedKeysResult = getUnindexedKeys.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        checkKeyOffsets(hashes, keysWithOffsets, getBucketOffsetsResult);
        Assert.assertEquals("Unexpected result from unblocked getBackpointerOffset().", -1L, (long) getBackpointersResult);
        Assert.assertEquals("Unexpected result from unblocked conditional update.",
                context.segment.getInfo().getLength() + 1L, (long) conditionalUpdateResult.get(0));

        // Depending on the order in which the internal recovery tracker (implemented by CompletableFuture.thenCompose)
        // executes its callbacks, the result of this call may be either 1 or 2 (it may unblock prior to the conditional
        // update unblocking or the other way around).
        Assert.assertTrue("Unexpected result size from unblocked getUnindexedKeyHashes().",
                getUnindexedKeysResult.size() == 1 || getUnindexedKeysResult.size() == 2);

        // However, verify that in the end, we have 2 unindexed keys.
        val finalGetUnindexedKeysResult = context.index.getUnindexedKeyHashes(context.segment).get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result size from final getUnindexedKeyHashes().", 2, finalGetUnindexedKeysResult.size());

        // .. and the same 2 keys in the sorted index. Since we haven't done any actual background indexing, the sorted
        // index should have been cleared up when notifyIndexOffsetChanged was invoked, leaving only these 2 keys around.
        checkSortedKeys(Arrays.asList(unconditionalUpdateKey.getKey(), conditionalUpdateKey.getKey()), context);

        // 5. Verify no new requests are blocked now.
        getBucketOffsets.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS); // A timeout check will suffice

        // 6. Verify requests are cancelled if we notify the segment has been removed.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        val cancelledKey = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val cancelledRequest = context.index.update(
                context.segment,
                toUpdateBatch(cancelledKey),
                () -> CompletableFuture.completedFuture(context.segment.getInfo().getLength() + 3L),
                context.timer);
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        AssertExtensions.assertFutureThrows(
                "Blocked request was not cancelled when a segment remove notification was received.",
                cancelledRequest,
                ex -> ex instanceof CancellationException);

        // 6. Verify requests are cancelled (properly) when we close the index.
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), -1L);
        val cancelledKey2 = TableKey.notExists(generateUnversionedKeys(1, context).get(0).getKey());
        val cancelledRequest2 = context.index.update(
                context.segment,
                toUpdateBatch(cancelledKey2),
                () -> CompletableFuture.completedFuture(context.segment.getInfo().getLength() + 4L),
                context.timer);
        context.index.close();
        AssertExtensions.assertFutureThrows(
                "Blocked request was not cancelled when a the index was closed.",
                cancelledRequest2,
                ex -> ex instanceof ObjectClosedException);
    }

    /**
     * Checks the ability for the {@link ContainerKeyIndex} class to cancel recovery-bound tasks if recovery took too long.
     */
    @Test
    public void testRecoveryTimeout() throws Exception {
        val s = new EntrySerializer();
        @Cleanup
        val context = new TestContext();

        // Setup the segment with initial attributes.
        val iw = new IndexWriter(HASHER, executorService());

        // Generate initial set of keys.
        val keys = generateUnversionedKeys(BATCH_SIZE, context);
        long offset = 0;
        val hashes = new ArrayList<UUID>();
        val keysWithOffsets = new HashMap<UUID, KeyWithOffset>();
        for (val k : keys) {
            val hash = HASHER.hash(k.getKey());
            hashes.add(hash);
            keysWithOffsets.put(hash, new KeyWithOffset(k.getKey(), offset));
            offset += k.getKey().getLength();
        }

        // Write some garbage data to the segment, but make it longer than the threshold to trigger pre-caching; we don't
        // want to deal with that now since we can't control its runtime.
        context.segment.append(new ByteArraySegment(new byte[TEST_MAX_TAIL_CACHE_PRE_INDEX_LENGTH + 1]), null, TIMEOUT).join();

        // Update the index, but keep the LastIndexedOffset at 0.
        val buckets = iw.locateBuckets(context.segment, keysWithOffsets.keySet(), context.timer).join();
        Collection<BucketUpdate> bucketUpdates = buckets.entrySet().stream()
                                                        .map(e -> {
                                                            val builder = BucketUpdate.forBucket(e.getValue());
                                                            val ko = keysWithOffsets.get(e.getKey());
                                                            builder.withKeyUpdate(new BucketUpdate.KeyUpdate(ko.key, ko.offset, ko.offset, false));
                                                            return builder.build();
                                                        })
                                                        .collect(Collectors.toList());
        iw.updateBuckets(context.segment, bucketUpdates, 0L, 0, keysWithOffsets.size(), TIMEOUT).join();

        // Issue a request and verify it times out.
        AssertExtensions.assertSuppliedFutureThrows(
                "Request did not fail when recovery timed out.",
                () -> context.index.getBucketOffsets(context.segment, hashes, context.timer),
                ex -> ex instanceof TimeoutException);

        // Verify that a new operation will be unblocked if we notify that the recovery completed successfully.
        val get1 = context.index.getBucketOffsets(context.segment, hashes, context.timer);
        context.index.notifyIndexOffsetChanged(context.segment.getSegmentId(), context.segment.getInfo().getLength());
        val result1 = get1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val expected1 = new HashMap<UUID, Long>();
        keysWithOffsets.forEach((k, o) -> expected1.put(k, o.offset));
        AssertExtensions.assertMapEquals("Unexpected result from getBucketOffsets() after a retry.", expected1, result1);
    }

    /**
     * Checks the functionality of the {@link ContainerKeyIndex#executeIfEmpty} method.
     */
    @Test
    public void testExecuteIfEmpty() throws Exception {
        @Cleanup
        val context = new TestContext();
        val unversionedKeys = generateUnversionedKeys(BATCH_SIZE, context);

        // 1. Verify executeIfEmpty works on an empty segment. Also verifies it blocks a conditional update.
        val toRun1 = new CompletableFuture<Void>();
        val persist1 = new CompletableFuture<Long>();
        val empty1 = context.index.executeIfEmpty(context.segment, () -> toRun1, context.timer);
        val updateBatch = toUpdateBatch(unversionedKeys.stream().map(k -> TableKey.notExists(k.getKey())).collect(Collectors.toList()));
        val update1 = empty1.thenCompose(v -> {
            Assert.assertTrue("Conditional update not blocked by executeIfEmpty.", toRun1.isDone());
            return context.index.update(context.segment, updateBatch, () -> persist1, context.timer);
        });
        Assert.assertFalse("Not expecting any task to be done yet.", empty1.isDone() || update1.isDone());

        // Verify that conditional updates are properly blocked.
        toRun1.complete(null);
        empty1.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertFalse("Not expecting first update to complete.", update1.isDone());
        persist1.complete(1L);
        update1.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // 2. After an update.
        AssertExtensions.assertSuppliedFutureThrows(
                "executeIfEmpty should not run if table is not empty.",
                () -> context.index.executeIfEmpty(context.segment, () -> Futures.failedFuture(new AssertionError("This should not run")), context.timer),
                ex -> ex instanceof TableSegmentNotEmptyException);

        // 3. After a removal.
        val removeKeys = new ArrayList<TableKey>();
        val removeVersions = update1.join();
        for (int i = 0; i < unversionedKeys.size(); i++) {
            removeKeys.add(TableKey.versioned(unversionedKeys.get(i).getKey(), removeVersions.get(i)));
        }

        val removeBatch = toRemoveBatch(removeKeys);
        val persist2 = new CompletableFuture<Long>();
        val update2 = context.index.update(context.segment, removeBatch, () -> persist2, context.timer);
        val empty2 = update2.thenCompose(v -> context.index.executeIfEmpty(context.segment, () -> {
            Assert.assertTrue("executeIfEmpty should not execute prior to call to update()..", persist2.isDone());
            return CompletableFuture.completedFuture(null);
        }, context.timer));

        // Verify that executeIfEmpty is blocked on a conditional update.
        persist2.complete(2L);
        update2.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        empty2.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
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

        // Check sorted index.
        val keys = highestUpdate.batch.getItems().stream().map(i -> i.getKey().getKey()).collect(Collectors.toList());
        checkSortedKeys(keys, context);
    }

    private void checkSortedKeys(List<BufferView> expectedSortedKeys, TestContext context) {
        expectedSortedKeys = expectedSortedKeys.stream().sorted(KEY_COMPARATOR).collect(Collectors.toList());
        val actualSortedKeys = new ArrayList<BufferView>();
        val si = context.index.getSortedKeyIndex(context.segment).join();
        si.iterator(si.getIteratorRange(null, null), TIMEOUT).forEachRemaining(actualSortedKeys::addAll, executorService()).join();
        AssertExtensions.assertListEquals("Unexpected keys returned by getSortedKeyIndex().iterator.",
                expectedSortedKeys, actualSortedKeys, BufferView::equals);
    }

    private void checkBackpointers(List<UpdateItem> updates, TestContext context) {
        val sortedUpdates = updates.stream()
                .sorted(Comparator.comparingLong(u -> u.offset.get()))
                .collect(Collectors.toList());
        val highestUpdate = sortedUpdates.get(sortedUpdates.size() - 1);
        val highestUpdateHashes = highestUpdate.batch.getItems().stream()
                .map(TableKeyBatch.Item::getHash)
                .collect(Collectors.toList());

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
                Assert.assertNotNull("No backpointer for update " + updateId, a);
                Assert.assertEquals("Unexpected backpointer for update " + updateId, e.getValue(), a);
            }

            backpointerSources = expectedBackpointers;
        }

        // Check unindexed key hashes.
        val unindexedHashes = context.index.getUnindexedKeyHashes(context.segment).join().entrySet().stream()
                                           .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getSegmentOffset()));
        val expectedHashes = highestUpdate.batch.getItems().stream()
                                                .collect(Collectors.toMap(TableKeyBatch.Item::getHash, i -> highestUpdate.offset.get() + i.getOffset()));
        AssertExtensions.assertMapEquals("Unexpected result from getUnindexedKeyHashes", expectedHashes, unindexedHashes);
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

    private TableKeyBatch toRemoveBatch(List<TableKey> keyList) {
        val batch = TableKeyBatch.removal();
        for (val key : keyList) {
            batch.add(key, HASHER.hash(key.getKey()), key.getKey().getLength());
        }

        return batch;
    }

    private boolean keyMatches(Map<TableKey, Long> expectedVersions, BufferView k2) {
        return expectedVersions.size() == 1 && expectedVersions.keySet().stream().findFirst().get().getKey().equals(k2);
    }

    //region Helper Classes

    @RequiredArgsConstructor
    private static class KeyWithOffset {
        final BufferView key;
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
        final CacheStorage cacheStorage;
        final CacheManager cacheManager;
        final SegmentMock segment;
        final TableStoreMock sortedKeyStorage;
        final ContainerSortedKeyIndex sortedKeyIndex;
        final ContainerKeyIndex index;
        final TimeoutTimer timer;
        final Random random;

        TestContext() {
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, this.cacheStorage, executorService());
            this.segment = new SegmentMock(executorService());
            this.segment.updateAttributes(TableAttributes.DEFAULT_VALUES);
            // Sorted Table Segments are an extension of regular Table Segments. By setting the SORTED flag, we will end
            // up testing the additional code for it and also all the base code for regular Table Segments.
            this.segment.updateAttributes(Collections.singletonMap(TableAttributes.SORTED, Attributes.BOOLEAN_TRUE));
            this.sortedKeyStorage = new TableStoreMock(executorService());
            this.sortedKeyStorage.createSegment(this.segment.getInfo().getName(), SegmentType.TABLE_SEGMENT_HASH, TIMEOUT).join();
            val ds = new SortedKeyIndexDataSource(this.sortedKeyStorage::put, this.sortedKeyStorage::remove, this.sortedKeyStorage::get);
            this.sortedKeyIndex = new ContainerSortedKeyIndex(ds, executorService());
            this.index = new TestContainerKeyIndex(CONTAINER_ID, this.cacheManager, this.sortedKeyIndex, KeyHashers.DEFAULT_HASHER, executorService());
            this.timer = new TimeoutTimer(TIMEOUT);
            this.random = new Random(0);
        }

        @Override
        public void close() {
            this.index.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }

        private class TestContainerKeyIndex extends ContainerKeyIndex {
            TestContainerKeyIndex(int containerId, @NonNull CacheManager cacheManager, @NonNull ContainerSortedKeyIndex sortedKeyIndex,
                                  @NonNull KeyHasher keyHasher, @NonNull ScheduledExecutorService executor) {
                super(containerId, cacheManager, sortedKeyIndex, keyHasher, executor);
            }

            @Override
            protected long getMaxTailCachePreIndexLength() {
                return TEST_MAX_TAIL_CACHE_PRE_INDEX_LENGTH;
            }

            @Override
            protected Duration getRecoveryTimeout() {
                return ContainerKeyIndexTests.RECOVERY_TIMEOUT;
            }
        }
    }

    //endregion
}
