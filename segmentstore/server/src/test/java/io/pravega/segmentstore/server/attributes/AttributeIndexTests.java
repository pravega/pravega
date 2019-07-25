/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.TestCacheManager;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

public class AttributeIndexTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 9999;
    private static final String SEGMENT_NAME = "Segment";
    private static final long SEGMENT_ID = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(10000);
    private static final AttributeIndexConfig DEFAULT_CONFIG = AttributeIndexConfig
            .builder()
            .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 4 * 1024)
            .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 16 * 1024)
            .build();

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the ability to record Attribute values successively by updating one at a time..
     */
    @Test
    public void testSingleUpdates() {
        testRegularOperations(1000, 1, 5, DEFAULT_CONFIG);
    }

    /**
     * Tests the ability to record Attribute values successively with larger update batches.
     */
    @Test
    public void testBatchedUpdates() {
        testRegularOperations(1000, 50, 5, DEFAULT_CONFIG);
    }

    /**
     * Tests the ability to iterate through a certain range of attributes in the Index.
     */
    @Test
    public void testIterator() {
        final int count = 2000;
        final int checkSkipCount = 10;
        val attributes = IntStream.range(-count / 2, count / 2).mapToObj(i -> new UUID(i, -i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (UUID attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
        }

        idx.update(expectedValues, TIMEOUT).join();

        // Check iterator.
        // Sort the keys using natural order (UUID comparator).
        val sortedKeys = expectedValues.keySet().stream().sorted().collect(Collectors.toList());

        // Add some values outside of the bounds.
        sortedKeys.add(0, new UUID(Long.MIN_VALUE, Long.MIN_VALUE));
        sortedKeys.add(new UUID(Long.MAX_VALUE, Long.MAX_VALUE));

        // Check various combinations.
        for (int startIndex = 0; startIndex < sortedKeys.size() / 2; startIndex += checkSkipCount) {
            int lastIndex = sortedKeys.size() - startIndex - 1;
            val fromId = sortedKeys.get(startIndex);
            val toId = sortedKeys.get(lastIndex);

            // The expected keys are all the Keys from the start index to the end index, excluding the outside-the-bounds values.
            val expectedIterator = sortedKeys.subList(Math.max(1, startIndex), Math.min(sortedKeys.size() - 1, lastIndex + 1)).iterator();
            val iterator = idx.iterator(fromId, toId, TIMEOUT);
            iterator.forEachRemaining(batch -> batch.forEach(attribute -> {
                Assert.assertTrue("Not expecting any more attributes in the iteration.", expectedIterator.hasNext());
                val expectedId = expectedIterator.next();
                val expectedValue = expectedValues.get(expectedId);
                Assert.assertEquals("Unexpected Id.", expectedId, attribute.getKey());
                Assert.assertEquals("Unexpected value for " + expectedId, expectedValue, attribute.getValue());

            }), executorService()).join();

            // Verify there are no more attributes that we are expecting.
            Assert.assertFalse("Not all expected attributes were returned.", expectedIterator.hasNext());
        }
    }

    /**
     * Tests the ability to Seal an Attribute Segment (create a final snapshot and disallow new changes).
     */
    @Test
    public void testSeal() {
        int attributeCount = 1000;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        val config = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, DEFAULT_CONFIG.getMaxIndexPageSize())
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 10)
                .build();

        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (UUID attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
        }
        idx.update(expectedValues, TIMEOUT).join();

        // Check index before sealing.
        checkIndex(idx, expectedValues);

        // Seal twice (to check idempotence).
        idx.seal(TIMEOUT).join();
        idx.seal(TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Index allowed adding new values after being sealed.",
                () -> idx.update(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);

        // Check index again, after sealing.
        checkIndex(idx, expectedValues);
    }

    /**
     * Tests the ability to handle an already-sealed index.
     */
    @Test
    public void testAlreadySealedIndex() {
        val config = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, DEFAULT_CONFIG.getMaxIndexPageSize())
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 10)
                .build();

        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        context.storage.create(StreamSegmentNameUtils.getAttributeSegmentName(SEGMENT_NAME), TIMEOUT)
                       .thenCompose(handle -> context.storage.seal(handle, TIMEOUT));
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Index allowed adding new values when sealed.",
                () -> idx.update(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);

        idx.seal(TIMEOUT).join();
    }

    /**
     * Tests the ability to delete all AttributeData for a particular Segment.
     */
    @Test
    public void testDelete() {
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // 1. Initialize a new index and write one entry to it - that will ensure it gets created.
        val sm = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx.update(Collections.singletonMap(UUID.randomUUID(), 0L), TIMEOUT).join();

        // We intentionally delete twice to make sure the operation is idempotent.
        context.index.delete(sm.getName(), TIMEOUT).join();
        context.index.delete(sm.getName(), TIMEOUT).join();

        AssertExtensions.assertSuppliedFutureThrows(
                "update() worked after delete().",
                () -> idx.update(Collections.singletonMap(UUID.randomUUID(), 0L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertSuppliedFutureThrows(
                "seal() worked after delete().",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Check index after deleting.
        checkIndex(idx, Collections.emptyMap());

        // ... and after cleaning up the cache.
        idx.removeAllCacheEntries();
        checkIndex(idx, Collections.emptyMap());
        Assert.assertFalse("Not expecting Attribute Segment to be recreated.",
                context.storage.exists(StreamSegmentNameUtils.getAttributeSegmentName(SEGMENT_NAME), TIMEOUT).join());
    }

    /**
     * Tests the case when Snapshots fail due to a Storage failure. This should prevent whatever operation triggered it
     * to completely fail and not record the data.
     */
    @Test
    public void testStorageFailure() {
        val attributeId = UUID.randomUUID();
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // 1. When writing normally
        context.storage.writeInterceptor = (streamSegmentName, offset, data, length, wrappedStorage) -> {
            throw new IntentionalException();
        };
        AssertExtensions.assertSuppliedFutureThrows(
                "update() worked with Storage failure.",
                () -> idx.update(Collections.singletonMap(attributeId, 0L), TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("A value was retrieved after a failed update().", 0, idx.get(Collections.singleton(attributeId), TIMEOUT).join().size());

        // 2. When Sealing.
        context.storage.sealInterceptor = (streamSegmentName, wrappedStorage) -> {
            throw new IntentionalException();
        };
        AssertExtensions.assertSuppliedFutureThrows(
                "seal() worked with Storage failure.",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof IntentionalException);
    }

    /**
     * Verifies we cannot create indices for Deleted Segments.
     */
    @Test
    public void testSegmentNotExists() {
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // Verify we cannot create new indices for deleted segments.
        val deletedSegment = context.containerMetadata.mapStreamSegmentId(SEGMENT_NAME + "deleted", SEGMENT_ID + 1);
        deletedSegment.setLength(0);
        deletedSegment.setStorageLength(0);
        deletedSegment.markDeleted();
        AssertExtensions.assertSuppliedFutureThrows(
                "forSegment() worked on deleted segment.",
                () -> context.index.forSegment(deletedSegment.getId(), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
        Assert.assertFalse("Attribute segment was created in Storage for a deleted Segment..",
                context.storage.exists(StreamSegmentNameUtils.getAttributeSegmentName(deletedSegment.getName()), TIMEOUT).join());

        // Create one index before main segment deletion.
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx.update(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT).join();

        // Clear the cache (otherwise we'll just end up serving cached entries and not try to access Storage).
        idx.removeAllCacheEntries();

        // Delete the Segment.
        val sm = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        sm.markDeleted();
        context.index.delete(sm.getName(), TIMEOUT).join();

        // Verify relevant operations cannot proceed.
        AssertExtensions.assertSuppliedFutureThrows(
                "update() worked on deleted segment.",
                () -> idx.update(Collections.singletonMap(UUID.randomUUID(), 2L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertSuppliedFutureThrows(
                "get() worked on deleted segment.",
                () -> idx.get(Collections.singleton(UUID.randomUUID()), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertSuppliedFutureThrows(
                "seal() worked on deleted segment.",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests a scenario where there are multiple concurrent updates to the Attribute Segment, at least one of which involves
     * a Snapshot.
     */
    @Test
    public void testConditionalUpdates() {
        val attributeId = UUID.randomUUID();
        val attributeId2 = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Write some data first.
        context.storage.writeInterceptor = null;
        idx.update(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()), TIMEOUT).join();

        // We intercept the Storage write. When doing so, we essentially duplicate whatever was already in there. This
        // does not corrupt the index (which would have happened in case of writing some random value), but it does test
        // our reconciliation mechanism.
        AtomicBoolean intercepted = new AtomicBoolean(false);
        context.storage.writeInterceptor = (segmentName, offset, data, length, wrappedStorage) -> {
            if (intercepted.compareAndSet(false, true)) {
                try {
                    // Duplicate the current index.
                    byte[] buffer = new byte[(int) offset];
                    wrappedStorage.read(idx.getAttributeSegmentHandle(), 0, buffer, 0, buffer.length);
                    wrappedStorage.write(idx.getAttributeSegmentHandle(), buffer.length, new ByteArrayInputStream(buffer), buffer.length);
                } catch (StreamSegmentException ex) {
                    throw new CompletionException(ex);
                }
            }

            // Complete the interception and indicate (false) that we haven't written anything.
            return CompletableFuture.completedFuture(false);
        };

        // This call should trigger a conditional update conflict. We want to use a different attribute so that we can
        // properly test the reconciliation algorithm by validating the written value for another attribute.
        idx.update(Collections.singletonMap(attributeId2, 0L), TIMEOUT).join();
        val value1 = idx.get(Collections.singleton(attributeId), TIMEOUT).join().get(attributeId);
        val value2 = idx.get(Collections.singleton(attributeId2), TIMEOUT).join().get(attributeId2);
        Assert.assertEquals("Unexpected value after reconciliation.", lastWrittenValue.get(), (long) value1);
        Assert.assertEquals("Unexpected value for second attribute after reconciliation.", 0L, (long) value2);
        Assert.assertTrue("No interception was done.", intercepted.get());
    }

    /**
     * Tests reading from the Attribute Segment while a truncation was in progress. Scenario:
     * 1. We have a concurrent get() and update()/delete(), where get() starts executing first.
     * 2. get() manages to fetch the location of the root page, but doesn't read it yet.
     * 3. The update() causes the previous root page to become obsolete and truncates the segment after it.
     * 4. The code should be able to handle and recover from this.
     */
    @Test
    public void testTruncatedSegmentGet() throws Exception {
        testTruncatedSegment(
                (attributeId, idx) -> idx.get(Collections.singleton(attributeId), TIMEOUT),
                result -> result.entrySet().stream().findFirst().get());
    }

    /**
     * Tests iterating from the Attribute Segment while a truncation was in progress. Scenario:
     * 1. We have a concurrent (active) iterator() and put()/delete(), where the iterator starts executing first.
     * 2. The iterator manages to fetch the location of the root page, but doesn't read it yet.
     * 3. The put() causes the previous root page to become obsolete and truncates the segment after it.
     * 4. The code should be able to handle and recover from this.
     */
    @Test
    public void testTruncatedSegmentIterator() throws Exception {
        testTruncatedSegment(
                (attributeId, idx) -> idx.iterator(attributeId, attributeId, TIMEOUT).getNext(),
                result -> result.get(0));
    }

    private <T> void testTruncatedSegment(BiFunction<UUID, AttributeIndex, CompletableFuture<T>> toTest,
                                          Function<T, Map.Entry<UUID, Long>> getValue) throws Exception {
        val attributeId = UUID.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val config = AttributeIndexConfig.builder()
                                         .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 10) // Very, very frequent rollovers.
                                         .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Write one value.
        idx.update(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()), TIMEOUT).join();

        // Clear the cache so we are guaranteed to attempt to read from Storage.
        idx.removeAllCacheEntries();

        // Initiate a get(), but block it right after the first read.
        val intercepted = new AtomicBoolean(false);
        val blockRead = new CompletableFuture<Void>();
        val waitForInterception = new CompletableFuture<Void>();
        context.storage.readInterceptor = (name, offset, storage) -> {
            if (intercepted.compareAndSet(false, true)) {
                // This should attempt to read the Root Page; return the future to wait on and clear the interceptor.
                context.storage.readInterceptor = null;
                waitForInterception.complete(null);
                return blockRead;
            } else {
                Assert.fail("Not expecting to get here.");
                return null;
            }
        };
        val get = toTest.apply(attributeId, idx);
        waitForInterception.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // Make sure we got the the part where we read the root.

        // Initiate (and complete) a update(), this should truncate the segment beyond the first root, causing the get to fail.
        idx.update(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()), TIMEOUT).join();

        // Release the read.
        Assert.assertFalse("Not expecting the read to be done yet.", get.isDone());
        blockRead.complete(null);

        // Finally, verify we can read the data we want to.
        val attributePair = getValue.apply(get.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        Assert.assertEquals("Unexpected id read.", attributeId, attributePair.getKey());
        Assert.assertEquals("Unexpected value read.", lastWrittenValue.get(), (long) attributePair.getValue());
        Assert.assertTrue("No interception done.", intercepted.get());
    }

    /**
     * Tests the ability to process Cache Eviction signals and re-caching evicted values.
     */
    @Test
    public void testCacheEviction() {
        int attributeCount = 1000;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        val config = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 1024)
                .build();

        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (UUID attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
        }
        idx.update(expectedValues, TIMEOUT).join();

        // Everything should already be cached, so four our first check we don't expect any Storage reads.
        context.storage.readInterceptor = (String streamSegmentName, long offset, SyncStorage wrappedStorage) ->
                Futures.failedFuture(new AssertionError("Not expecting storage reads yet."));
        checkIndex(idx, expectedValues);
        val cacheStatus = idx.getCacheStatus();
        Assert.assertEquals("Not expecting different generations yet.", cacheStatus.getOldestGeneration(), cacheStatus.getNewestGeneration());
        val newGen = cacheStatus.getNewestGeneration() + 1;
        val removedSize = idx.updateGenerations(newGen, newGen);
        AssertExtensions.assertGreaterThan("Expecting something to be evicted.", 0, removedSize);

        // Re-check the index and verify at least one Storage Read happened.
        AtomicBoolean intercepted = new AtomicBoolean(false);
        context.storage.readInterceptor = (String streamSegmentName, long offset, SyncStorage wrappedStorage) -> {
            intercepted.set(true);
            return CompletableFuture.completedFuture(null);
        };

        checkIndex(idx, expectedValues);
        Assert.assertTrue("Expected at least one Storage read.", intercepted.get());

        // Now everything should be cached again.
        intercepted.set(false);
        checkIndex(idx, expectedValues);
        Assert.assertFalse("Not expecting any Storage read.", intercepted.get());
    }

    /**
     * Tests the ability to identify throw the correct exception when the Index gets corrupted.
     */
    @Test
    public void testIndexCorruption() {
        val attributeId = UUID.randomUUID();
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // Create an index.
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // Intercept the write and corrupt a value, then write the corrupted data to Storage.
        context.storage.writeInterceptor = (segmentName, offset, data, length, wrappedStorage) -> {
            try {
                byte[] buffer = StreamHelpers.readAll(data, length);
                // Offset 2 should correspond to the root page's ID; if we corrupt that, the BTreeIndex will refuse to
                // load that page, thinking it was reading garbage data.
                buffer[2] = (byte) (buffer[2] + 1);
                wrappedStorage.write(idx.getAttributeSegmentHandle(), offset, new ByteArrayInputStream(buffer), buffer.length);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }

            // Complete the interception and indicate (true) that we did write the data.
            return CompletableFuture.completedFuture(true);
        };

        // This will attempt to write something; the interceptor above will take care of it.
        idx.update(Collections.singletonMap(attributeId, 1L), TIMEOUT).join();
        context.storage.writeInterceptor = null; // Clear this so it doesn't interfere with us.

        // Clear the cache (so that we may read directly from Storage).
        idx.removeAllCacheEntries();

        // Verify an exception is thrown when we update something
        AssertExtensions.assertSuppliedFutureThrows(
                "update() succeeded with index corruption.",
                () -> idx.update(Collections.singletonMap(attributeId, 2L), TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Verify an exception is thrown when we read something.
        AssertExtensions.assertSuppliedFutureThrows(
                "get() succeeded with index corruption.",
                () -> idx.get(Collections.singleton(attributeId), TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Verify an exception is thrown when we try to initialize.
        context.index.cleanup(Collections.singleton(SEGMENT_ID));
    }

    /**
     * Tests the ability to create the Attribute Segment only upon the first write.
     */
    @Test
    public void testLazyCreateAttributeSegment() {
        val attributeId = UUID.randomUUID();
        val attributeSegmentName = StreamSegmentNameUtils.getAttributeSegmentName(SEGMENT_NAME);
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // Initialize the index and verify the attribute segment hasn't been created yet.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        Assert.assertFalse("Attribute Segment created after initialization.", context.storage.exists(attributeSegmentName, TIMEOUT).join());

        // Verify sealing works with empty segment and that it does not create the file.
        idx.seal(TIMEOUT).join();
        Assert.assertFalse("Attribute Segment created after sealing.", context.storage.exists(attributeSegmentName, TIMEOUT).join());

        // Verify reading works with empty segment and that it does not create the file.
        val get1 = idx.get(Collections.singleton(attributeId), TIMEOUT).join();
        Assert.assertFalse("Attribute Segment created after reading.", context.storage.exists(attributeSegmentName, TIMEOUT).join());
        Assert.assertNull("Not expecting any result from an empty index get.", get1.get(attributeId));

        // Verify updating creates the segment.
        idx.update(Collections.singletonMap(attributeId, 1L), TIMEOUT).join();
        Assert.assertTrue("Attribute Segment not created after updating.", context.storage.exists(attributeSegmentName, TIMEOUT).join());
        val get2 = idx.get(Collections.singleton(attributeId), TIMEOUT).join();
        Assert.assertEquals("Unexpected result after retrieval.", 1L, (long) get2.get(attributeId));
        idx.seal(TIMEOUT).join();
        Assert.assertTrue("Expecting sealed in storage.", context.storage.getStreamSegmentInfo(attributeSegmentName, TIMEOUT).join().isSealed());
    }

    private void testRegularOperations(int attributeCount, int batchSize, int repeats, AttributeIndexConfig config) {
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> new UUID(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<UUID, Long>();

        // Record every time we read from Storage.
        AtomicBoolean storageRead = new AtomicBoolean(false);
        context.storage.readInterceptor = (name, offset, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));

        // Populate data.
        val updateBatch = new HashMap<UUID, Long>();
        AtomicLong nextValue = new AtomicLong(0);
        for (int r = 0; r < repeats; r++) {
            for (UUID attributeId : attributes) {
                long value = nextValue.getAndIncrement();
                expectedValues.put(attributeId, value);
                updateBatch.put(attributeId, value);
                if (updateBatch.size() % batchSize == 0) {
                    idx.update(updateBatch, TIMEOUT).join();
                    updateBatch.clear();
                }
            }
        }

        // Commit any leftovers.
        if (updateBatch.size() > 0) {
            idx.update(updateBatch, TIMEOUT).join();
        }

        storageRead.set(false);
        checkIndex(idx, expectedValues);
        Assert.assertFalse("Not expecting any storage reads.", storageRead.get());

        // 2. Reload index and verify it still has the correct values. This also forces a cache cleanup so we read data
        // directly from Storage.
        context.index.cleanup(null);
        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        storageRead.set(false);
        checkIndex(idx2, expectedValues);
        Assert.assertTrue("Expecting storage reads after reload.", storageRead.get());

        // 3. Remove all values.
        idx2.update(toDelete(expectedValues.keySet()), TIMEOUT).join();
        expectedValues.replaceAll((key, v) -> Attributes.NULL_ATTRIBUTE_VALUE);
        checkIndex(idx2, expectedValues);
    }

    private Map<UUID, Long> toDelete(Collection<UUID> keys) {
        val result = new HashMap<UUID, Long>();
        keys.forEach(key -> result.put(key, null));
        return result;
    }

    private void checkIndex(AttributeIndex index, Map<UUID, Long> expectedValues) {
        val actual = index.get(expectedValues.keySet(), TIMEOUT).join();
        val expected = expectedValues.entrySet().stream()
                .filter(e -> e.getValue() != Attributes.NULL_ATTRIBUTE_VALUE)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        AssertExtensions.assertMapEquals("Unexpected attributes in index.", expected, actual);
    }

    private void populateSegments(TestContext context) {
        val sm = context.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        sm.setLength(0);
        sm.setStorageLength(0);
    }

    //region TestContext

    private class TestContext implements AutoCloseable {
        final InMemoryStorage memoryStorage;
        final TestContext.TestStorage storage;
        final UpdateableContainerMetadata containerMetadata;
        final ContainerAttributeIndexImpl index;
        final CacheStorage cacheStorage;
        final TestCacheManager cacheManager;

        TestContext(AttributeIndexConfig config) {
            this(config, CachePolicy.INFINITE);
        }

        TestContext(AttributeIndexConfig config, CachePolicy cachePolicy) {
            this.memoryStorage = new InMemoryStorage();
            this.memoryStorage.initialize(1);
            this.storage = new TestContext.TestStorage(new RollingStorage(this.memoryStorage, config.getAttributeSegmentRollingPolicy()), executorService());
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
            this.cacheManager = new TestCacheManager(cachePolicy, this.cacheStorage, executorService());
            val factory = new ContainerAttributeIndexFactoryImpl(config, this.cacheManager, executorService());
            this.index = factory.createContainerAttributeIndex(this.containerMetadata, this.storage);
        }

        @Override
        @SneakyThrows
        public void close() {
            this.index.close();
            this.storage.close();
            this.memoryStorage.close();
            AssertExtensions.assertEventuallyEquals("MEMORY LEAK: Attribute Index did not delete all CacheStorage entries after closing.",
                    0L, () -> this.cacheStorage.getSnapshot().getStoredBytes(), 10, TIMEOUT.toMillis());
            this.cacheManager.close();
            this.cacheStorage.close();
        }

        private class TestStorage extends AsyncStorageWrapper {
            private final SyncStorage wrappedStorage;
            private WriteInterceptor writeInterceptor;
            private SealInterceptor sealInterceptor;
            private ReadInterceptor readInterceptor;

            TestStorage(SyncStorage syncStorage, Executor executor) {
                super(syncStorage, executor);
                this.wrappedStorage = syncStorage;
            }

            @Override
            public CompletableFuture<Void> write(SegmentHandle handle, long offset, InputStream data, int length, Duration timeout) {
                WriteInterceptor wi = this.writeInterceptor;
                if (wi != null) {
                    return wi.apply(handle.getSegmentName(), offset, data, length, this.wrappedStorage)
                             .thenCompose(handled -> handled ? CompletableFuture.completedFuture(null) : super.write(handle, offset, data, length, timeout));
                } else {
                    return super.write(handle, offset, data, length, timeout);
                }
            }

            @Override
            public CompletableFuture<Integer> read(SegmentHandle handle, long offset, byte[] buffer, int bufferOffset, int length, Duration timeout) {
                ReadInterceptor ri = this.readInterceptor;
                if (ri != null) {
                    return ri.apply(handle.getSegmentName(), offset, this.wrappedStorage)
                            .thenCompose(v -> super.read(handle, offset, buffer, bufferOffset, length, timeout));
                } else {
                    return super.read(handle, offset, buffer, bufferOffset, length, timeout);
                }
            }

            @Override
            public CompletableFuture<Void> seal(SegmentHandle handle, Duration timeout) {
                SealInterceptor si = this.sealInterceptor;
                if (si != null) {
                    return si.apply(handle.getSegmentName(), this.wrappedStorage)
                            .thenCompose(v -> super.seal(handle, timeout));
                } else {
                    return super.seal(handle, timeout);
                }
            }
        }
    }

    @FunctionalInterface
    interface WriteInterceptor {
        CompletableFuture<Boolean> apply(String streamSegmentName, long offset, InputStream data, int length, SyncStorage wrappedStorage);
    }

    @FunctionalInterface
    interface SealInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, SyncStorage wrappedStorage);
    }

    @FunctionalInterface
    interface ReadInterceptor {
        CompletableFuture<Void> apply(String streamSegmentName, long offset, SyncStorage wrappedStorage);
    }

    //endregion
}
