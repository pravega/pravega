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
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
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
        idx.put(expectedValues, TIMEOUT).join();

        // Check index before sealing.
        checkIndex(idx, expectedValues);

        // Seal twice (to check idempotence).
        idx.seal(TIMEOUT).join();
        idx.seal(TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Index allowed adding new values after being sealed.",
                () -> idx.put(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);

        // Check index again, after sealing.
        checkIndex(idx, expectedValues);
    }

    /**
     * Tests the ability to delete all AttributeData for a particular Segment.
     */
    @Test
    public void testDelete() {
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // 1. Populate and verify first index.
        val sm = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

        // We intentionally delete twice to make sure the operation is idempotent.
        context.index.delete(sm.getName(), TIMEOUT).join();
        context.index.delete(sm.getName(), TIMEOUT).join();

        AssertExtensions.assertThrows(
                "put() worked after delete().",
                () -> idx.put(Collections.singletonMap(UUID.randomUUID(), 0L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "seal() worked after delete().",
                () -> idx.seal(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Check index after deleting.
        checkIndex(idx, Collections.emptyMap());
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
        AssertExtensions.assertThrows(
                "put() worked with Storage failure.",
                () -> idx.put(Collections.singletonMap(attributeId, 0L), TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("A value was retrieved after a failed put().", 0, idx.get(Collections.singleton(attributeId), TIMEOUT).join().size());

        // 2. When Sealing.
        context.storage.sealInterceptor = (streamSegmentName, wrappedStorage) -> {
            throw new IntentionalException();
        };
        AssertExtensions.assertThrows(
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
        AssertExtensions.assertThrows(
                "forSegment() worked on deleted segment.",
                () -> context.index.forSegment(deletedSegment.getId(), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
        Assert.assertFalse("Attribute segment was created in Storage for a deleted Segment..",
                context.storage.exists(StreamSegmentNameUtils.getAttributeSegmentName(deletedSegment.getName()), TIMEOUT).join());

        // Create one index before main segment deletion.
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx.put(Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT).join();

        // Clear the cache (otherwise we'll just end up serving cached entries and not try to access Storage).
        idx.removeAllCacheEntries();

        // Delete the Segment.
        val sm = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        sm.markDeleted();
        context.index.delete(sm.getName(), TIMEOUT).join();

        // Verify relevant operations cannot proceed.
        AssertExtensions.assertThrows(
                "put() worked on deleted segment.",
                () -> idx.put(Collections.singletonMap(UUID.randomUUID(), 2L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "get() worked on deleted segment.",
                () -> idx.get(Collections.singleton(UUID.randomUUID()), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
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
        idx.put(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()), TIMEOUT).join();

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
        idx.put(Collections.singletonMap(attributeId2, 0L), TIMEOUT).join();
        val value1 = idx.get(Collections.singleton(attributeId), TIMEOUT).join().get(attributeId);
        val value2 = idx.get(Collections.singleton(attributeId2), TIMEOUT).join().get(attributeId2);
        Assert.assertEquals("Unexpected value after reconciliation.", lastWrittenValue.get(), (long) value1);
        Assert.assertEquals("Unexpected value for second attribute after reconciliation.", 0L, (long) value2);
        Assert.assertTrue("No interception was done.", intercepted.get());
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
        idx.put(expectedValues, TIMEOUT).join();

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
        idx.put(Collections.singletonMap(attributeId, 1L), TIMEOUT).join();
        context.storage.writeInterceptor = null; // Clear this so it doesn't interfere with us.

        // Clear the cache (so that we may read directly from Storage).
        idx.removeAllCacheEntries();

        // Verify an exception is thrown when we write something
        AssertExtensions.assertThrows(
                "",
                () -> idx.put(Collections.singletonMap(attributeId, 2L), TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Verify an exception is thrown when we read something.
        AssertExtensions.assertThrows(
                "",
                () -> idx.get(Collections.singleton(attributeId), TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Verify an exception si thrown when we remove something.
        AssertExtensions.assertThrows(
                "",
                () -> idx.remove(Collections.singleton(attributeId), TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Verify an exception is thrown when we try to initialize.
        context.index.cleanup(Collections.singleton(SEGMENT_ID));
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
                    idx.put(updateBatch, TIMEOUT).join();
                    updateBatch.clear();
                }
            }
        }

        // Commit any leftovers.
        if (updateBatch.size() > 0) {
            idx.put(updateBatch, TIMEOUT).join();
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
        idx2.remove(expectedValues.keySet(), TIMEOUT).join();
        expectedValues.replaceAll((key, v) -> Attributes.NULL_ATTRIBUTE_VALUE);
        checkIndex(idx2, expectedValues);
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
        final InMemoryCacheFactory cacheFactory;
        final TestCacheManager cacheManager;

        TestContext(AttributeIndexConfig config) {
            this(config, CachePolicy.INFINITE);
        }

        TestContext(AttributeIndexConfig config, CachePolicy cachePolicy) {
            this.memoryStorage = new InMemoryStorage();
            this.memoryStorage.initialize(1);
            this.storage = new TestContext.TestStorage(new RollingStorage(this.memoryStorage, config.getAttributeSegmentRollingPolicy()), executorService());
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new TestCacheManager(cachePolicy, executorService());
            val factory = new ContainerAttributeIndexFactoryImpl(config, this.cacheFactory, this.cacheManager, executorService());
            this.index = factory.createContainerAttributeIndex(this.containerMetadata, this.storage);
        }

        @Override
        public void close() {
            this.index.close();
            this.cacheManager.close();
            this.cacheFactory.close();
            this.storage.close();
            this.memoryStorage.close();
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
