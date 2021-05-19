/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.AttributeIndex;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.TestCacheManager;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.cache.CacheFullException;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
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
        val attributes = IntStream.range(-count / 2, count / 2).mapToObj(i -> AttributeId.uuid(i, -i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<AttributeId, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (AttributeId attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
        }

        idx.update(expectedValues, TIMEOUT).join();

        // Check iterator.
        // Sort the keys using natural order (AttributeId comparator).
        val sortedKeys = expectedValues.keySet().stream().sorted().collect(Collectors.toList());

        // Add some values outside of the bounds.
        sortedKeys.add(0, AttributeId.uuid(Long.MIN_VALUE, Long.MIN_VALUE));
        sortedKeys.add(AttributeId.uuid(Long.MAX_VALUE, Long.MAX_VALUE));

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
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> AttributeId.uuid(i, i)).collect(Collectors.toList());
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
        val expectedValues = new HashMap<AttributeId, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (AttributeId attributeId : attributes) {
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
                () -> idx.update(Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT),
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

        context.storage.create(NameUtils.getAttributeSegmentName(SEGMENT_NAME), TIMEOUT)
                       .thenCompose(handle -> context.storage.seal(handle, TIMEOUT));
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "Index allowed adding new values when sealed.",
                () -> idx.update(Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT),
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
        idx.update(Collections.singletonMap(AttributeId.randomUUID(), 0L), TIMEOUT).join();

        // We intentionally delete twice to make sure the operation is idempotent.
        context.index.delete(sm.getName(), TIMEOUT).join();
        context.index.delete(sm.getName(), TIMEOUT).join();

        AssertExtensions.assertSuppliedFutureThrows(
                "update() worked after delete().",
                () -> idx.update(Collections.singletonMap(AttributeId.randomUUID(), 0L), TIMEOUT),
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
                context.storage.exists(NameUtils.getAttributeSegmentName(SEGMENT_NAME), TIMEOUT).join());
    }

    /**
     * Tests the case when updates fail due to a Storage failure. This should prevent whatever operation triggered it
     * to completely fail and not record the data.
     */
    @Test
    public void testStorageFailure() {
        val attributeId = AttributeId.randomUUID();
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
     * Tests the case when updates/reads fail due to a Cache failure (i.e., Cache Full). This should not interfere with
     * whatever operation triggered it and it should not affect ongoing operations on the index.
     */
    @Test
    public void testCacheWriteFailure() throws Exception {
        val exceptionMakers = Arrays.<Supplier<RuntimeException>>asList(
                IntentionalException::new, () -> new CacheFullException("intentional"));
        val attributeId = AttributeId.randomUUID();
        val attributeSegmentName = NameUtils.getAttributeSegmentName(SEGMENT_NAME);
        val initialValue = 0L;
        val finalValue = 1L;

        for (val exceptionMaker : exceptionMakers) {
            @Cleanup
            val context = new TestContext(DEFAULT_CONFIG);
            populateSegments(context);
            val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

            // Populate the index with some initial value.
            val pointer1 = idx.update(Collections.singletonMap(attributeId, initialValue), TIMEOUT).join();

            // For the next insertion, simulate some cache exception.
            val insertInvoked = new AtomicBoolean(false);
            context.cacheStorage.beforeInsert = buffer -> {
                insertInvoked.set(true);
                throw exceptionMaker.get();
            };

            // Update the value. The cache insertion should fail because of the exception above.
            val pointer2 = idx.update(Collections.singletonMap(attributeId, finalValue), TIMEOUT).join();
            TestUtils.await(() -> context.storage.getStreamSegmentInfo(attributeSegmentName, TIMEOUT).join().getStartOffset() > pointer1, 5, TIMEOUT.toMillis());

            Assert.assertTrue(insertInvoked.get());
            AssertExtensions.assertGreaterThan("", pointer1, pointer2);

            // If the cache insertion would have actually failed, then the following read call would fail (it would try
            // to read from the truncated portion (that's why we have the asserts above).
            // First, setup a read interceptor to ensure we actually want to read from Storage.
            val readCount = new AtomicInteger(0);
            context.storage.readInterceptor = (segment, offset, length, wrappedStorage) -> {
                readCount.incrementAndGet();
                return CompletableFuture.completedFuture(null);
            };
            val value = idx.get(Collections.singletonList(attributeId), TIMEOUT).join();
            Assert.assertEquals(finalValue, (long) value.get(attributeId));
            AssertExtensions.assertGreaterThan("Expected Storage reads.", 0, readCount.get());
        }
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
                context.storage.exists(NameUtils.getAttributeSegmentName(deletedSegment.getName()), TIMEOUT).join());

        // Create one index before main segment deletion.
        @Cleanup
        val idx = (SegmentAttributeBTreeIndex) context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx.update(Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT).join();

        // Clear the cache (otherwise we'll just end up serving cached entries and not try to access Storage).
        idx.removeAllCacheEntries();

        // Delete the Segment.
        val sm = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        sm.markDeleted();
        context.index.delete(sm.getName(), TIMEOUT).join();

        // Verify relevant operations cannot proceed.
        AssertExtensions.assertSuppliedFutureThrows(
                "update() worked on deleted segment.",
                () -> idx.update(Collections.singletonMap(AttributeId.randomUUID(), 2L), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertSuppliedFutureThrows(
                "get() worked on deleted segment.",
                () -> idx.get(Collections.singleton(AttributeId.randomUUID()), TIMEOUT),
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
        val attributeId = AttributeId.randomUUID();
        val attributeId2 = AttributeId.randomUUID();
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

    private <T> void testTruncatedSegment(BiFunction<AttributeId, AttributeIndex, CompletableFuture<T>> toTest,
                                          Function<T, Map.Entry<AttributeId, Long>> getValue) throws Exception {
        val attributeId = AttributeId.randomUUID();
        val lastWrittenValue = new AtomicLong(0);
        val config = AttributeIndexConfig.builder()
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 10) // Very, very frequent rollovers.
                .build();
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        @Cleanup
        val idx = new TestSegmentAttributeBTreeIndex(config, context);
        idx.initialize(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Write one value.
        idx.update(Collections.singletonMap(attributeId, lastWrittenValue.incrementAndGet()), TIMEOUT).join();

        // Clear the cache so we are guaranteed to attempt to read from Storage.
        idx.removeAllCacheEntries();

        // Initiate a get(), but block it right after the first read.
        val intercepted = new AtomicBoolean(false);
        val blockRead = new CompletableFuture<Void>();
        val waitForInterception = new CompletableFuture<Void>();
        idx.readPageInterceptor = (handle, offset, length) -> {
            if (intercepted.compareAndSet(false, true)) {
                // This should attempt to read the Root Page; return the future to wait on and clear the interceptor.
                idx.readPageInterceptor = null;
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
     * Tests a scenario where multiple, concurrent reads are received for the same BTreeIndex pages. Verifies that no
     * two concurrent reads for the same page are issued to Storage (subsequent ones are piggybacked onto the first one).
     */
    @Test
    public void testConcurrentReads() throws Exception {
        val config = AttributeIndexConfig.builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 4096)
                .build();
        val attributeCount = config.getMaxIndexPageSize() / 16;
        val attributes = IntStream.range(0, attributeCount).boxed()
                .collect(Collectors.toMap(value -> AttributeId.randomUUID(), value -> (long) value));
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);
        @Cleanup
        val idx = new TestSegmentAttributeBTreeIndex(config, context);
        idx.initialize(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        val writeBytes = new AtomicInteger(0);
        context.storage.writeInterceptor = (name, offset, data, length, wrappedStorage) -> {
            writeBytes.addAndGet(length);
            return CompletableFuture.completedFuture(false); // Not interfering with the actual write.
        };

        // Write all the values.
        idx.update(attributes, TIMEOUT).join();
        context.storage.writeInterceptor = null;
        AssertExtensions.assertGreaterThan("Needing to write at least 2 pages.", config.getMaxIndexPageSize(), writeBytes.get());

        // Clear the cache so we are guaranteed to attempt to read from Storage.
        idx.removeAllCacheEntries();

        // Keep count of the read requests made to storage (Key = Offset&Length, Value = Count).
        val readCounts = Collections.synchronizedMap(new HashMap<Map.Entry<Long, Integer>, AtomicInteger>());

        // Every Storage Read is blocked (by offset & length).
        val readBlocks = Collections.synchronizedMap(new HashMap<Map.Entry<Long, Integer>, CompletableFuture<Void>>());
        context.storage.readInterceptor = (name, offset, length, wrappedStorage) -> {
            val key = new AbstractMap.SimpleImmutableEntry<>(offset, length);
            val cnt = readCounts.computeIfAbsent(key, e -> new AtomicInteger(0));
            val result = readBlocks.computeIfAbsent(key, e -> new CompletableFuture<>());
            cnt.incrementAndGet();
            return result;
        };

        // Issue 2 concurrent reads. Since we have a 2-level index, this should block initially on the root page being read,
        // and then on the 2 leaf pages that we have.
        val get1 = idx.get(attributes.keySet(), TIMEOUT);
        Assert.assertFalse(get1.isDone());
        val get2 = idx.get(attributes.keySet(), TIMEOUT);
        Assert.assertFalse(get2.isDone());

        // Verify that only one attempt was made to read from Storage.
        AssertExtensions.assertEventuallyEquals("Expected 1 read (root page) to be blocked.", 1, readCounts::size, 10, TIMEOUT.toMillis());
        val rootPageBlockCount = readCounts.entrySet().stream().findFirst().get();
        int expectedPageBlockCount = 1;
        Assert.assertEquals("Expected one attempt to read root page from Storage.", expectedPageBlockCount, rootPageBlockCount.getValue().get());

        // Unblock the root read.
        val rootPageBLock = readBlocks.entrySet().stream().findFirst().get();
        rootPageBLock.getValue().complete(null);

        expectedPageBlockCount += 2; // 2 Non-root (leaf) pages.
        AssertExtensions.assertEventuallyEquals("Expected 2 leaf page reads to be blocked.", expectedPageBlockCount, readCounts::size, 10, TIMEOUT.toMillis());

        // Unblock everything and get the results.
        readBlocks.values().forEach(f -> f.complete(null));
        val result1 = get1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val result2 = get2.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertMapEquals("Unexpected result 1.", attributes, result1);
        AssertExtensions.assertMapEquals("Unexpected result 2.", attributes, result2);

        // Issue the request again. Validate that it never touches the Storage (cached).
        context.storage.readInterceptor = (name, offset, length, wrappedStorage) -> {
            Assert.fail("Not expecting a Storage read.");
            return null;
        };
        val get3 = idx.get(attributes.keySet(), TIMEOUT);
        val result3 = get3.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        AssertExtensions.assertMapEquals("Unexpected result 3 (from cache).", attributes, result3);

        for (val cnt : readCounts.entrySet()) {
            Assert.assertEquals("Expected a single storage read attempt for " + cnt.getKey(), 1, cnt.getValue().get());
        }

        for (val block : readBlocks.entrySet()) {
            Assert.assertTrue("Expected all reads to be unblocked for " + block.getKey(), block.getValue().isDone());
        }
    }

    /**
     * Tests the ability to process Cache Eviction signals and re-caching evicted values.
     */
    @Test
    public void testCacheEviction() {
        int attributeCount = 1000;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> AttributeId.uuid(i, i)).collect(Collectors.toList());
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
        val expectedValues = new HashMap<AttributeId, Long>();

        // Populate data.
        AtomicLong nextValue = new AtomicLong(0);
        for (AttributeId attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
        }
        idx.update(expectedValues, TIMEOUT).join();

        // Everything should already be cached, so four our first check we don't expect any Storage reads.
        context.storage.readInterceptor = (String streamSegmentName, long offset, int length, SyncStorage wrappedStorage) ->
                Futures.failedFuture(new AssertionError("Not expecting storage reads yet."));
        checkIndex(idx, expectedValues);
        val cacheStatus = idx.getCacheStatus();
        Assert.assertEquals("Not expecting different generations yet.", cacheStatus.getOldestGeneration(), cacheStatus.getNewestGeneration());
        val newGen = cacheStatus.getNewestGeneration() + 1;
        boolean anythingRemoved = idx.updateGenerations(newGen, newGen, false);
        Assert.assertTrue("Expecting something to be evicted.", anythingRemoved);

        // Re-check the index and verify at least one Storage Read happened.
        AtomicBoolean intercepted = new AtomicBoolean(false);
        context.storage.readInterceptor = (String streamSegmentName, long offset, int length, SyncStorage wrappedStorage) -> {
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
     * Tests the ability to enable or disable the cache based on {@link SegmentAttributeBTreeIndex#updateGenerations}
     * when it receives different values for the "essentialOnly" arg.
     */
    @Test
    public void testNonEssentialCache() throws Exception {
        val attributeId = AttributeId.randomUUID();
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
        val expectedValues = new HashMap<AttributeId, Long>();
        expectedValues.put(attributeId, 1L);

        // Populate data.
        idx.update(expectedValues, TIMEOUT).join();
        checkIndex(idx, expectedValues);

        val cacheStatus = idx.getCacheStatus();
        val newGen = cacheStatus.getNewestGeneration() + 1;
        boolean anythingRemoved = idx.updateGenerations(newGen, newGen, false);
        Assert.assertTrue("Expecting something to be evicted (essential=false).", anythingRemoved);

        val readCount = new AtomicInteger(0);
        context.storage.readInterceptor = (String streamSegmentName, long offset, int length, SyncStorage wrappedStorage) -> {
            readCount.incrementAndGet();
            return CompletableFuture.completedFuture(null);
        };

        // Re-insert that page into the cache and verify it's being loaded and cached (by reading it twice).
        checkIndex(idx, expectedValues);
        checkIndex(idx, expectedValues);
        Assert.assertEquals("Not expecting caching to be disabled yet.", 1, readCount.get());
        TestUtils.await(() -> idx.getPendingReadCount() == 0, 5, TIMEOUT.toMillis()); // Let the pending read index clear before proceeding.

        // Set the non-essential-only flag.
        anythingRemoved = idx.updateGenerations(newGen, newGen, true);
        Assert.assertFalse("Not expecting anything to be evicted (essential=true).", anythingRemoved);
        checkIndex(idx, expectedValues);
        Assert.assertEquals("Not expecting anything to be evicted yet (essential=true).", 1, readCount.get());

        expectedValues.put(attributeId, 2L);
        idx.update(expectedValues, TIMEOUT).join();
        Assert.assertEquals("Not expecting any storage reads yet (essential=true).", 1, readCount.get());
        checkIndex(idx, expectedValues);
        Assert.assertEquals("Expected a storage read (essential=true).", 2, readCount.get());

        // Disable the non-essential-only flag.
        anythingRemoved = idx.updateGenerations(newGen, newGen, false);
        TestUtils.await(() -> idx.getPendingReadCount() == 0, 5, TIMEOUT.toMillis()); // Let the pending read index clear before proceeding.
        Assert.assertFalse("Not expecting anything to be evicted (essential=false).", anythingRemoved);
        checkIndex(idx, expectedValues);
        checkIndex(idx, expectedValues); // Do it twice to check that the cache has been properly re-enabled.
        Assert.assertEquals("Expected a single storage read (essential=false).", 3, readCount.get());
    }

    /**
     * Tests the ability to identify throw the correct exception when the Index gets corrupted.
     */
    @Test
    public void testIndexCorruption() {
        val attributeId = AttributeId.randomUUID();
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
     * Tests the AttributeIndex recovery when the underlying Storage adapter supports atomic writes. It is expected that
     * any metadata-stored Root Pointer is ignored in this case.
     */
    @Test
    public void testRecoveryWithAtomicStorageWrites() {
        val attribute = AttributeId.randomUUID();
        val value1 = 1L;
        val value2 = 2L;
        val noRollingConfig = AttributeIndexConfig.builder().build(); // So we don't trip over rollovers.

        @Cleanup
        val context = new TestContext(noRollingConfig);
        context.storage.supportsTruncation = false; // So we don't trip over truncations or rollovers.
        context.storage.supportsAtomicWrites = true;
        populateSegments(context);

        // 1. Populate the index with first value
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val rootPointer1 = idx.update(Collections.singletonMap(attribute, value1), TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Expected a valid Root pointer after update #2.", 0, rootPointer1);

        val rootPointer2 = idx.update(Collections.singletonMap(attribute, value2), TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Expected a valid Root pointer after update #2.", rootPointer1, rootPointer2);

        // 2. Update the Container Metadata with the first root pointer.
        context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID)
                .updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, rootPointer1));

        // 3. Reload index and verify it ignores the metadata-stored root pointer (and uses the actual index).
        context.index.cleanup(null);
        val storageRead = new AtomicBoolean();
        context.storage.readInterceptor = (name, offset, length, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));

        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val actualValue = idx2.get(Collections.singleton(attribute), TIMEOUT).join();
        Assert.assertEquals("Unexpected value read.", value2, (long) actualValue.get(attribute));
        Assert.assertTrue("Expecting storage reads after reload.", storageRead.get());
    }

    /**
     * Tests the behavior of the Attribute Index when it is told that the underlying Storage adapter supports atomic
     * writes but in fact it does not. It is expected to fail with corrupted data.
     */
    @Test
    public void testRecoveryAfterIncompleteUpdateWithNoRootPointer() {
        val attribute = AttributeId.randomUUID();
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        // Lie to the SegmentAttributeBTreeIndex. Tell it that the Storage adapter does support atomic writes, but it
        // reality it does not.
        context.storage.supportsAtomicWrites = true;
        populateSegments(context);

        // 1. Populate the index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val rootPointer = idx.update(Collections.singletonMap(attribute, 0L), TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Expected a valid Root pointer after update.", 0, rootPointer);

        // 2. Write some garbage data at the end of the segment. This simulates a partial (incomplete update) that did not
        // fully write the BTree pages to the end of the segment.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(SEGMENT_NAME);
        val partialUpdate = new byte[1234];
        context.storage.openWrite(attributeSegmentName)
                .thenCompose(handle -> context.storage.write(
                        handle,
                        context.storage.getStreamSegmentInfo(attributeSegmentName, TIMEOUT).join().getLength(),
                        new ByteArrayInputStream(partialUpdate),
                        partialUpdate.length, TIMEOUT))
                .join();

        // 3. Reload index and verify it fails to initialize.
        context.index.cleanup(null);
        val storageRead = new AtomicBoolean();
        context.storage.readInterceptor = (name, offset, length, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));

        AssertExtensions.assertSuppliedFutureThrows(
                "Expected index initialization/read to fail.",
                () -> context.index.forSegment(SEGMENT_ID, TIMEOUT)
                        .thenCompose(idx2 -> idx2.get(Collections.singleton(attribute), TIMEOUT)),
                ex -> ex instanceof Exception);
        Assert.assertTrue("Expecting storage reads after reload.", storageRead.get());
    }

    /**
     * Tests the ability of the Attribute Index to recover correctly after a partial update has been written to Storage.
     * This simulates how it should be used by a caller: after every update, the {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER}
     * attribute of the segment should be set to the return value from {@link AttributeIndex#update} in order to perform
     * a correct recovery.
     */
    @Test
    public void testRecoveryAfterIncompleteUpdateWithRootPointer() {
        final int attributeCount = 1000;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> AttributeId.uuid(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(DEFAULT_CONFIG);
        // Root pointers are read from Segment's Metadata if Storage does not support atomic writes. This test validates
        // that case: a partial write with the help of metadata-stored Root Pointers (if the Root Pointers were not stored
        // in the metadata, then the recovery would fail).
        context.storage.supportsAtomicWrites = false;
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<AttributeId, Long>();
        val updateBatch = new HashMap<AttributeId, Long>();
        AtomicLong nextValue = new AtomicLong(0);
        for (AttributeId attributeId : attributes) {
            long value = nextValue.getAndIncrement();
            expectedValues.put(attributeId, value);
            updateBatch.put(attributeId, value);
        }

        // Perform the update and remember the root pointer.
        long rootPointer = idx.update(updateBatch, TIMEOUT).join();
        context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID).updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, rootPointer));

        // 2. Write some garbage data at the end of the segment. This simulates a partial (incomplete update) that did not
        // fully write the BTree pages to the end of the segment.
        String attributeSegmentName = NameUtils.getAttributeSegmentName(SEGMENT_NAME);
        byte[] partialUpdate = new byte[1234];
        context.storage.openWrite(attributeSegmentName)
                .thenCompose(handle -> context.storage.write(
                        handle,
                        context.storage.getStreamSegmentInfo(attributeSegmentName, TIMEOUT).join().getLength(),
                        new ByteArrayInputStream(partialUpdate),
                        partialUpdate.length, TIMEOUT))
                .join();

        // 3. Reload index and verify it still has the correct values. This also forces a cache cleanup so we read data
        // directly from Storage.
        context.index.cleanup(null);
        val storageRead = new AtomicBoolean();
        context.storage.readInterceptor = (name, offset, length, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));
        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        checkIndex(idx2, expectedValues);
        Assert.assertTrue("Expecting storage reads after reload.", storageRead.get());

        // 4. Remove all values (and thus force an update - validates conditional updates still work in this case).
        idx2.update(toDelete(expectedValues.keySet()), TIMEOUT).join();
        expectedValues.replaceAll((key, v) -> Attributes.NULL_ATTRIBUTE_VALUE);
        checkIndex(idx2, expectedValues);
    }

    /**
     * Tests the ability of the Attribute Index to recover correctly after an update has been successfully written to Storage,
     * but the previous value of the {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER} has been truncated out without
     * having the new value persisted (most likely due to a system crash).
     * In this case, the {@link Attributes#ATTRIBUTE_SEGMENT_ROOT_POINTER} value should be ignored and the index should
     * be attempted to be read from Storage without providing hints to where to start reading from.
     */
    @Test
    public void testTruncatedRootPointer() throws Exception {
        val attributeSegmentName = NameUtils.getAttributeSegmentName(SEGMENT_NAME);
        val config = AttributeIndexConfig
                .builder()
                .with(AttributeIndexConfig.MAX_INDEX_PAGE_SIZE, 1024)
                .with(AttributeIndexConfig.ATTRIBUTE_SEGMENT_ROLLING_SIZE, 8)
                .build();
        final int attributeCount = 20;
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> AttributeId.uuid(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val expectedValues = new HashMap<AttributeId, Long>();
        long nextValue = 0;
        long previousRootPointer = -1;
        boolean invalidRootPointer = false;
        for (AttributeId attributeId : attributes) {
            val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();

            val value = nextValue++;
            expectedValues.put(attributeId, value);
            val updateBatch = Collections.singletonMap(attributeId, value);

            // Inject a callback to know when we finished truncating, as it is an async task.
            @Cleanup("release")
            val truncated = new ReusableLatch();
            context.storage.truncateCallback = (s, o) -> truncated.release();
            val rootPointer = idx.update(updateBatch, TIMEOUT).join();
            truncated.await(TIMEOUT.toMillis());
            val startOffset = context.storage.getStreamSegmentInfo(attributeSegmentName, TIMEOUT).join().getStartOffset();
            if (previousRootPointer >= 0) {
                // We always set the Root Pointer to be one "value behind". Since we truncate the Attribute Segment with
                // every update (small rolling size), doing this ensures that its value should always be less than the
                // segment's start offset. This is further validated by asserting that invalidRootPointer is true at the
                // end of this loop.
                context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID)
                        .updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, previousRootPointer));
                invalidRootPointer |= previousRootPointer < startOffset;
            }

            previousRootPointer = rootPointer;

            // Clean up the index cache and force a reload. Verify that we can read from the index.
            context.index.cleanup(null);
            val storageRead = new AtomicBoolean();
            context.storage.readInterceptor = (name, offset, length, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));
            val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
            checkIndex(idx2, expectedValues);
            Assert.assertTrue("Expecting storage reads after reload.", storageRead.get());
        }

        Assert.assertTrue("No invalid Root Pointers generated during the test.", invalidRootPointer);

        // 3. Remove all values (and thus force an update - validates conditional updates still work in this case).
        context.index.cleanup(null);
        val idx2 = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        idx2.update(toDelete(expectedValues.keySet()), TIMEOUT).join();
        expectedValues.replaceAll((key, v) -> Attributes.NULL_ATTRIBUTE_VALUE);
        checkIndex(idx2, expectedValues);
    }

    /**
     * Tests the ability to create the Attribute Segment only upon the first write.
     */
    @Test
    public void testLazyCreateAttributeSegment() {
        val attributeId = AttributeId.randomUUID();
        val attributeSegmentName = NameUtils.getAttributeSegmentName(SEGMENT_NAME);
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
        val attributes = IntStream.range(0, attributeCount).mapToObj(i -> AttributeId.uuid(i, i)).collect(Collectors.toList());
        @Cleanup
        val context = new TestContext(config);
        populateSegments(context);

        // 1. Populate and verify first index.
        val idx = context.index.forSegment(SEGMENT_ID, TIMEOUT).join();
        val expectedValues = new HashMap<AttributeId, Long>();

        // Record every time we read from Storage.
        AtomicBoolean storageRead = new AtomicBoolean(false);
        context.storage.readInterceptor = (name, offset, length, storage) -> CompletableFuture.runAsync(() -> storageRead.set(true));

        // Populate data.
        val updateBatch = new HashMap<AttributeId, Long>();
        AtomicLong nextValue = new AtomicLong(0);
        for (int r = 0; r < repeats; r++) {
            for (AttributeId attributeId : attributes) {
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

    private Map<AttributeId, Long> toDelete(Collection<AttributeId> keys) {
        val result = new HashMap<AttributeId, Long>();
        keys.forEach(key -> result.put(key, null));
        return result;
    }

    private void checkIndex(AttributeIndex index, Map<AttributeId, Long> expectedValues) {
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
        final TestCache cacheStorage;
        final TestCacheManager cacheManager;

        TestContext(AttributeIndexConfig config) {
            this(config, CachePolicy.INFINITE);
        }

        TestContext(AttributeIndexConfig config, CachePolicy cachePolicy) {
            this.memoryStorage = new InMemoryStorage();
            this.memoryStorage.initialize(1);
            this.storage = new TestContext.TestStorage(new RollingStorage(this.memoryStorage, config.getAttributeSegmentRollingPolicy()), executorService());
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.cacheStorage = new TestCache(Integer.MAX_VALUE);
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
                    0L, () -> this.cacheStorage.getState().getStoredBytes(), 10, TIMEOUT.toMillis());
            this.cacheManager.close();
            this.cacheStorage.close();
        }

        private class TestCache extends DirectMemoryCache {
            volatile Consumer<BufferView> beforeInsert;

            TestCache(long maxSizeBytes) {
                super(maxSizeBytes);
            }

            @Override
            public int insert(BufferView data) {
                val c = this.beforeInsert;
                if (c != null) {
                    c.accept(data);
                }
                return super.insert(data);
            }
        }

        private class TestStorage extends AsyncStorageWrapper {
            private final SyncStorage wrappedStorage;
            private final Map<String, Long> startOffsets;
            private WriteInterceptor writeInterceptor;
            private SealInterceptor sealInterceptor;
            private ReadInterceptor readInterceptor;
            private BiConsumer<String, Long> truncateCallback;
            private boolean supportsAtomicWrites;
            private boolean supportsTruncation;

            TestStorage(SyncStorage syncStorage, Executor executor) {
                super(syncStorage, executor);
                this.wrappedStorage = syncStorage;
                this.startOffsets = new ConcurrentHashMap<>();
                this.supportsTruncation = true;
            }

            @Override
            public boolean supportsTruncation() {
                return this.supportsTruncation;
            }

            @Override
            public boolean supportsAtomicWrites() {
                return this.supportsAtomicWrites;
            }

            @Override
            public CompletableFuture<Void> truncate(SegmentHandle handle, long offset, Duration timeout) {
                Preconditions.checkState(supportsTruncation(), "Truncations not enabled");
                // We need to simulate the ChunkedSegmentStorage (correct) behavior for truncating segments. While the
                // legacy RollingStorage would approximate a StartOffset to an offset at most equal to the requested
                // Truncation Offset, the ChunkedSegmentStorage is very strict about that, so it will deny any read prior
                // to that offset. In addition, the ChunkedSegmentStorage also returns the correct StartOffset as part of
                // getStreamSegmentInfo while RollingStorage does not.
                return super.truncate(handle, offset, timeout)
                        .thenRun(() -> {
                            this.startOffsets.put(handle.getSegmentName(), offset);
                            BiConsumer<String, Long> c = this.truncateCallback;
                            if (c != null) {
                                c.accept(handle.getSegmentName(), offset);
                            }
                        });
            }

            @Override
            public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
                return super.getStreamSegmentInfo(streamSegmentName, timeout)
                        .thenApply(si -> {
                            val startOffset = this.startOffsets.getOrDefault(streamSegmentName, -1L);
                            if (startOffset >= 0) {
                                return StreamSegmentInformation.from(si).startOffset(startOffset).build();
                            } else {
                                return si;
                            }
                        });
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
                val startOffset = this.startOffsets.getOrDefault(handle.getSegmentName(), -1L);
                if (startOffset >= 0 && offset < startOffset) {
                    return Futures.failedFuture(new StreamSegmentTruncatedException(handle.getSegmentName(), startOffset, offset));
                }

                ReadInterceptor ri = this.readInterceptor;
                if (ri != null) {
                    return ri.apply(handle.getSegmentName(), offset, length, this.wrappedStorage)
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

    private class TestSegmentAttributeBTreeIndex extends SegmentAttributeBTreeIndex {
        private ReadPageInterceptor readPageInterceptor;

        TestSegmentAttributeBTreeIndex(AttributeIndexConfig config, TestContext context) {
            super(context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID), context.storage, context.cacheStorage,
                    config, executorService());
        }

        @Override
        CompletableFuture<ByteArraySegment> readPageFromStorage(SegmentHandle handle, long offset, int length, boolean shouldCache, Duration timeout) {
            val interceptor = this.readPageInterceptor;
            if (interceptor != null) {
                return interceptor.apply(handle, offset, length)
                        .thenCompose(v -> super.readPageFromStorage(handle, offset, length, shouldCache, timeout));
            }
            return super.readPageFromStorage(handle, offset, length, shouldCache, timeout);
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
        CompletableFuture<Void> apply(String streamSegmentName, long offset, int length, SyncStorage wrappedStorage);
    }

    @FunctionalInterface
    interface ReadPageInterceptor {
        CompletableFuture<Void> apply(SegmentHandle handle, long offset, int length);
    }

    //endregion
}
