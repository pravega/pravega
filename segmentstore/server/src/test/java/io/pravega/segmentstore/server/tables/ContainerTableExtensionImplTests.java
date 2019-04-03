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

import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link ContainerTableExtensionImpl} class.
 */
public class ContainerTableExtensionImplTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 1;
    private static final long SEGMENT_ID = 2L;
    private static final String SEGMENT_NAME = "TableSegment";
    private static final int MAX_KEY_LENGTH = 128;
    private static final int MAX_VALUE_LENGTH = 64;
    private static final int SINGLE_UPDATE_COUNT = 10;
    private static final int BATCH_UPDATE_COUNT = 10000;
    private static final int BATCH_SIZE = 689;
    private static final int ITERATOR_BATCH_UPDATE_COUNT = 1000; // Iterators are slower to check, so we reduce our test size.
    private static final int ITERATOR_BATCH_UPDATE_SIZE = 69;
    private static final double REMOVE_FRACTION = 0.3; // 30% of generated operations are removes.
    private static final int SHORT_TIMEOUT_MILLIS = 20; // To verify a get() is blocked.
    private static final int DEFAULT_COMPACTION_SIZE = -1; // Inherits from parent.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis() * 4, TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the ability to create and delete TableSegments.
     */
    @Test
    public void testCreateDelete() {
        @Cleanup
        val context = new TestContext();
        val nonTableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.createSegmentMetadata());
        Assert.assertTrue("Not expecting any Writer Table Processors for non-table segment.", nonTableSegmentProcessors.isEmpty());

        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertNotNull("Segment not created", context.segment());
        val tableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.segment().getMetadata());
        Assert.assertFalse("Expecting Writer Table Processors for table segment.", tableSegmentProcessors.isEmpty());

        checkIterators(Collections.emptyMap(), context.ext);

        context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT).join();
        Assert.assertNull("Segment not deleted", context.segment());
        AssertExtensions.assertSuppliedFutureThrows(
                "Segment not deleted.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Verifies that the methods that are not yet implemented are not implemented by accident without unit tests.
     * This test should be removed once every method tested in it is implemented.
     */
    @Test
    public void testUnimplementedMethods() {
        @Cleanup
        val context = new TestContext();
        AssertExtensions.assertThrows(
                "merge() is implemented.",
                () -> context.ext.merge(SEGMENT_NAME, SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
        AssertExtensions.assertThrows(
                "seal() is implemented.",
                () -> context.ext.seal(SEGMENT_NAME, TIMEOUT),
                ex -> ex instanceof UnsupportedOperationException);
    }

    /**
     * Tests the ability to perform unconditional updates using a single key at a time using a {@link KeyHasher} that is
     * not prone to collisions.
     */
    @Test
    public void testSingleUpdateUnconditional() {
        testSingleUpdates(KeyHashers.DEFAULT_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform unconditional updates using a single key at a time using a {@link KeyHasher} that is
     * very prone to collisions (in this case, it hashes everything to the same hash).
     */
    @Test
    public void testSingleUpdateUnconditionalCollisions() {
        testSingleUpdates(KeyHashers.CONSTANT_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates using a single key at a time using a {@link KeyHasher} that is
     * not prone to collisions.
     */
    @Test
    public void testSingleUpdateConditional() {
        testSingleUpdates(KeyHashers.DEFAULT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates using a single key at a time using a {@link KeyHasher} that is
     * very prone to collisions (in this case, it hashes everything to the same hash).
     */
    @Test
    public void testSingleUpdateConditionalCollisions() {
        testSingleUpdates(KeyHashers.CONSTANT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to perform unconditional updates and removals using a {@link KeyHasher} that is not prone to
     * collisions.
     */
    @Test
    public void testBatchUpdatesUnconditional() {
        testBatchUpdates(KeyHashers.DEFAULT_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform unconditional updates and removals using a {@link KeyHasher} that is very prone to
     * collisions.
     */
    @Test
    public void testBatchUpdatesUnconditionalWithCollisions() {
        testBatchUpdates(KeyHashers.COLLISION_HASHER, this::toUnconditionalTableEntry, this::toUnconditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is not prone to collisions.
     */
    @Test
    public void testBatchUpdatesConditional() {
        testBatchUpdates(KeyHashers.DEFAULT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testBatchUpdatesConditionalWithCollisions() {
        testBatchUpdates(KeyHashers.COLLISION_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
    }

    /**
     * Tests the ability to iterate over keys or entries using a {@link KeyHasher} that is not prone to collisions.
     */
    @Test
    public void testIterators() {
        testBatchUpdates(
                ITERATOR_BATCH_UPDATE_COUNT,
                ITERATOR_BATCH_UPDATE_SIZE,
                KeyHashers.DEFAULT_HASHER,
                this::toUnconditionalTableEntry,
                this::toUnconditionalKey,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    /**
     * Tests the ability to iterate over keys or entries using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testIteratorsWithCollisions() {
        testBatchUpdates(
                ITERATOR_BATCH_UPDATE_COUNT,
                ITERATOR_BATCH_UPDATE_SIZE,
                KeyHashers.COLLISION_HASHER,
                this::toUnconditionalTableEntry,
                this::toUnconditionalKey,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    /**
     * Tests the ability update and access entries when compaction occurs using a {@link KeyHasher} that is not prone
     * to collisions.
     */
    @Test
    public void testTableSegmentCompacted() {
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER, this::check);
    }

    /**
     * Tests the ability update and access entries when compaction occurs using a {@link KeyHasher} that is very prone
     * to collisions.
     */
    @Test
    public void testTableSegmentCompactedWithCollisions() {
        // TODO: this fails.
        testTableSegmentCompacted(KeyHashers.COLLISION_HASHER, this::check);
    }

    /**
     * Tests the ability iterate over entries when compaction occurs using a {@link KeyHasher} that is not prone
     * to collisions.
     */
    @Test
    public void testTableSegmentCompactedIterators() {
        // TODO: this doesn't exercise the truncation retry.
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    @SneakyThrows
    private void testTableSegmentCompacted(KeyHasher keyHasher, CheckTable checkTable) {
        final int maxCompactionLength = (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * BATCH_SIZE;
        @Cleanup
        val context = new TestContext(keyHasher, maxCompactionLength);

        // Create the segment and the Table Writer Processor.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);

        // Generate test data (in update & remove batches).
        val data = generateTestData(BATCH_UPDATE_COUNT, BATCH_SIZE, context);

        // Process each such batch in turn. Keep track of the removed keys, as well as of existing key versions.
        val removedKeys = new HashSet<ArrayView>();
        val keyVersions = new HashMap<ArrayView, Long>();
        Function<ArrayView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        TestBatchData last = null;
        for (val current : data) {
            // Update entries.
            val toUpdate = current.toUpdate
                    .entrySet().stream().map(e -> toUnconditionalTableEntry(e.getKey(), e.getValue(), getKeyVersion.apply(e.getKey())))
                    .collect(Collectors.toList());
            addToProcessor(
                    () -> context.ext.put(SEGMENT_NAME, toUpdate, TIMEOUT)
                                     .thenAccept(versions -> {
                                         // Update key versions.
                                         Assert.assertEquals(toUpdate.size(), versions.size());
                                         for (int i = 0; i < versions.size(); i++) {
                                             keyVersions.put(toUpdate.get(i).getKey().getKey(), versions.get(i));
                                         }
                                     }),
                    processor,
                    context.segment().getInfo()::getLength);
            removedKeys.removeAll(current.toUpdate.keySet());

            // Remove entries.
            val toRemove = current.toRemove
                    .stream().map(k -> toUnconditionalKey(k, getKeyVersion.apply(k))).collect(Collectors.toList());

            addToProcessor(() -> context.ext.remove(SEGMENT_NAME, toRemove, TIMEOUT), processor, context.segment().getInfo()::getLength);
            removedKeys.addAll(current.toRemove);
            keyVersions.keySet().removeAll(current.toRemove);

            // Flush the processor.
            Assert.assertTrue("Unexpected result from WriterTableProcessor.mustFlush().", processor.mustFlush());
            long initialLength = context.segment().getInfo().getLength();
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            if (context.segment().getInfo().getLength() > initialLength) {
                // Need to add an operation so we account for compaction and get it indexed.
                addToProcessor(initialLength, (int) (context.segment().getInfo().getLength() - initialLength), processor);
            }

            // Verify result (from cache).
            checkTable.accept(current.expectedEntries, removedKeys, context.ext);
            last = current;
        }

        // Finally, remove all data and delete the segment.
        val finalRemoval = last.expectedEntries.keySet().stream()
                                               .map(k -> toUnconditionalKey(k, 1))
                                               .collect(Collectors.toList());
        context.ext.remove(SEGMENT_NAME, finalRemoval, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        removedKeys.addAll(last.expectedEntries.keySet());
        deleteSegment(Collections.emptyList(), context.ext);
    }

    /**
     * Tests the ability to resume operations after a recovery event. Scenarios include:
     * - Index is up-to-date ({@link TableAttributes#INDEX_OFFSET} equals Segment.Length.
     * - Index is not up-to-date ({@link TableAttributes#INDEX_OFFSET} is less than Segment.Length.
     */
    @Test
    public void testRecovery() throws Exception {
        // Generate a set of TestEntryData (List<TableEntry>, ExpectedResults.
        // Process each TestEntryData in turn.  After each time, re-create the Extension.
        // Verify gets are blocked on indexing. Then index, verify unblocked and then re-create the Extension, and verify again.
        @Cleanup
        val context = new TestContext(KeyHashers.DEFAULT_HASHER);

        // Create the Segment.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();

        // Close the initial extension, as we don't need it anymore.
        context.ext.close();

        // Generate test data (in update & remove batches).
        val data = generateTestData(context);

        // Process each such batch in turn.
        for (val current : data) {
            // First, add the updates from this iteration to the index, but do not flush them. The only long-term effect
            // of this is writing the data to the Segment.
            try (val ext = context.createExtension()) {
                val toUpdate = current.toUpdate
                        .entrySet().stream().map(e -> toUnconditionalTableEntry(e.getKey(), e.getValue(), 0))
                        .collect(Collectors.toList());
                ext.put(SEGMENT_NAME, toUpdate, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                val toRemove = current.toRemove.stream().map(k -> toUnconditionalKey(k, 0)).collect(Collectors.toList());
                ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            // Create a new instance of the extension (which simulates a recovery) and verify it exhibits the correct behavior.
            try (val ext = context.createExtension()) {
                // We should have unindexed data.
                long lastIndexedOffset = context.segment().getInfo().getAttributes().get(TableAttributes.INDEX_OFFSET);
                long segmentLength = context.segment().getInfo().getLength();
                AssertExtensions.assertGreaterThan("Expected some unindexed data.", lastIndexedOffset, segmentLength);

                // Verify get requests are blocked.
                val blockedKey = current.expectedEntries.keySet().stream().findFirst().orElse(null);
                val blockedGet = ext.get(SEGMENT_NAME, Collections.singletonList(blockedKey), TIMEOUT);
                AssertExtensions.assertThrows(
                        "get() is not blocked on lack of indexing.",
                        () -> blockedGet.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                        ex -> ex instanceof TimeoutException);

                // Create, populate, and flush the processor.
                @Cleanup
                val processor = (WriterTableProcessor) ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
                addToProcessor(lastIndexedOffset, (int) (segmentLength - lastIndexedOffset), processor);
                processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertFalse("Unexpected result from WriterTableProcessor.mustFlush() after flushing.", processor.mustFlush());

                val blockedGetResult = blockedGet.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertEquals("Unexpected completion result for previously blocked get.",
                        current.expectedEntries.get(blockedKey), new HashedArray(blockedGetResult.get(0).getValue()));
            }
        }

        // Verify final result. We create yet another extension here, and purposefully do not instantiate any writer processors;
        // we want to make sure the data are accessible even without that being created (since the indexing is all caught up).
        @Cleanup
        val ext2 = context.createExtension();
        check(data.get(data.size() - 1).expectedEntries, Collections.emptyList(), ext2);
    }

    @SneakyThrows
    private void testSingleUpdates(KeyHasher hasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        @Cleanup
        val context = new TestContext(hasher);

        // Generate the keys.
        val keys = IntStream.range(0, SINGLE_UPDATE_COUNT)
                .mapToObj(i -> createRandomData(MAX_KEY_LENGTH, context))
                .collect(Collectors.toList());

        // Create the Segment.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);

        // Update.
        val expectedEntries = new HashMap<HashedArray, HashedArray>();
        val removedKeys = new ArrayList<ArrayView>();
        val keyVersions = new HashMap<ArrayView, Long>();
        Function<ArrayView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        for (val key : keys) {
            val toUpdate = generateToUpdate.apply(key, createRandomData(MAX_VALUE_LENGTH, context), getKeyVersion.apply(key));
            val updateResult = new AtomicReference<List<Long>>();
            addToProcessor(
                    () -> context.ext.put(SEGMENT_NAME, Collections.singletonList(toUpdate), TIMEOUT).thenAccept(updateResult::set),
                    processor,
                    context.segment().getInfo()::getLength);
            Assert.assertEquals("Unexpected result size from update.", 1, updateResult.get().size());
            keyVersions.put(key, updateResult.get().get(0));

            expectedEntries.put(new HashedArray(toUpdate.getKey().getKey()), new HashedArray(toUpdate.getValue()));
            check(expectedEntries, removedKeys, context.ext);
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            check(expectedEntries, removedKeys, context.ext);
        }

        checkIterators(expectedEntries, context.ext);

        // Remove.
        for (val key : keys) {
            val toRemove = generateToRemove.apply(key, getKeyVersion.apply(key));
            addToProcessor(() -> context.ext.remove(SEGMENT_NAME, Collections.singleton(toRemove), TIMEOUT), processor, context.segment().getInfo()::getLength);
            removedKeys.add(key);
            expectedEntries.remove(new HashedArray(key));
            keyVersions.remove(key);
            check(expectedEntries, removedKeys, context.ext);
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            check(expectedEntries, removedKeys, context.ext);
        }

        checkIterators(expectedEntries, context.ext);
        deleteSegment(expectedEntries.keySet(), context.ext);
    }

    private void testBatchUpdates(KeyHasher keyHasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        testBatchUpdates(BATCH_UPDATE_COUNT, BATCH_SIZE, keyHasher, generateToUpdate, generateToRemove, this::check);
    }

    @SneakyThrows
    private void testBatchUpdates(int updateCount, int maxBatchSize, KeyHasher keyHasher, EntryGenerator generateToUpdate,
                                  KeyGenerator generateToRemove, CheckTable checkTable) {
        @Cleanup
        val context = new TestContext(keyHasher);

        // Create the segment and the Table Writer Processor.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);

        // Generate test data (in update & remove batches).
        val data = generateTestData(updateCount, maxBatchSize, context);

        // Process each such batch in turn. Keep track of the removed keys, as well as of existing key versions.
        val removedKeys = new HashSet<ArrayView>();
        val keyVersions = new HashMap<ArrayView, Long>();
        Function<ArrayView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        TestBatchData last = null;
        for (val current : data) {
            // Update entries.
            val toUpdate = current.toUpdate
                    .entrySet().stream().map(e -> generateToUpdate.apply(e.getKey(), e.getValue(), getKeyVersion.apply(e.getKey())))
                    .collect(Collectors.toList());
            addToProcessor(
                    () -> context.ext.put(SEGMENT_NAME, toUpdate, TIMEOUT)
                                     .thenAccept(versions -> {
                                         // Update key versions.
                                         Assert.assertEquals(toUpdate.size(), versions.size());
                                         for (int i = 0; i < versions.size(); i++) {
                                             keyVersions.put(toUpdate.get(i).getKey().getKey(), versions.get(i));
                                         }
                                     }),
                    processor,
                    context.segment().getInfo()::getLength);
            removedKeys.removeAll(current.toUpdate.keySet());

            // Remove entries.
            val toRemove = current.toRemove
                    .stream().map(k -> generateToRemove.apply(k, getKeyVersion.apply(k))).collect(Collectors.toList());

            addToProcessor(() -> context.ext.remove(SEGMENT_NAME, toRemove, TIMEOUT), processor, context.segment().getInfo()::getLength);
            removedKeys.addAll(current.toRemove);
            keyVersions.keySet().removeAll(current.toRemove);

            // Flush the processor.
            Assert.assertTrue("Unexpected result from WriterTableProcessor.mustFlush().", processor.mustFlush());
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertFalse("Unexpected result from WriterTableProcessor.mustFlush() after flushing.", processor.mustFlush());

            // Verify result (from cache).
            checkTable.accept(current.expectedEntries, removedKeys, context.ext);

            last = current;
        }

        // At the end, wait for the data to be indexed, clear the cache and verify result.
        context.ext.close(); // Close the current instance so that we can discard the cache.

        @Cleanup
        val ext2 = context.createExtension();
        checkTable.accept(last.expectedEntries, removedKeys, ext2);

        // Finally, remove all data.
        val finalRemoval = last.expectedEntries.keySet().stream()
                .map(k -> toUnconditionalKey(k, 1))
                .collect(Collectors.toList());
        ext2.remove(SEGMENT_NAME, finalRemoval, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        removedKeys.addAll(last.expectedEntries.keySet());
        checkTable.accept(Collections.emptyMap(), removedKeys, ext2);
        deleteSegment(Collections.emptyList(), ext2);
    }

    private void deleteSegment(Collection<HashedArray> remainingKeys, ContainerTableExtension ext) throws Exception {
        if (remainingKeys.size() > 0) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "deleteIfEmpty worked on a non-empty segment.",
                    () -> ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                    ex -> ex instanceof TableSegmentNotEmptyException);
        }

        val toRemove = remainingKeys.stream().map(TableKey::unversioned).collect(Collectors.toList());
        ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void check(Map<HashedArray, HashedArray> expectedEntries, Collection<ArrayView> nonExistentKeys, ContainerTableExtension ext) throws Exception {
        // Verify that non-existing keys are not returned by accident.
        val nonExistingResult = ext.get(SEGMENT_NAME, new ArrayList<>(nonExistentKeys), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result size for non-existing key search.", nonExistentKeys.size(), nonExistingResult.size());
        Assert.assertTrue("Unexpected result for non-existing key search.", nonExistingResult.stream().allMatch(Objects::isNull));

        // Verify existing Keys.
        val expectedResult = new ArrayList<HashedArray>();
        val existingKeys = new ArrayList<ArrayView>();
        expectedEntries.forEach((k, v) -> {
            existingKeys.add(k);
            expectedResult.add(v);
        });

        val existingResult = ext.get(SEGMENT_NAME, existingKeys, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result size for existing key search.", expectedResult.size(), existingResult.size());
        for (int i = 0; i < expectedResult.size(); i++) {
            val expectedValue = expectedResult.get(i);
            val expectedKey = existingKeys.get(i);
            val actualEntry = existingResult.get(i);
            Assert.assertEquals("Unexpected key at position " + i, expectedKey, new HashedArray(actualEntry.getKey().getKey()));
            Assert.assertEquals("Unexpected value at position " + i, expectedValue, new HashedArray(actualEntry.getValue()));
        }
    }

    @SneakyThrows
    private void checkIterators(Map<HashedArray, HashedArray> expectedEntries, ContainerTableExtension ext) {
        // Collect and verify all Table Entries.
        val entryIterator = ext.entryIterator(SEGMENT_NAME, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualEntries = collectIteratorItems(entryIterator);
        actualEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));

        // Get the existing keys. We will use this to check Key Versions.
        val existingEntries = ext.get(SEGMENT_NAME, new ArrayList<>(expectedEntries.keySet()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        existingEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));
        val existingKeys = existingEntries.stream().map(TableEntry::getKey).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected Table Entries from entryIterator().", existingEntries, actualEntries, TableEntry::equals);

        // Collect and verify all Table Keys.
        val keyIterator = ext.keyIterator(SEGMENT_NAME, null, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualKeys = collectIteratorItems(keyIterator);
        actualKeys.sort(Comparator.comparingLong(TableKey::getVersion));
        AssertExtensions.assertListEquals("Unexpected Table Keys from keyIterator().", existingKeys, actualKeys, TableKey::equals);
    }

    private <T> List<T> collectIteratorItems(AsyncIterator<IteratorItem<T>> iterator) throws Exception {
        val result = new ArrayList<T>();
        val hashes = new HashSet<HashedArray>();
        iterator.forEachRemaining(item -> {
            Assert.assertTrue("Duplicate IteratorItem.getState().", hashes.add(new HashedArray(item.getState())));
            result.addAll(item.getEntries());
        }, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return result;
    }

    @SneakyThrows
    private void addToProcessor(Supplier<CompletableFuture<?>> action, WriterTableProcessor processor, Supplier<Long> getSegmentLength) {
        // Determine the length of the modified data.
        int initialLength = (int) (long) getSegmentLength.get();
        action.get().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        int newLength = (int) (long) getSegmentLength.get();
        AssertExtensions.assertGreaterThan("Expected segment length to increase.", initialLength, newLength);

        // Queue up an append operation. We don't care about the payload, as the data have already been added to the segment.
        addToProcessor(initialLength, newLength - initialLength, processor);
    }

    @SneakyThrows(DataCorruptionException.class)
    private void addToProcessor(long offset, int length, WriterTableProcessor processor) {
        val op = new StreamSegmentAppendOperation(SEGMENT_ID, new byte[length], null);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(offset);
        processor.add(new CachedStreamSegmentAppendOperation(op));
    }

    private ArrayList<TestBatchData> generateTestData(TestContext context) {
        return generateTestData(BATCH_UPDATE_COUNT, BATCH_SIZE, context);
    }

    private ArrayList<TestBatchData> generateTestData(int batchUpdateCount, int maxBatchSize, TestContext context) {
        val result = new ArrayList<TestBatchData>();
        int count = 0;
        while (count < batchUpdateCount) {
            int batchSize = Math.min(maxBatchSize, batchUpdateCount - count);
            TestBatchData prev = result.isEmpty() ? null : result.get(result.size() - 1);
            result.add(generateAndPopulateEntriesBatch(batchSize, prev, context));
            count += batchSize;
        }

        return result;
    }

    private TestBatchData generateAndPopulateEntriesBatch(int batchSize, TestBatchData previous, TestContext context) {
        val expectedEntries = previous == null
                ? new HashMap<HashedArray, HashedArray>()
                : new HashMap<>(previous.expectedEntries);

        val removalCandidates = previous == null
                ? new ArrayList<HashedArray>()
                : new ArrayList<>(expectedEntries.keySet()); // Need a list so we can efficiently pick removal candidates.

        val toUpdate = new HashMap<HashedArray, HashedArray>();
        val toRemove = new ArrayList<HashedArray>();

        for (int i = 0; i < batchSize; i++) {
            // We only generate a remove if we have something to remove.
            boolean remove = removalCandidates.size() > 0 && (context.random.nextDouble() < REMOVE_FRACTION);
            if (remove) {
                val key = removalCandidates.get(context.random.nextInt(removalCandidates.size()));
                toRemove.add(key);
                removalCandidates.remove(key);
            } else {
                // Generate a new Table Entry.
                HashedArray key = createRandomData(Math.max(1, context.random.nextInt(MAX_KEY_LENGTH)), context);
                HashedArray value = createRandomData(context.random.nextInt(MAX_VALUE_LENGTH), context);
                toUpdate.put(key, value);
                removalCandidates.add(key);
            }
        }

        expectedEntries.putAll(toUpdate);
        expectedEntries.keySet().removeAll(toRemove);
        return new TestBatchData(toUpdate, toRemove, expectedEntries);
    }

    private HashedArray createRandomData(int length, TestContext context) {
        byte[] data = new byte[length];
        context.random.nextBytes(data);
        return new HashedArray(data);
    }

    private TableEntry toConditionalTableEntry(ArrayView key, ArrayView value, long currentVersion) {
        return TableEntry.versioned(key, value, currentVersion);
    }

    private TableEntry toUnconditionalTableEntry(ArrayView key, ArrayView value, long currentVersion) {
        return TableEntry.unversioned(key, value);
    }

    private TableKey toConditionalKey(ArrayView keyData, long currentVersion) {
        return TableKey.versioned(keyData, currentVersion);
    }

    private TableKey toUnconditionalKey(ArrayView keyData, long currentVersion) {
        return TableKey.unversioned(keyData);
    }

    //region Helper Classes

    private class TestContext implements AutoCloseable {
        final KeyHasher hasher;
        final MockSegmentContainer container;
        final InMemoryCacheFactory cacheFactory;
        final CacheManager cacheManager;
        final ContainerTableExtensionImpl ext;
        final Random random;

        TestContext() {
            this(KeyHashers.DEFAULT_HASHER);
        }

        TestContext(KeyHasher hasher) {
            this(hasher, DEFAULT_COMPACTION_SIZE);
        }

        TestContext(KeyHasher hasher, int maxCompactionSize) {
            this.hasher = hasher;
            this.container = new MockSegmentContainer(() -> new SegmentMock(createSegmentMetadata(), executorService()));
            this.cacheFactory = new InMemoryCacheFactory();
            this.cacheManager = new CacheManager(CachePolicy.INFINITE, executorService());
            this.ext = createExtension(maxCompactionSize);
            this.random = new Random(0);
        }

        @Override
        public void close() {
            this.ext.close();
            this.cacheManager.close();
            this.cacheFactory.close();
            this.container.close();
        }
        ContainerTableExtensionImpl createExtension() {
            return createExtension(DEFAULT_COMPACTION_SIZE);
        }

        ContainerTableExtensionImpl createExtension(int maxCompactionSize) {
            return new TestTableExtensionImpl(this.container, this.cacheFactory, this.cacheManager, this.hasher, executorService(), maxCompactionSize);
        }

        UpdateableSegmentMetadata createSegmentMetadata() {
            val result = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, CONTAINER_ID);
            result.setLength(0);
            result.setStorageLength(0);
            return result;
        }

        SegmentMock segment() {
            return this.container.segment.get();
        }
    }

    private class TestTableExtensionImpl extends ContainerTableExtensionImpl {
        private final int maxCompactionSize;

        TestTableExtensionImpl(SegmentContainer segmentContainer, CacheFactory cacheFactory, CacheManager cacheManager,
                               KeyHasher hasher, ScheduledExecutorService executor, int maxCompactionSize) {
            super(segmentContainer, cacheFactory, cacheManager, hasher, executor);
            this.maxCompactionSize = maxCompactionSize;
        }

        @Override
        protected int getMaxCompactionSize() {
            return this.maxCompactionSize == DEFAULT_COMPACTION_SIZE ? super.getMaxCompactionSize() : this.maxCompactionSize;
        }
    }

    private class MockSegmentContainer implements SegmentContainer {
        private final AtomicReference<SegmentMock> segment;
        private final Supplier<SegmentMock> segmentCreator;
        private final AtomicBoolean closed;

        MockSegmentContainer(Supplier<SegmentMock> segmentCreator) {
            this.segmentCreator = segmentCreator;
            this.segment = new AtomicReference<>();
            this.closed = new AtomicBoolean();
        }

        @Override
        public int getId() {
            return CONTAINER_ID;
        }

        @Override
        public void close() {
            this.closed.set(true);
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> forSegment(String segmentName, Duration timeout) {
            Exceptions.checkNotClosed(this.closed.get(), this);
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }

            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            return CompletableFuture.supplyAsync(() -> segment, executorService());
        }

        @Override
        public CompletableFuture<Void> createStreamSegment(String segmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
            if (this.segment.get() != null) {
                return Futures.failedFuture(new StreamSegmentExistsException(segmentName));
            }

            return CompletableFuture
                    .runAsync(() -> {
                        SegmentMock segment = this.segmentCreator.get();
                        Assert.assertTrue(this.segment.compareAndSet(null, segment));
                    }, executorService())
                    .thenCompose(v -> this.segment.get().updateAttributes(attributes == null ? Collections.emptyList() : attributes, timeout));
        }

        @Override
        public CompletableFuture<Void> deleteStreamSegment(String segmentName, Duration timeout) {
            SegmentMock segment = this.segment.get();
            if (segment == null) {
                return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
            }
            Assert.assertEquals("Unexpected segment name.", segment.getInfo().getName(), segmentName);
            Assert.assertTrue(this.segment.compareAndSet(segment, null));
            return CompletableFuture.completedFuture(null);
        }

        //region Not Implemented Methods

        @Override
        public Collection<SegmentProperties> getActiveSegments() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service startAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public State state() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Service stopAsync() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitRunning(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void awaitTerminated(long timeout, TimeUnit unit) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public Throwable failureCause() {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public void addListener(Listener listener, Executor executor) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<SegmentProperties> mergeStreamSegment(String targetSegmentName, String sourceSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
            throw new UnsupportedOperationException("Not Expected");
        }

        @Override
        public boolean isOffline() {
            throw new UnsupportedOperationException("Not Expected");
        }

        //endregion
    }

    @RequiredArgsConstructor
    private class TestBatchData {
        /**
         * A Map of Keys to Values that need updating.
         */
        final Map<HashedArray, HashedArray> toUpdate;

        /**
         * A Collection of unique Keys that need removal.
         */
        final Collection<HashedArray> toRemove;

        /**
         * The expected result after toUpdate and toRemove have been applied. Key->Value.
         */
        final Map<HashedArray, HashedArray> expectedEntries;
    }

    @FunctionalInterface
    private interface EntryGenerator {
        TableEntry apply(ArrayView key, ArrayView value, long currentVersion);
    }

    @FunctionalInterface
    private interface KeyGenerator {
        TableKey apply(ArrayView key, long currentVersion);
    }

    @FunctionalInterface
    private interface CheckTable {
        void accept(Map<HashedArray, HashedArray> expectedEntries, Collection<ArrayView> removedKeys, ContainerTableExtension ext) throws Exception;
    }

    //endregion
}
