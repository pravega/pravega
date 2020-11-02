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

import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
    private static final int DEFAULT_COMPACTION_SIZE = -1; // Inherits from parent.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Comparator<BufferView> KEY_COMPARATOR = BufferViewComparator.create()::compare;
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
        val context = new TableContext(DEFAULT_COMPACTION_SIZE, executorService());
        val nonTableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.createSegmentMetadata());
        Assert.assertTrue("Not expecting any Writer Table Processors for non-table segment.", nonTableSegmentProcessors.isEmpty());

        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertNotNull("Segment not created", context.segment());

        val attributes = context.segment().getAttributes(ContainerTableExtensionImpl.DEFAULT_COMPACTION_ATTRIBUTES.keySet(), false, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected compaction attributes.", ContainerTableExtensionImpl.DEFAULT_COMPACTION_ATTRIBUTES, attributes);

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
     * Tests to make sure that any invalid state passed to an iterator during instantiation is handled accordingly.
     */
    @Test
    public void testInvalidIteratorState() {
        @Cleanup
        val context = new TableContext(executorService());
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        val iteratorArgs = IteratorArgs
                .builder()
                .fetchTimeout(TIMEOUT)
                .serializedState(new ByteArraySegment("INVALID".getBytes()))
                .build();
        AssertExtensions.assertThrows("Invalid entryIterator state.",
                () -> context.ext.entryIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof IllegalDataFormatException);
    }

    /**
     * Tests the ability to delete a TableSegment, but only if it is empty.
     */
    @Test
    public void testDeleteIfEmpty() {
        @Cleanup
        val context = new TableContext(DEFAULT_COMPACTION_SIZE, executorService());
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        val key1 = new ByteArraySegment("key1".getBytes());
        val key2 = new ByteArraySegment("key2".getBytes());
        val value = new ByteArraySegment("value".getBytes());
        context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key1, value)), TIMEOUT).join();

        // key1 is present.
        AssertExtensions.assertSuppliedFutureThrows(
                "deleteSegment(mustBeEmpty==true) worked with non-empty segment #1.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof TableSegmentNotEmptyException);

        // Remove key1 and insert key2.
        context.ext.remove(SEGMENT_NAME, Collections.singleton(TableKey.unversioned(key1)), TIMEOUT).join();
        context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key2, value)), TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "deleteSegment(mustBeEmpty==true) worked with non-empty segment #2.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof TableSegmentNotEmptyException);

        // Remove key2 and reinsert it.
        context.ext.remove(SEGMENT_NAME, Collections.singleton(TableKey.unversioned(key2)), TIMEOUT).join();
        context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key2, value)), TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "deleteSegment(mustBeEmpty==true) worked with non-empty segment #3.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof TableSegmentNotEmptyException);

        // Remove key2.
        context.ext.remove(SEGMENT_NAME, Collections.singleton(TableKey.unversioned(key2)), TIMEOUT).join();
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
        val context = new TableContext(DEFAULT_COMPACTION_SIZE, executorService());
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
     * Tests operations that currently accept an offset argument, and whether they fail expectedly.
     */
    @Test
    public void testOffsetAcceptingMethods() {
        @Cleanup
        val context = new TableContext(executorService());
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        val key1 = new ByteArraySegment("key1".getBytes());
        val key2 = new ByteArraySegment("key2".getBytes());
        val value = new ByteArraySegment("value".getBytes());
        val v1 = context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key1, value)), TIMEOUT).join();

        // key1 is present.
        AssertExtensions.assertSuppliedFutureThrows(
                "deleteSegment(mustBeEmpty==true) worked with non-empty segment #1.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                ex -> ex instanceof TableSegmentNotEmptyException);

        val length = context.segment().getInfo().getLength();
        // The SegmentMock used does not process appends via the OperationProcessor so conditional appends are not processed accurately.
        // Instead just make sure that this 'conditional' append still appends.
        val v2 = context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key2, value)), length, TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Invalid entry ordering.", v1.get(0), v2.get(0));
        // Remove k1.
        context.ext.remove(SEGMENT_NAME, Collections.singletonList(TableKey.versioned(key1, v1.get(0))), TIMEOUT).join();
        //// Make sure its been removed.
        val entries = context.ext.get(SEGMENT_NAME, Collections.singletonList(key1), TIMEOUT).join();
        Assert.assertTrue(entries.size() == 1);
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
                false,
                KeyHashers.DEFAULT_HASHER,
                this::toUnconditionalTableEntry,
                this::toUnconditionalKey,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    /**
     * Tests the ability to iterate over keys or entries of a Sorted Table Segment.
     */
    @Test
    public void testIteratorsSorted() {
        testBatchUpdates(
                ITERATOR_BATCH_UPDATE_COUNT,
                ITERATOR_BATCH_UPDATE_SIZE,
                true,
                KeyHashers.DEFAULT_HASHER,
                this::toUnconditionalTableEntry,
                this::toUnconditionalKey,
                (expectedEntries, removedKeys, ext) -> checkIteratorsSorted(expectedEntries, ext));
    }

    /**
     * Tests the ability to iterate over keys or entries using a {@link KeyHasher} that is very prone to collisions.
     */
    @Test
    public void testIteratorsWithCollisions() {
        testBatchUpdates(
                ITERATOR_BATCH_UPDATE_COUNT,
                ITERATOR_BATCH_UPDATE_SIZE,
                false,
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
    public void testCompaction() {
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER, this::check);
    }

    /**
     * Tests the ability update and access entries when compaction occurs using a {@link KeyHasher} that is very prone
     * to collisions.
     */
    @Test
    public void testCompactionWithCollisions() {
        testTableSegmentCompacted(KeyHashers.COLLISION_HASHER, this::check);
    }

    /**
     * Tests the ability iterate over entries when compaction occurs using a {@link KeyHasher} that is not prone
     * to collisions.
     */
    @Test
    public void testCompactionWithIterators() {
        // Normally this shouldn't be affected; the iteration does not include the cache - it iterates over the index
        // directly.
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER,
                (expectedEntries, removedKeys, ext) -> checkIterators(expectedEntries, ext));
    }

    @SneakyThrows
    private void testTableSegmentCompacted(KeyHasher keyHasher, CheckTable checkTable) {
        final int maxCompactionLength = (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * BATCH_SIZE;
        @Cleanup
        val context = new TableContext(maxCompactionLength, executorService());

        // Create the segment and the Table Writer Processor.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);
        context.segment().setAppendCallback((offset, length) -> addToProcessor(offset, length, processor));

        // Generate test data (in update & remove batches).
        val data = generateTestData(BATCH_UPDATE_COUNT, BATCH_SIZE, context);

        // Process each such batch in turn. Keep track of the removed keys, as well as of existing key versions.
        val removedKeys = new HashSet<BufferView>();
        val keyVersions = new HashMap<BufferView, Long>();
        Function<BufferView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        TestBatchData last = null;
        for (val current : data) {
            // Update entries.
            val toUpdate = current.toUpdate
                    .entrySet().stream().map(e -> toUnconditionalTableEntry(e.getKey(), e.getValue(), getKeyVersion.apply(e.getKey())))
                    .collect(Collectors.toList());
            context.ext.put(SEGMENT_NAME, toUpdate, TIMEOUT)
                    .thenAccept(versions -> {
                        // Update key versions.
                        Assert.assertEquals(toUpdate.size(), versions.size());
                        for (int i = 0; i < versions.size(); i++) {
                            keyVersions.put(toUpdate.get(i).getKey().getKey(), versions.get(i));
                        }
                    }).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            removedKeys.removeAll(current.toUpdate.keySet());

            // Remove entries.
            val toRemove = current.toRemove
                    .stream().map(k -> toUnconditionalKey(k, getKeyVersion.apply(k))).collect(Collectors.toList());

            context.ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            removedKeys.addAll(current.toRemove);
            keyVersions.keySet().removeAll(current.toRemove);

            // Flush the processor.
            Assert.assertTrue("Unexpected result from WriterTableProcessor.mustFlush().", processor.mustFlush());
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Verify result (from cache).
            checkTable.accept(current.expectedEntries, removedKeys, context.ext);
            last = current;
        }

        // Verify we have had at least one compaction during this test.
        val ir = new IndexReader(executorService());
        AssertExtensions.assertGreaterThan("No compaction occurred.", 0, ir.getCompactionOffset(context.segment().getInfo()));
        AssertExtensions.assertGreaterThan("No truncation occurred", 0, context.segment().getInfo().getStartOffset());

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
        val context = new TableContext(executorService());

        // Create the Segment.
        context.ext.createSegment(SEGMENT_NAME, true, TIMEOUT).join();

        // Close the initial extension, as we don't need it anymore.
        context.ext.close();

        // Generate test data (in update & remove batches).
        val data = generateTestData(context);

        // Process each such batch in turn.
        for (int i = 0; i < data.size(); i++) {
            val current = data.get(i);

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

                boolean useProcessor = i % 2 == 0; // This ensures that last iteration uses the processor.

                // Verify get requests are blocked.
                val key1 = current.expectedEntries.keySet().stream().findFirst().orElse(null);
                val get1 = ext.get(SEGMENT_NAME, Collections.singletonList(key1), TIMEOUT);
                val getResult1 = get1.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertEquals("Unexpected completion result for recovered get.",
                        current.expectedEntries.get(key1), getResult1.get(0).getValue());

                if (useProcessor) {
                    // Create, populate, and flush the processor.
                    @Cleanup
                    val processor = (WriterTableProcessor) ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
                    addToProcessor(lastIndexedOffset, (int) (segmentLength - lastIndexedOffset), processor);
                    processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    Assert.assertFalse("Unexpected result from WriterTableProcessor.mustFlush() after flushing.", processor.mustFlush());
                }
            }
        }

        // Verify final result. We create yet another extension here, and purposefully do not instantiate any writer processors;
        // we want to make sure the data are accessible even without that being created (since the indexing is all caught up).
        @Cleanup
        val ext2 = context.createExtension();
        check(data.get(data.size() - 1).expectedEntries, Collections.emptyList(), ext2);
        checkIteratorsSorted(data.get(data.size() - 1).expectedEntries, ext2);
    }

    @SneakyThrows
    private void testSingleUpdates(KeyHasher hasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        @Cleanup
        val context = new TableContext(DEFAULT_COMPACTION_SIZE, executorService());

        // Generate the keys.
        val keys = IntStream.range(0, SINGLE_UPDATE_COUNT)
                .mapToObj(i -> createRandomData(MAX_KEY_LENGTH, context))
                .collect(Collectors.toList());

        // Create the Segment.
        context.ext.createSegment(SEGMENT_NAME, TIMEOUT).join();
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);
        context.segment().setAppendCallback((offset, length) -> addToProcessor(offset, length, processor));

        // Update.
        val expectedEntries = new HashMap<BufferView, BufferView>();
        val removedKeys = new ArrayList<BufferView>();
        val keyVersions = new HashMap<BufferView, Long>();
        Function<BufferView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        for (val key : keys) {
            val toUpdate = generateToUpdate.apply(key, createRandomData(MAX_VALUE_LENGTH, context), getKeyVersion.apply(key));
            val updateResult = new AtomicReference<List<Long>>();
            context.ext.put(SEGMENT_NAME, Collections.singletonList(toUpdate), TIMEOUT).thenAccept(updateResult::set)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected result size from update.", 1, updateResult.get().size());
            keyVersions.put(key, updateResult.get().get(0));

            expectedEntries.put(toUpdate.getKey().getKey(), toUpdate.getValue());
            check(expectedEntries, removedKeys, context.ext);
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            check(expectedEntries, removedKeys, context.ext);
        }

        checkIterators(expectedEntries, context.ext);

        // Remove.
        for (val key : keys) {
            val toRemove = generateToRemove.apply(key, getKeyVersion.apply(key));
            context.ext.remove(SEGMENT_NAME, Collections.singleton(toRemove), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            removedKeys.add(key);
            expectedEntries.remove(key);
            keyVersions.remove(key);
            check(expectedEntries, removedKeys, context.ext);
            processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            check(expectedEntries, removedKeys, context.ext);
        }

        checkIterators(expectedEntries, context.ext);
        deleteSegment(expectedEntries.keySet(), context.ext);
    }

    private void testBatchUpdates(KeyHasher keyHasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        testBatchUpdates(BATCH_UPDATE_COUNT, BATCH_SIZE, false, keyHasher, generateToUpdate, generateToRemove, this::check);
    }

    @SneakyThrows
    private void testBatchUpdates(int updateCount, int maxBatchSize, boolean sortedTableSegment, KeyHasher keyHasher,
                                  EntryGenerator generateToUpdate, KeyGenerator generateToRemove, CheckTable checkTable) {
        @Cleanup
        val context = new TableContext(DEFAULT_COMPACTION_SIZE, executorService());

        // Create the segment and the Table Writer Processor. We make `sortedTableSegment` configurable because some tests
        // explicitly test the offsets that are written to the segment, which would be very hard to do in the presence of
        // sorted table segment indexing.
        context.ext.createSegment(SEGMENT_NAME, sortedTableSegment, TIMEOUT).join();
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = (WriterTableProcessor) context.ext.createWriterSegmentProcessors(context.segment().getMetadata()).stream().findFirst().orElse(null);
        Assert.assertNotNull(processor);
        context.segment().setAppendCallback((offset, length) -> addToProcessor(offset, length, processor));

        // Generate test data (in update & remove batches).
        val data = generateTestData(updateCount, maxBatchSize, context);

        // Process each such batch in turn. Keep track of the removed keys, as well as of existing key versions.
        val removedKeys = new HashSet<BufferView>();
        val keyVersions = new HashMap<BufferView, Long>();
        Function<BufferView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        TestBatchData last = null;
        for (val current : data) {
            // Update entries.
            val toUpdate = current.toUpdate
                    .entrySet().stream().map(e -> generateToUpdate.apply(e.getKey(), e.getValue(), getKeyVersion.apply(e.getKey())))
                    .collect(Collectors.toList());
            context.ext.put(SEGMENT_NAME, toUpdate, TIMEOUT)
                    .thenAccept(versions -> {
                        // Update key versions.
                        Assert.assertEquals(toUpdate.size(), versions.size());
                        for (int i = 0; i < versions.size(); i++) {
                            keyVersions.put(toUpdate.get(i).getKey().getKey(), versions.get(i));
                        }
                    }).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            removedKeys.removeAll(current.toUpdate.keySet());

            // Remove entries.
            val toRemove = current.toRemove
                    .stream().map(k -> generateToRemove.apply(k, getKeyVersion.apply(k))).collect(Collectors.toList());

            context.ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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

        if (sortedTableSegment) {
            // Sorted table segments do not support must-be-empty as a condition.
            AssertExtensions.assertSuppliedFutureThrows(
                    "deleteIfEmpty worked on a sorted table segment.",
                    () -> ext2.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                    ex -> ex instanceof TableSegmentNotEmptyException);
            deleteSegment(Collections.emptyList(), false, ext2);
        } else {
            deleteSegment(Collections.emptyList(), ext2);
        }
    }

    private void deleteSegment(Collection<BufferView> remainingKeys, ContainerTableExtension ext) throws Exception {
        deleteSegment(remainingKeys, true, ext);
    }

    private void deleteSegment(Collection<BufferView> remainingKeys, boolean mustBeEmpty, ContainerTableExtension ext) throws Exception {
        if (remainingKeys.size() > 0) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "deleteIfEmpty worked on a non-empty segment.",
                    () -> ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                    ex -> ex instanceof TableSegmentNotEmptyException);
        }

        val toRemove = remainingKeys.stream().map(TableKey::unversioned).collect(Collectors.toList());
        ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        ext.deleteSegment(SEGMENT_NAME, mustBeEmpty, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void check(Map<BufferView, BufferView> expectedEntries, Collection<BufferView> nonExistentKeys, ContainerTableExtension ext) throws Exception {
        // Verify that non-existing keys are not returned by accident.
        val nonExistingResult = ext.get(SEGMENT_NAME, new ArrayList<>(nonExistentKeys), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result size for non-existing key search.", nonExistentKeys.size(), nonExistingResult.size());
        Assert.assertTrue("Unexpected result for non-existing key search.", nonExistingResult.stream().allMatch(Objects::isNull));

        // Verify existing Keys.
        val expectedResult = new ArrayList<BufferView>();
        val existingKeys = new ArrayList<BufferView>();
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
            Assert.assertEquals("Unexpected key at position " + i, expectedKey, actualEntry.getKey().getKey());
            Assert.assertEquals("Unexpected value at position " + i, expectedValue, actualEntry.getValue());
        }
    }

    @SneakyThrows
    private void checkIterators(Map<BufferView, BufferView> expectedEntries, ContainerTableExtension ext) {
        val emptyIteratorArgs = IteratorArgs
                .builder()
                .serializedState(new IteratorStateImpl(KeyHasher.MAX_HASH).serialize())
                .fetchTimeout(TIMEOUT)
                .build();
        // Check that invalid serializer state is handled properly.
        val emptyEntryIterator = ext.entryIterator(SEGMENT_NAME, emptyIteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualEmptyEntries = collectIteratorItems(emptyEntryIterator);
        Assert.assertTrue("Unexpected entries returned.", actualEmptyEntries.size() == 0);

        val iteratorArgs = IteratorArgs.builder().fetchTimeout(TIMEOUT).build();
        // Collect and verify all Table Entries.
        val entryIterator = ext.entryIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualEntries = collectIteratorItems(entryIterator);
        actualEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));

        // Get the existing keys. We will use this to check Key Versions.
        val existingEntries = ext.get(SEGMENT_NAME, new ArrayList<>(expectedEntries.keySet()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        existingEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));
        val existingKeys = existingEntries.stream().map(TableEntry::getKey).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected Table Entries from entryIterator().", existingEntries, actualEntries, TableEntry::equals);

        // Collect and verify all Table Keys.
        val keyIterator = ext.keyIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualKeys = collectIteratorItems(keyIterator);
        actualKeys.sort(Comparator.comparingLong(TableKey::getVersion));
        AssertExtensions.assertListEquals("Unexpected Table Keys from keyIterator().", existingKeys, actualKeys, TableKey::equals);
    }

    @SneakyThrows
    private void checkIteratorsSorted(Map<BufferView, BufferView> expectedEntries, ContainerTableExtension ext) {
        val iteratorArgs = IteratorArgs.builder().fetchTimeout(TIMEOUT).build();

        // Collect and verify all Table Entries.
        val actualEntries = collectIteratorItems(ext.entryIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));

        // Get the existing entries and sort them.
        val existingEntries = ext.get(SEGMENT_NAME, new ArrayList<>(expectedEntries.keySet()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        existingEntries.sort((e1, e2) -> KEY_COMPARATOR.compare(e1.getKey().getKey(), e2.getKey().getKey()));

        // Extract the keys from the entries. They should still be sorted.
        val existingKeys = existingEntries.stream().map(TableEntry::getKey).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected Table Entries from sorted entryIterator().",
                existingEntries, actualEntries, TableEntry::equals);

        // Collect and verify all Table Keys. KeyIterator does not return versions for sorted table segments, so we'll
        // need to only check key equality.
        val actualKeys = collectIteratorItems(ext.keyIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        AssertExtensions.assertListEquals("Unexpected Table Keys from keyIterator().", existingKeys, actualKeys,
                (k1, k2) -> k1.getKey().equals(k2.getKey()));
    }

    private <T> List<T> collectIteratorItems(AsyncIterator<IteratorItem<T>> iterator) throws Exception {
        val result = new ArrayList<T>();
        val hashes = new HashSet<BufferView>();
        iterator.forEachRemaining(item -> {
            Assert.assertTrue("Duplicate IteratorItem.getState().", hashes.add(item.getState()));
            result.addAll(item.getEntries());
        }, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        return result;
    }

    @SneakyThrows(DataCorruptionException.class)
    private void addToProcessor(long offset, int length, WriterTableProcessor processor) {
        val op = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(new byte[length]), null);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(offset);
        processor.add(new CachedStreamSegmentAppendOperation(op));
    }

    private ArrayList<TestBatchData> generateTestData(TableContext context) {
        return generateTestData(BATCH_UPDATE_COUNT, BATCH_SIZE, context);
    }

    private ArrayList<TestBatchData> generateTestData(int batchUpdateCount, int maxBatchSize, TableContext context) {
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

    private TestBatchData generateAndPopulateEntriesBatch(int batchSize, TestBatchData previous, TableContext context) {
        val expectedEntries = previous == null
                ? new HashMap<BufferView, BufferView>()
                : new HashMap<>(previous.expectedEntries);

        val removalCandidates = previous == null
                ? new ArrayList<BufferView>()
                : new ArrayList<>(expectedEntries.keySet()); // Need a list so we can efficiently pick removal candidates.

        val toUpdate = new HashMap<BufferView, BufferView>();
        val toRemove = new ArrayList<BufferView>();

        for (int i = 0; i < batchSize; i++) {
            // We only generate a remove if we have something to remove.
            boolean remove = removalCandidates.size() > 0 && (context.random.nextDouble() < REMOVE_FRACTION);
            if (remove) {
                val key = removalCandidates.get(context.random.nextInt(removalCandidates.size()));
                toRemove.add(key);
                removalCandidates.remove(key);
            } else {
                // Generate a new Table Entry.
                BufferView key = createRandomData(Math.max(1, context.random.nextInt(MAX_KEY_LENGTH)), context);
                BufferView value = createRandomData(context.random.nextInt(MAX_VALUE_LENGTH), context);
                toUpdate.put(key, value);
                removalCandidates.add(key);
            }
        }

        expectedEntries.putAll(toUpdate);
        expectedEntries.keySet().removeAll(toRemove);
        return new TestBatchData(toUpdate, toRemove, expectedEntries);
    }

    private BufferView createRandomData(int length, TableContext context) {
        byte[] data = new byte[length];
        context.random.nextBytes(data);
        return new ByteArraySegment(data);
    }

    private TableEntry toConditionalTableEntry(BufferView key, BufferView value, long currentVersion) {
        return TableEntry.versioned(key, value, currentVersion);
    }

    private TableEntry toUnconditionalTableEntry(BufferView key, BufferView value, long currentVersion) {
        return TableEntry.unversioned(key, value);
    }

    private TableKey toConditionalKey(BufferView keyData, long currentVersion) {
        return TableKey.versioned(keyData, currentVersion);
    }

    private TableKey toUnconditionalKey(BufferView keyData, long currentVersion) {
        return TableKey.unversioned(keyData);
    }

    //region Helper Classes

    @RequiredArgsConstructor
    private class TestBatchData {
        /**
         * A Map of Keys to Values that need updating.
         */
        final Map<BufferView, BufferView> toUpdate;

        /**
         * A Collection of unique Keys that need removal.
         */
        final Collection<BufferView> toRemove;

        /**
         * The expected result after toUpdate and toRemove have been applied. Key->Value.
         */
        final Map<BufferView, BufferView> expectedEntries;
    }

    @FunctionalInterface
    private interface EntryGenerator {
        TableEntry apply(BufferView key, BufferView value, long currentVersion);
    }

    @FunctionalInterface
    private interface KeyGenerator {
        TableKey apply(BufferView key, long currentVersion);
    }

    @FunctionalInterface
    private interface CheckTable {
        void accept(Map<BufferView, BufferView> expectedEntries, Collection<BufferView> removedKeys, ContainerTableExtension ext) throws Exception;
    }

    //endregion
}
