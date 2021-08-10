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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.BufferViewComparator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentNotEmptyException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.TestUtils;
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
import org.junit.Test;

/**
 * Base Unit tests for the {@link ContainerTableExtensionImpl} class. Derived classes may apply these tests to specific
 * implementations of {@link TableSegmentLayout}.
 */
public abstract class TableSegmentLayoutTestBase extends ThreadPooledTestSuite {
    protected static final long SEGMENT_ID = 2L;
    protected static final String SEGMENT_NAME = "TableSegment";
    protected static final int MAX_KEY_LENGTH = 128;
    protected static final int MAX_VALUE_LENGTH = 64;
    protected static final int SINGLE_UPDATE_COUNT = 10;
    protected static final int BATCH_UPDATE_COUNT = 10000;
    protected static final int BATCH_SIZE = 689;
    protected static final int ITERATOR_BATCH_UPDATE_COUNT = 1000; // Iterators are slower to check, so we reduce our test size.
    protected static final int ITERATOR_BATCH_UPDATE_SIZE = 69;
    protected static final double REMOVE_FRACTION = 0.3; // 30% of generated operations are removes.
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    //region Abstract Methods (to be implemented by derived classes).

    /**
     * When implemented in a derived class, returns the Key Length to use.
     *
     * @return The Key Length.
     */
    protected abstract int getKeyLength();

    /**
     * When implemented in a derived class, creates a new Table Segment with appropriate layout.
     *
     * @param context     TableContext.
     * @param segmentName Segment Name.
     */
    protected abstract void createSegment(TableContext context, String segmentName);

    /**
     * When implemented in a derived class, returns the expected Segment Attributes for a newly created Table Segment.
     * This is used for validating {@link #createSegment}.
     *
     * @param context TableContext.
     * @return The expected initial Segment Attributes.
     */
    protected abstract Map<AttributeId, Long> getExpectedNewSegmentAttributes(TableContext context);

    /**
     * When implemented in a derived class, returns true if delete-if-empty is supported.
     *
     * @return True if delete-if-empty is supported, false otherwise.
     */
    protected abstract boolean supportsDeleteIfEmpty();

    protected WriterTableProcessor createWriterTableProcessor(TableContext context) {
        return createWriterTableProcessor(context.ext, context);
    }

    /**
     * When implemented in a derived class, creates a {@link WriterTableProcessor} for async indexing.
     *
     * @param ext     The extension to attach to.
     * @param context TableContext.
     * @return A new {@link WriterTableProcessor}, or null if not supported ({@link #shouldExpectWriterTableProcessors()} == false).
     */
    protected abstract WriterTableProcessor createWriterTableProcessor(ContainerTableExtension ext, TableContext context);

    /**
     * When implemented in a derived class, determines if {@link WriterTableProcessor}s are expected/supported.
     *
     * @return True if {@link WriterTableProcessor}s are expected, false otherwise.
     */
    protected abstract boolean shouldExpectWriterTableProcessors();

    /**
     * When implemented in a derived class, creates an {@link IteratorArgs} for an empty Iterator (one that has already
     * served all its contents.
     *
     * @return An {@link IteratorArgs}.
     */
    protected abstract IteratorArgs createEmptyIteratorArgs();

    protected abstract void checkTableAttributes(int totalUpdateCount, int totalRemoveCount, int uniqueKeyCount, TableContext context);

    //endregion

    /**
     * Tests the ability to create and delete TableSegments.
     */
    @Test
    public void testCreateDelete() {
        @Cleanup
        val context = new TableContext(executorService());
        val nonTableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.createSegmentMetadata());
        Assert.assertTrue("Not expecting any Writer Table Processors for non-table segment.", nonTableSegmentProcessors.isEmpty());

        createSegment(context, SEGMENT_NAME);
        Assert.assertNotNull("Segment not created", context.segment());

        val expectedAttributes = getExpectedNewSegmentAttributes(context);
        val attributes = context.segment().getAttributes(expectedAttributes.keySet(), false, TIMEOUT).join();
        AssertExtensions.assertMapEquals("Unexpected compaction attributes.", expectedAttributes, attributes);

        val tableSegmentProcessors = context.ext.createWriterSegmentProcessors(context.segment().getMetadata());
        Assert.assertNotEquals("Expecting Writer Table Processors for table segment.", shouldExpectWriterTableProcessors(), tableSegmentProcessors.isEmpty());

        checkIterators(Collections.emptyMap(), context.ext);

        context.ext.deleteSegment(SEGMENT_NAME, supportsDeleteIfEmpty(), TIMEOUT).join();
        Assert.assertNull("Segment not deleted", context.segment());
        AssertExtensions.assertSuppliedFutureThrows(
                "Segment not deleted.",
                () -> context.ext.deleteSegment(SEGMENT_NAME, this.supportsDeleteIfEmpty(), TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests the ability to delete a TableSegment, but only if it is empty.
     */
    @Test
    public void testDeleteIfEmpty() {
        @Cleanup
        val context = new TableContext(executorService());
        createSegment(context, SEGMENT_NAME);
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
     * Tests operations that currently accept an offset argument, and whether they fail expectedly.
     */
    @Test
    public void testOffsetAcceptingMethods() {
        @Cleanup
        val context = new TableContext(executorService());
        createSegment(context, SEGMENT_NAME);
        val key1 = createRandomKey(context);
        val key2 = createRandomKey(context);
        val value = new ByteArraySegment("value".getBytes());
        val v1 = context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key1, value)), TIMEOUT).join();

        // key1 is present.
        Assert.assertEquals(value, context.ext.get(SEGMENT_NAME, Collections.singletonList(key1), TIMEOUT).join().get(0).getValue());
        if (supportsDeleteIfEmpty()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "deleteSegment(mustBeEmpty==true) worked with non-empty segment #1.",
                    () -> context.ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                    ex -> ex instanceof TableSegmentNotEmptyException);
        }

        val length = context.segment().getInfo().getLength();
        // The SegmentMock used does not process appends via the OperationProcessor so conditional appends are not processed accurately.
        // Instead just make sure that this 'conditional' append still appends.
        val v2 = context.ext.put(SEGMENT_NAME, Collections.singletonList(TableEntry.notExists(key2, value)), length, TIMEOUT).join();
        AssertExtensions.assertGreaterThan("Invalid entry ordering.", v1.get(0), v2.get(0));
        // Remove k1.
        context.ext.remove(SEGMENT_NAME, Collections.singletonList(TableKey.versioned(key1, v1.get(0))), TIMEOUT).join();
        //// Make sure its been removed.
        val entries = context.ext.get(SEGMENT_NAME, Collections.singletonList(key1), TIMEOUT).join();
        Assert.assertEquals(1, entries.size());
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
     * Tests the ability to perform conditional updates using a single key at a time using a {@link KeyHasher} that is
     * not prone to collisions.
     */
    @Test
    public void testSingleUpdateConditional() {
        testSingleUpdates(KeyHashers.DEFAULT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
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
     * Tests the ability to perform conditional updates and removals using a {@link KeyHasher} that is not prone to collisions.
     */
    @Test
    public void testBatchUpdatesConditional() {
        testBatchUpdates(KeyHashers.DEFAULT_HASHER, this::toConditionalTableEntry, this::toConditionalKey);
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
     * Tests the ability to update and access entries when compaction occurs using a {@link KeyHasher} that is not prone
     * to collisions.
     */
    @Test
    public void testCompaction() {
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER, this::check);
    }

    /**
     * Tests the ability to conditionally update entries when compaction occurs using a {@link KeyHasher} that is not prone
     * to collisions.
     */
    @Test
    public void testCompactionWithConditionalUpdates() {
        testTableSegmentCompacted(KeyHashers.DEFAULT_HASHER, this::conditionalUpdateCheck);
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
    protected void testTableSegmentCompacted(KeyHasher keyHasher, CheckTable checkTable) {
        val config = TableExtensionConfig.builder()
                .with(TableExtensionConfig.MAX_COMPACTION_SIZE, (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * BATCH_SIZE)
                .with(TableExtensionConfig.COMPACTION_FREQUENCY, 1)
                .build();
        @Cleanup
        val context = new TableContext(config, keyHasher, executorService());

        // Create the segment and the Table Writer Processor.
        createSegment(context, SEGMENT_NAME);
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = createWriterTableProcessor(context);

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

            if (processor != null) {
                // Background indexer Table Segment - flush the processor.
                Assert.assertTrue("Unexpected result from WriterTableProcessor.mustFlush().", processor.mustFlush());
                processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            // Verify result (from cache).
            checkTable.accept(current.expectedEntries, removedKeys, context.ext);
            last = current;
        }

        // Verify we have had at least one compaction during this test. This may happen in the background, so give it
        // some time to execute.
        TestUtils.await(() -> 0 < IndexReader.getCompactionOffset(context.segment().getInfo()), 10, TIMEOUT.toMillis());
        AssertExtensions.assertGreaterThan("No truncation occurred", 0, context.segment().getInfo().getStartOffset());

        // Finally, remove all data and delete the segment.
        val finalRemoval = last.expectedEntries.keySet().stream()
                .map(k -> toUnconditionalKey(k, 1))
                .collect(Collectors.toList());
        context.ext.remove(SEGMENT_NAME, finalRemoval, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        removedKeys.addAll(last.expectedEntries.keySet());
        deleteSegment(Collections.emptyList(), supportsDeleteIfEmpty(), context.ext);
    }

    @SneakyThrows
    protected void testSingleUpdates(KeyHasher hasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        @Cleanup
        val context = new TableContext(hasher, executorService());

        // Generate the keys.
        val keys = IntStream.range(0, SINGLE_UPDATE_COUNT)
                .mapToObj(i -> createRandomKey(context))
                .collect(Collectors.toList());

        // Create the Segment.
        createSegment(context, SEGMENT_NAME);
        @Cleanup
        val processor = createWriterTableProcessor(context);

        // Update.
        val expectedEntries = new HashMap<BufferView, BufferView>();
        val removedKeys = new ArrayList<BufferView>();
        val keyVersions = new HashMap<BufferView, Long>();
        Function<BufferView, Long> getKeyVersion = k -> keyVersions.getOrDefault(k, TableKey.NOT_EXISTS);
        int totalUpdateCount = 0;
        int totalRemoveCount = 0;
        for (val key : keys) {
            val toUpdate = generateToUpdate.apply(key, createRandomData(MAX_VALUE_LENGTH, context), getKeyVersion.apply(key));
            val updateResult = new AtomicReference<List<Long>>();
            context.ext.put(SEGMENT_NAME, Collections.singletonList(toUpdate), TIMEOUT).thenAccept(updateResult::set)
                    .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected result size from update.", 1, updateResult.get().size());
            keyVersions.put(key, updateResult.get().get(0));

            expectedEntries.put(toUpdate.getKey().getKey(), toUpdate.getValue());
            check(expectedEntries, removedKeys, context.ext);
            if (processor != null) {
                processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                check(expectedEntries, removedKeys, context.ext);
            }

            totalUpdateCount++;
            checkTableAttributes(totalUpdateCount, totalRemoveCount, totalUpdateCount, context);
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
            if (processor != null) {
                processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                check(expectedEntries, removedKeys, context.ext);
            }

            totalRemoveCount++;
            checkTableAttributes(totalUpdateCount, totalRemoveCount, totalUpdateCount - totalRemoveCount, context);
        }

        checkIterators(expectedEntries, context.ext);
        deleteSegment(expectedEntries.keySet(), supportsDeleteIfEmpty(), context.ext);
    }

    protected void testBatchUpdates(KeyHasher keyHasher, EntryGenerator generateToUpdate, KeyGenerator generateToRemove) {
        testBatchUpdates(BATCH_UPDATE_COUNT, BATCH_SIZE, keyHasher, generateToUpdate, generateToRemove, this::check);
    }

    @SneakyThrows
    protected void testBatchUpdates(int updateCount, int maxBatchSize, KeyHasher keyHasher,
                                    EntryGenerator generateToUpdate, KeyGenerator generateToRemove, CheckTable checkTable) {
        @Cleanup
        val context = new TableContext(keyHasher, executorService());

        // Create the segment and the Table Writer Processor.
        createSegment(context, SEGMENT_NAME);
        context.segment().updateAttributes(Collections.singletonMap(TableAttributes.MIN_UTILIZATION, 99L));
        @Cleanup
        val processor = createWriterTableProcessor(context);

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

            if (processor != null) {
                // Flush the processor.
                Assert.assertTrue("Unexpected result from WriterTableProcessor.mustFlush().", processor.mustFlush());
                processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertFalse("Unexpected result from WriterTableProcessor.mustFlush() after flushing.", processor.mustFlush());
            }

            // Verify result (from cache).
            checkTable.accept(current.expectedEntries, removedKeys, context.ext);
            checkTableAttributes(current.totalUpdatesCount, current.totalRemoveCount, current.expectedEntries.size(), context);
            last = current;
        }

        // At the end, wait for the data to be indexed, clear the cache and verify result.
        context.ext.close(); // Close the current instance so that we can discard the cache.

        @Cleanup
        val ext2 = context.createExtension();
        @Cleanup
        val processor2 = createWriterTableProcessor(ext2, context);
        checkTable.accept(last.expectedEntries, removedKeys, ext2);

        // Finally, remove all data.
        val finalRemoval = last.expectedEntries.keySet().stream()
                .map(k -> toUnconditionalKey(k, 1))
                .collect(Collectors.toList());
        ext2.remove(SEGMENT_NAME, finalRemoval, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        removedKeys.addAll(last.expectedEntries.keySet());
        if (processor2 != null) {
            processor2.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }
        checkTable.accept(Collections.emptyMap(), removedKeys, ext2);
        deleteSegment(Collections.emptyList(), supportsDeleteIfEmpty(), ext2);
    }

    private void deleteSegment(Collection<BufferView> remainingKeys, boolean mustBeEmpty, ContainerTableExtension ext) throws Exception {
        if (remainingKeys.size() > 0 && supportsDeleteIfEmpty()) {
            AssertExtensions.assertSuppliedFutureThrows(
                    "deleteIfEmpty worked on a non-empty segment.",
                    () -> ext.deleteSegment(SEGMENT_NAME, true, TIMEOUT),
                    ex -> ex instanceof TableSegmentNotEmptyException);
        }

        val toRemove = remainingKeys.stream().map(TableKey::unversioned).collect(Collectors.toList());
        ext.remove(SEGMENT_NAME, toRemove, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        ext.deleteSegment(SEGMENT_NAME, mustBeEmpty, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void conditionalUpdateCheck(Map<BufferView, BufferView> expectedEntries, Collection<BufferView> nonExistentKeys, ContainerTableExtension ext) throws Exception {
        for (val e : expectedEntries.entrySet()) {
            val te = TableEntry.versioned(e.getKey(), e.getValue(), 1234L);
            AssertExtensions.assertSuppliedFutureThrows(
                    "Expected the update to have failed with BadKeyVersionException.",
                    () -> ext.put(SEGMENT_NAME, Collections.singletonList(te), TIMEOUT),
                    ex -> ex instanceof BadKeyVersionException);
        }
    }

    protected void check(Map<BufferView, BufferView> expectedEntries, Collection<BufferView> nonExistentKeys, ContainerTableExtension ext) throws Exception {
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

        val info = ext.getInfo(SEGMENT_NAME, TIMEOUT).join();
        Assert.assertEquals("Unexpected entry count for " + info, expectedEntries.size(), info.getEntryCount());
    }

    @SneakyThrows
    protected void checkIterators(Map<BufferView, BufferView> expectedEntries, ContainerTableExtension ext) {
        val emptyIteratorArgs = createEmptyIteratorArgs();
        // Check that invalid serializer state is handled properly.
        val emptyEntryIterator = ext.entryIterator(SEGMENT_NAME, emptyIteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualEmptyEntries = collectIteratorItems(emptyEntryIterator);
        Assert.assertEquals("Unexpected entries returned.", 0, actualEmptyEntries.size());

        val iteratorArgs = IteratorArgs.builder().fetchTimeout(TIMEOUT).build();
        // Collect and verify all Table Entries.
        val entryIterator = ext.entryIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualEntries = collectIteratorItems(entryIterator);

        // When we check Entry Iterator, we order by Version and verify that versions match. Entry Iterators also return
        // versions so it's important that we check those.
        actualEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));

        // Get the existing keys. We will use this to check Key Versions.
        val existingEntries = ext.get(SEGMENT_NAME, new ArrayList<>(expectedEntries.keySet()), TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        existingEntries.sort(Comparator.comparingLong(e -> e.getKey().getVersion()));
        AssertExtensions.assertListEquals("Unexpected Table Entries from entryIterator().", existingEntries, actualEntries, TableEntry::equals);

        // Collect and verify all Table Keys. We now need to sort by Key, as Key Iterators do not return Version.
        val c = BufferViewComparator.create();
        val existingKeys = existingEntries.stream()
                .sorted((e1, e2) -> c.compare(e1.getKey().getKey(), e2.getKey().getKey()))
                .map(TableEntry::getKey).collect(Collectors.toList());
        val keyIterator = ext.keyIterator(SEGMENT_NAME, iteratorArgs).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val actualKeys = collectIteratorItems(keyIterator);
        actualKeys.sort((e1, e2) -> c.compare(e1.getKey(), e2.getKey()));
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


    protected ArrayList<TestBatchData> generateTestData(TableContext context) {
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
                BufferView key = createRandomKey(context);
                BufferView value = createRandomData(context.random.nextInt(MAX_VALUE_LENGTH), context);
                toUpdate.put(key, value);
                removalCandidates.add(key);
            }
        }

        expectedEntries.putAll(toUpdate);
        expectedEntries.keySet().removeAll(toRemove);

        // Keep track of expected metrics.
        int totalUpdateCount = toUpdate.size();
        int totalRemoveCount = toRemove.size();
        if (previous != null) {
            totalUpdateCount += previous.totalUpdatesCount;
            totalRemoveCount += previous.totalRemoveCount;
        }

        return new TestBatchData(toUpdate, toRemove, expectedEntries, totalUpdateCount, totalRemoveCount);
    }

    protected BufferView createRandomKey(TableContext context) {
        return createRandomData(getKeyLength(), context);
    }

    protected BufferView createRandomKeyRandomLength(TableContext context) {
        return createRandomData(Math.max(1, context.random.nextInt(MAX_KEY_LENGTH)), context);
    }

    protected BufferView createRandomData(int length, TableContext context) {
        byte[] data = new byte[length];
        context.random.nextBytes(data);
        return new ByteArraySegment(data);
    }

    protected TableEntry toConditionalTableEntry(BufferView key, BufferView value, long currentVersion) {
        return TableEntry.versioned(key, value, currentVersion);
    }

    protected TableEntry toUnconditionalTableEntry(BufferView key, BufferView value, long currentVersion) {
        return TableEntry.unversioned(key, value);
    }

    protected TableKey toConditionalKey(BufferView keyData, long currentVersion) {
        return TableKey.versioned(keyData, currentVersion);
    }

    protected TableKey toUnconditionalKey(BufferView keyData, long currentVersion) {
        return TableKey.unversioned(keyData);
    }

    //region Helper Classes

    @RequiredArgsConstructor
    protected static class TestBatchData {
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

        /**
         * The accumulated total update count so far.
         */
        final int totalUpdatesCount;

        /**
         * The accumulated total remove count so far.
         */
        final int totalRemoveCount;
    }

    @FunctionalInterface
    protected interface EntryGenerator {
        TableEntry apply(BufferView key, BufferView value, long currentVersion);
    }

    @FunctionalInterface
    protected interface KeyGenerator {
        TableKey apply(BufferView key, long currentVersion);
    }

    @FunctionalInterface
    protected interface CheckTable {
        void accept(Map<BufferView, BufferView> expectedEntries, Collection<BufferView> removedKeys, ContainerTableExtension ext) throws Exception;
    }

    //endregion
}
