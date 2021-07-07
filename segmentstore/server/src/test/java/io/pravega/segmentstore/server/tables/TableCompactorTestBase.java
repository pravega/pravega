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

import com.google.common.collect.ImmutableMap;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test base for any class implementing {@link TableCompactor}.
 */
abstract class TableCompactorTestBase extends ThreadPooledTestSuite {
    protected static final int KEY_COUNT = 100; // Number of distinct keys in the tests, numbered 0 to KEY_COUNT-1.
    protected static final int KEY_LENGTH = 32;
    protected static final int VALUE_LENGTH = 64;
    protected static final int UPDATE_ENTRY_LENGTH = KEY_LENGTH + VALUE_LENGTH + EntrySerializer.HEADER_LENGTH;
    protected static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final int SKIP_COUNT = 5; // At each iteration i, we update all keys K>=i*SKIP_COUNT+DELETE_COUNT.
    private static final int DELETE_COUNT = 1; // At each iteration i, we remove keys K>=i*SKIP_COUNT to K<i*SKIP_COUNT+DELETE_COUNT
    private static final String SEGMENT_NAME = "TableSegment";

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    protected abstract TestContext createContext(int maxCompactionLength);

    /**
     * Tests the {@link TableCompactor#isCompactionRequired} method.
     */
    @Test
    public void testIsCompactionRequired() {
        final int compactionReadLength = 100; // This also determines whether to compact or not.
        @Cleanup
        val c = createContext(compactionReadLength);
        c.segmentMetadata.setLength(compactionReadLength);

        // TruncationOffset < Compaction offset, and CompactionOffset >= LastIndexedOffset.
        c.setSegmentState(50, 50, 1, 100, 50);
        Assert.assertFalse("Unexpected result when CompactionOffset is equal to IndexOffset.", c.getCompactor().isCompactionRequired().join());

        // TruncationOffset > Compaction offset, and TruncationOffset >= LastIndexedOffset.
        c.segmentMetadata.setStartOffset(50);
        c.setSegmentState(10, 50, 1, 100, 50);
        Assert.assertFalse("Unexpected result when TruncationOffset is equal to IndexOffset.", c.getCompactor().isCompactionRequired().join());

        // Utilization == Threshold
        c.setSegmentState(0, 100, 50, 100, 50);
        Assert.assertFalse("Unexpected result when Utilization==MinUtilization.", c.getCompactor().isCompactionRequired().join());

        // Utilization > Threshold
        c.setSegmentState(0, 100, 51, 100, 50);
        Assert.assertFalse("Unexpected result when Utilization>MinUtilization.", c.getCompactor().isCompactionRequired().join());

        // Empty table
        c.setSegmentState(0, 100, 10, 0, 50);
        Assert.assertFalse("Unexpected result when TotalEntryCount==0.", c.getCompactor().isCompactionRequired().join());

        // Utilization < Threshold, but not enough "uncompacted" length.
        c.setSegmentState(0, 100, 49, 100, 50);
        Assert.assertFalse("Unexpected result when Utilization>MinUtilization.", c.getCompactor().isCompactionRequired().join());

        // Utilization < Threshold, and enough "uncompacted" length (IndexLength-Max(StartOffset,CompactOffset))>ReadLength.
        c.segmentMetadata.setLength(Math.max(c.segmentMetadata.getLength(), c.segmentMetadata.getStartOffset() + 151));
        c.setSegmentState(0, 151, 49, 100, 50);
        Assert.assertTrue("Unexpected result when Utilization>MinUtilization.", c.getCompactor().isCompactionRequired().join());
    }

    /**
     * Tests the {@link TableCompactor#calculateTruncationOffset} method.
     */
    @Test
    public void testCalculateTruncationOffset() {
        final long noOffset = -1;
        long compactionOffset = 100;
        @Cleanup
        val c = createContext(UPDATE_ENTRY_LENGTH);
        c.segmentMetadata.setLength(250);
        c.setSegmentState(compactionOffset, 200, 1, 1, 100);

        // 1. If we encountered compacted items during indexing.
        Assert.assertEquals("Unexpected result when highestCopiedOffset>0.",
                1, c.getCompactor().calculateTruncationOffset(1));
        Assert.assertEquals("Unexpected result when highestCopiedOffset>0.",
                101, c.getCompactor().calculateTruncationOffset(101));

        // 2. No compacted items during indexing.
        if (c.hasDelayedIndexing()) {
            // Segment is not fully indexed.
            c.setLastIndexedOffset(c.segmentMetadata.getLength() - 1);
            Assert.assertEquals("Unexpected result when segment not fully indexed.",
                    noOffset, c.getCompactor().calculateTruncationOffset(0));
        }

        // Segment is fully indexed, but segment is already truncated at compaction offset.
        c.setLastIndexedOffset(c.segmentMetadata.getLength());
        c.segmentMetadata.setStartOffset(compactionOffset);
        Assert.assertEquals("Unexpected result when segment already truncated at compaction offset.",
                noOffset, c.getCompactor().calculateTruncationOffset(0));

        // Segment is fully indexed, and COMPACTION_OFFSET is higher than start offset.
        compactionOffset += 5;
        setCompactionOffset(compactionOffset, c);
        Assert.assertEquals("Unexpected result when segment is truncated before compaction offset.",
                compactionOffset, c.getCompactor().calculateTruncationOffset(0));
    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction is up-to-date.
     */
    @Test
    public void testCompactionUpToDate() throws Exception {
        @Cleanup
        val context = createContext(UPDATE_ENTRY_LENGTH);

        // Generate and index the data.
        populate(context);

        // Set the COMPACTION_OFFSET to the limit and verify the segment does not change.
        long length = context.segmentMetadata.getLength();
        setCompactionOffset(length, context);
        setMinUtilization(100, context);
        Assert.assertFalse("Not expecting compaction to be required.", context.getCompactor().isCompactionRequired().join());
        context.getCompactor().compact(context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Not expecting any segment modifications.", length, context.segmentMetadata.getLength());
        Assert.assertEquals("Not expecting any compaction changes.", length, IndexReader.getCompactionOffset(context.segmentMetadata));

        // Set the COMPACTION_OFFSET to an invalid value and verify the appropriate exception is thrown.
        setCompactionOffset(length + 1, context);
        Assert.assertFalse("Not expecting compaction to be required.", context.getCompactor().isCompactionRequired().join());
        AssertExtensions.assertSuppliedFutureThrows(
                "compact() worked with invalid segment state.",
                () -> context.getCompactor().compact(context.timer),
                ex -> ex instanceof DataCorruptionException);

        // Set Segment's StartOffset to max.
        setCompactionOffset(length - 1, context);
        context.segmentMetadata.setStartOffset(length);
        Assert.assertFalse("Not expecting compaction to be required.", context.getCompactor().isCompactionRequired().join());
        context.getCompactor().compact(context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Not expecting any segment modifications.", length, context.segmentMetadata.getLength());
        Assert.assertEquals("Not expecting any compaction changes.", length - 1, IndexReader.getCompactionOffset(context.segmentMetadata));
    }

    /**
     * Tests the {@link TableCompactor#compact} method when the compaction can only move one entry at a time.
     */
    @Test
    public void testCompactionSingleEntry() {
        testCompaction(UPDATE_ENTRY_LENGTH);
    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction may need to process multiple entries at once.
     */
    @Test
    public void testCompactionMultipleEntries() {
        final int batchCount = 10;
        testCompaction(batchCount * UPDATE_ENTRY_LENGTH);
    }

    @SneakyThrows
    private void testCompaction(int readLength) {
        @Cleanup
        val context = createContext(readLength);

        // Generate and index the data.
        val keyData = populate(context);

        // Sort the table entries by offset and identify which entries are "active" or not.
        val sortedEntries = sort(keyData, context).listIterator();

        // Keep track of various segment state and attributes - we will be progressively be checking it at each step.
        long compactionOffset = IndexReader.getCompactionOffset(context.segmentMetadata);
        final long compactionEndOffset = context.segmentMetadata.getLength();
        final long lastIndexedOffset = context.getCompactor().getLastIndexedOffset();
        Assert.assertEquals("Expected segment to be fully indexed prior to test.", compactionEndOffset, lastIndexedOffset);
        long totalEntryCount = IndexReader.getTotalEntryCount(context.segmentMetadata);
        final long entryCount = context.getCompactor().getUniqueEntryCount().join();

        // Perform compaction, step-by-step, until there is nothing left to compact.
        while (compactionOffset < compactionEndOffset) {
            // Collect the entries that we expect to be compacted in this iteration.
            val candidates = collect(sortedEntries, readLength);
            AssertExtensions.assertGreaterThan("No more entries to process yet compaction not done.", 0, candidates.size());
            int candidatesLength = candidates.stream().mapToInt(k -> k.length).sum();
            val copyCandidates = candidates.stream().filter(k -> k.isActive).collect(Collectors.toList());

            // Remember the Segment Length before compaction - this way we can figure out if anything was copied over.
            long initialLength = context.segmentMetadata.getLength();

            // Execute a compaction.
            context.getCompactor().compact(context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Check that the appropriate Table Segment attributes changed as expected.
            long expectedCompactionOffset = compactionOffset + candidatesLength;
            long newCompactionOffset = IndexReader.getCompactionOffset(context.segmentMetadata);
            Assert.assertEquals("Expected COMPACTION_OFFSET to have advanced.", expectedCompactionOffset, newCompactionOffset);
            long newTotalEntryCount = IndexReader.getTotalEntryCount(context.segmentMetadata);
            val expectedTotalEntryCount = context.hasDelayedIndexing()
                    ? totalEntryCount - candidates.size()
                    : totalEntryCount - candidates.size() + candidates.stream().filter(c -> c.isActive).count();
            Assert.assertEquals("Unexpected TOTAL_ENTRY_COUNT after partial compaction.",
                    expectedTotalEntryCount, newTotalEntryCount);

            // Check that these attributes have NOT changed (as applicable).
            if (context.hasDelayedIndexing()) {
                Assert.assertEquals("Not expecting LAST_INDEX_OFFSET to have changed.",
                        lastIndexedOffset, context.getCompactor().getLastIndexedOffset());
            }
            Assert.assertEquals("Not expecting ENTRY_COUNT to have changed.",
                    entryCount, (long) context.getCompactor().getUniqueEntryCount().join());

            if (copyCandidates.size() > 0) {
                // Check that the segment's length has increased (which would indicate copied entries).
                int expectedCopyLength = copyCandidates.stream().mapToInt(k -> k.length).sum();
                Assert.assertEquals("Expecting at least one entry to have been copied.",
                        initialLength + expectedCopyLength, context.segmentMetadata.getLength());

                // Verify that the copied entries are as expected.
                long expectedNewOffset = initialLength;
                for (val e : copyCandidates) {
                    val copiedEntry = readEntryAt(expectedNewOffset, e.length, context);
                    val expectedEntry = keyData.get(e.key).values.get(e.offset);
                    Assert.assertEquals("Unexpected Entry copied over from offset " + e.offset, expectedEntry, copiedEntry);
                    expectedNewOffset += e.length;
                }

                Assert.assertEquals("Expected copy candidates to have filled up remaining segment space.",
                        expectedNewOffset, context.segmentMetadata.getLength());
            } else {
                Assert.assertEquals("Not expected any entries to have been copied.",
                        initialLength, context.segmentMetadata.getLength());
            }

            compactionOffset = newCompactionOffset;
            totalEntryCount = newTotalEntryCount;
        }

        Assert.assertFalse("Not expecting any more entries to be compacted.", sortedEntries.hasNext());

        if (context.hasDelayedIndexing()) {
            // TOTAL_ENTRY_COUNT should have been reduced to 0 at the end - we have moved all entries out of the index.
            // In the real world, the IndexWriter will readjust this number as appropriate when reindexing these values.
            Assert.assertEquals("Expecting TOTAL_ENTRY_COUNT to be 0 after a full compaction.",
                    0, IndexReader.getTotalEntryCount(context.segmentMetadata));
        } else {
            // For Fixed-Key-Length Table Segments, the entries are instantly indexed so these attributes should match.
            Assert.assertEquals("Expecting TOTAL_ENTRY_COUNT to match UNIQUE_ENTRY_COUNT after a full compaction.",
                    (long) context.getCompactor().getUniqueEntryCount().join(), IndexReader.getTotalEntryCount(context.segmentMetadata));
        }
    }

    /**
     * Generates a set of Table Entries and serializes them into the segment, then indexes them, using the following strategy:
     * - Keys are identified by their index (0..KEY_COUNT-1)
     * - At each iteration I:
     * -- The first I * ({@link #DELETE_COUNT} + {@link #SKIP_COUNT} keys are ignored.
     * -- The next {@link #DELETE_COUNT} Keys are removed from the index.
     * -- The next {@link #SKIP_COUNT} are also ignored.
     * -- The remaining keys are updated to a new value.
     * - The algorithm ends when there would no longer be any keys to update for a particular iteration.
     *
     * @param context TestContext.
     * @return A Map of Keys to {@link KeyData}.
     */
    private Map<BufferView, KeyData> populate(TestContext context) {
        val rnd = new Random(0);

        // Generate keys.
        val keys = new ArrayList<KeyData>();
        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = new byte[KEY_LENGTH];
            rnd.nextBytes(key);
            keys.add(new KeyData(new ByteArraySegment(key)));
        }

        // Populate segment.
        int minIndex = 0;
        context.setSegmentState(0, 0, 0, 0, 0);
        while (minIndex < keys.size()) {
            int deleteCount = Math.min(DELETE_COUNT, keys.size() - minIndex);
            for (int i = 0; i < deleteCount; i++) {
                val keyData = keys.get(minIndex);
                if (context.areDeletesSerialized()) {
                    // Serialize removal and append it to the segment.
                    val key = TableKey.unversioned(keyData.key);
                    val serialization = context.serializer.serializeRemoval(Collections.singleton(key));
                    val offset = context.segment.append(serialization, null, TIMEOUT).join();

                    // Index it.
                    val previousOffset = keyData.values.isEmpty() ? -1 : (long) keyData.values.lastKey();
                    minIndex++;
                    val keyUpdate = new BucketUpdate.KeyUpdate(keyData.key, offset, offset, true);
                    context.index(keyUpdate, previousOffset, serialization.getLength());

                    // Store it as a deletion.
                    keyData.values.put(offset, null);
                } else {
                    // If we don't need to serialize the removal, just index it.
                    val keyUpdate = new BucketUpdate.KeyUpdate(keyData.key, -1L, -1L, true);
                    context.index(keyUpdate, -1L, -1);
                }
            }

            // Update the rest.
            for (int keyIndex = minIndex; keyIndex < keys.size(); keyIndex++) {
                // Generate the value.
                val keyData = keys.get(keyIndex);
                byte[] valueData = new byte[VALUE_LENGTH];
                rnd.nextBytes(valueData);
                val value = new ByteArraySegment(valueData);

                // Serialize and append it to the segment.
                val entry = TableEntry.unversioned(keyData.key, value);
                val serialization = context.serializer.serializeUpdate(Collections.singleton(entry));
                val offset = context.segment.append(serialization, null, TIMEOUT).join();

                // Index it.
                val previousOffset = keyData.values.isEmpty() ? -1 : (long) keyData.values.lastKey();
                val keyUpdate = new BucketUpdate.KeyUpdate(keyData.key, offset, offset, false);
                context.index(keyUpdate, previousOffset, serialization.getLength());

                // Store it, but also encode its version within.
                keyData.values.put(offset, TableEntry.versioned(entry.getKey().getKey(), entry.getValue(), offset));
            }

            // Skip over the next few keys.
            minIndex += Math.min(SKIP_COUNT, keys.size() - minIndex);
        }

        // Sanity checks before we can move on with any test.
        Assert.assertEquals("Expecting the whole segment to have been indexed.",
                context.segmentMetadata.getLength(), context.getCompactor().getLastIndexedOffset());
        Assert.assertEquals("Not expecting any changes to the COMPACTION_OFFSET attribute.",
                0, IndexReader.getCompactionOffset(context.segmentMetadata));
        AssertExtensions.assertLessThan("Expecting fewer active Table Entries than keys.",
                keys.size(), IndexReader.getEntryCount(context.segmentMetadata));
        AssertExtensions.assertGreaterThan("Expecting more total Table Entries than keys.",
                keys.size(), IndexReader.getTotalEntryCount(context.segmentMetadata));
        return keys.stream().collect(Collectors.toMap(k -> k.key, k -> k));
    }

    /**
     * Flattens the given Map of {@link KeyData} and Sorts the result by offset, producing {@link KeyInfo} instances.
     *
     * @param keys    The Keys to flatten and sort.
     * @param context TestContext.
     * @return Result.
     */
    private List<KeyInfo> sort(Map<BufferView, KeyData> keys, TestContext context) {
        val result = new ArrayList<KeyInfo>();
        for (val keyData : keys.values()) {
            long lastOffset = keyData.values.lastKey();
            for (val e : keyData.values.entrySet()) {
                // An Entry is active only if it is the last indexed value for that key and it is not a deletion.
                boolean deleted = e.getValue() == null;
                boolean isActive = e.getKey() == lastOffset && !deleted;
                int length = deleted
                        ? context.serializer.getRemovalLength(TableKey.unversioned(keyData.key))
                        : context.serializer.getUpdateLength(e.getValue());
                result.add(new KeyInfo(keyData.key, e.getKey(), length, isActive));
            }
        }

        result.sort(Comparator.comparingLong(k -> k.offset));
        return result;
    }

    /**
     * Collects the next {@link KeyInfo} instances from the given ListIterator as long as the given maxLength is not
     * exceeded.
     *
     * @param sortedEntries Entries.
     * @param maxLength     Max length.
     * @return Result.
     */
    private List<KeyInfo> collect(ListIterator<KeyInfo> sortedEntries, int maxLength) {
        val result = new ArrayList<KeyInfo>();
        while (sortedEntries.hasNext()) {
            val e = sortedEntries.next();
            if (e.length > maxLength) {
                // We moved one entry too far. Backtrack and exit.
                sortedEntries.previous();
                break;
            }

            result.add(e);
            maxLength -= e.length;
        }

        return result;
    }

    private void setMinUtilization(long utilizationThreshold, TestContext context) {
        context.segmentMetadata.updateAttributes(ImmutableMap.<AttributeId, Long>builder()
                .put(TableAttributes.MIN_UTILIZATION, utilizationThreshold)
                .build());
    }

    private void setCompactionOffset(long compactionOffset, TestContext context) {
        context.segmentMetadata.updateAttributes(ImmutableMap.<AttributeId, Long>builder()
                .put(TableAttributes.COMPACTION_OFFSET, compactionOffset)
                .build());
    }

    private TableEntry readEntryAt(long offset, int length, TestContext context) throws Exception {
        byte[] copiedData = new byte[length];
        context.segment.read(offset, length, TIMEOUT).readRemaining(copiedData, TIMEOUT);
        val c = AsyncTableEntryReader.readEntryComponents(new ByteArraySegment(copiedData).getBufferViewReader(), offset, context.serializer);
        return TableEntry.versioned(c.getKey(), c.getValue(), c.getVersion());
    }

    @RequiredArgsConstructor
    private static class KeyInfo {
        final BufferView key;
        final long offset;
        final int length;
        final boolean isActive;

        @Override
        public String toString() {
            return String.format("%s: %s (%s)", this.key.hashCode(), this.offset, this.isActive ? "active" : "obsolete");
        }
    }

    @RequiredArgsConstructor
    private static class KeyData {
        final BufferView key;
        final SortedMap<Long, TableEntry> values = new TreeMap<>();

        @Override
        public String toString() {
            return String.format("%s: %s", this.key.hashCode(),
                    this.values.entrySet().stream().map(e -> e.getKey() + (e.getValue() == null ? "[D]" : ""))
                            .collect(Collectors.joining(", ")));
        }
    }

    protected abstract class TestContext implements AutoCloseable {
        final StreamSegmentMetadata segmentMetadata;
        final SegmentMock segment;
        final EntrySerializer serializer;
        final TimeoutTimer timer;

        TestContext() {
            this.segmentMetadata = new StreamSegmentMetadata(SEGMENT_NAME, 1, 1);
            this.segment = new SegmentMock(this.segmentMetadata, executorService());
            this.serializer = new EntrySerializer();
            this.timer = new TimeoutTimer(TIMEOUT);
        }

        abstract TableCompactor getCompactor();

        protected abstract boolean hasDelayedIndexing();

        protected abstract boolean areDeletesSerialized();

        protected abstract void setSegmentState(long compactionOffset, long indexOffset, long entryCount, long totalEntryCount, long utilizationThreshold);

        protected abstract void setLastIndexedOffset(long offset);

        /**
         * Uses the {@link IndexWriter} to indexes the given {@link BucketUpdate.KeyUpdate} at the given offset into the Segment's Index.
         *
         * @param keyUpdate      The update.
         * @param previousOffset The previous last index offset.
         * @param length         Update length.
         */
        protected abstract void index(BucketUpdate.KeyUpdate keyUpdate, long previousOffset, int length);

        @Override
        public void close() {
        }
    }

}
