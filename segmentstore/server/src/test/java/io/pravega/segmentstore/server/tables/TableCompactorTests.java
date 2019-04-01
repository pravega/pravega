/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.server.tables;

import com.google.common.collect.ImmutableMap;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableCompactor} class.
 */
public class TableCompactorTests extends ThreadPooledTestSuite {
    private static final int KEY_COUNT = 20; // Number of distinct keys in the tests, numbered 0 to KEY_COUNT-1.
    private static final int SKIP_COUNT = 2; // At each iteration i, we update all keys K>=i*SKIP_COUNT+DELETE_COUNT.
    private static final int DELETE_COUNT = 1; // At each iteration i, we remove keys K>=i*SKIP_COUNT to K<i*SKIP_COUNT+DELETE_COUNT
    private static final int KEY_LENGTH = 64;
    private static final int VALUE_LENGTH = 128;
    private static final String SEGMENT_NAME = "TableSegment";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final KeyHasher KEY_HASHER = KeyHashers.DEFAULT_HASHER;

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the {@link TableCompactor#isCompactionRequired} method.
     */
    @Test
    public void testIsCompactionRequired() {
        @Cleanup
        val c = new TestContext();
        c.segmentMetadata.setLength(100);

        // TruncationOffset < Compaction offset, and CompactionOffset >= LastIndexedOffset.
        setSegmentState(50, 50, 1, 100, 50, c);
        Assert.assertFalse("Unexpected result when CompactionOffset is equal to IndexOffset.", c.compactor.isCompactionRequired(c.segmentMetadata));

        // TruncationOffset > Compaction offset, and TruncationOffset >= LastIndexedOffset.
        c.segmentMetadata.setStartOffset(50);
        setSegmentState(10, 50, 1, 100, 50, c);
        Assert.assertFalse("Unexpected result when TruncationOffset is equal to IndexOffset.", c.compactor.isCompactionRequired(c.segmentMetadata));

        // Utilization == Threshold
        setSegmentState(0, 100, 50, 100, 50, c);
        Assert.assertFalse("Unexpected result when Utilization==MinUtilization.", c.compactor.isCompactionRequired(c.segmentMetadata));

        // Utilization > Threshold
        setSegmentState(0, 100, 51, 100, 50, c);
        Assert.assertFalse("Unexpected result when Utilization>MinUtilization.", c.compactor.isCompactionRequired(c.segmentMetadata));

        // Empty table
        setSegmentState(0, 100, 10, 0, 50, c);
        Assert.assertFalse("Unexpected result when TotalEntryCount==0.", c.compactor.isCompactionRequired(c.segmentMetadata));

        // Utilization < Threshold
        setSegmentState(0, 100, 49, 100, 50, c);
        Assert.assertTrue("Unexpected result when Utilization>MinUtilization.", c.compactor.isCompactionRequired(c.segmentMetadata));
    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction is up-to-date.
     */
    @Test
    public void testCompactionUpToDate() throws Exception {
        @Cleanup
        val context = new TestContext();

        // Generate and index the data.
        populate(context);

        // Set the COMPACTION_OFFSET to the limit and verify the segment does not change.
        long length = context.segmentMetadata.getLength();
        setCompactionOffset(length, context);
        setMinUtilization(100, context);
        Assert.assertFalse("Not expecting compaction to be required.", context.compactor.isCompactionRequired(context.segmentMetadata));
        context.compactor.compact(context.segment, context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Not expecting any segment modifications.", length, context.segmentMetadata.getLength());
        Assert.assertEquals("Not expecting any compaction changes.", length, context.indexWriter.getCompactionOffset(context.segmentMetadata));

        // Set the COMPACTION_OFFSET to an invalid value and verify the appropriate exception is thrown.
        setCompactionOffset(length + 1, context);
        Assert.assertFalse("Not expecting compaction to be required.", context.compactor.isCompactionRequired(context.segmentMetadata));
        AssertExtensions.assertSuppliedFutureThrows(
                "compac() worked with invalid segment state.",
                () -> context.compactor.compact(context.segment, context.timer),
                ex -> ex instanceof DataCorruptionException);

        // Set Segment's StartOffset to max.
        setCompactionOffset(length - 1, context);
        context.segmentMetadata.setStartOffset(length);
        Assert.assertFalse("Not expecting compaction to be required.", context.compactor.isCompactionRequired(context.segmentMetadata));
        context.compactor.compact(context.segment, context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Not expecting any segment modifications.", length, context.segmentMetadata.getLength());
        Assert.assertEquals("Not expecting any compaction changes.", length - 1, context.indexWriter.getCompactionOffset(context.segmentMetadata));
    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction results in no entries needing copying.
     */
    @Test
    public void testCompactionSingleEntry() throws Exception {
        // We read one key at a time.
        int readLength = KEY_LENGTH + VALUE_LENGTH + EntrySerializer.HEADER_LENGTH;
        @Cleanup
        val context = new TestContext(readLength);

        // Generate and index the data.
        val keyData = populate(context);
        setMinUtilization(100, context);
        Assert.assertTrue("Expecting compaction to be required.", context.compactor.isCompactionRequired(context.segmentMetadata));

        // Sort the table entries by offset and identify which entries are "active" or not.
        val sortedEntries = sort(keyData).iterator();

        // Perform compaction, step-by-step.
        long compactionOffset = context.indexWriter.getCompactionOffset(context.segmentMetadata);
        final long lastIndexedOffset = context.indexWriter.getLastIndexedOffset(context.segmentMetadata);
        long totalEntryCount = context.indexWriter.getTotalEntryCount(context.segmentMetadata);
        final long entryCount = context.indexWriter.getEntryCount(context.segmentMetadata);
        while (compactionOffset < lastIndexedOffset) {
            Assert.assertTrue("No more entries to process yet compaction not done.", sortedEntries.hasNext());
            val entry = sortedEntries.next();

            // TODO: finalize this test. There is a runtime error.
            long initialLength = context.segmentMetadata.getLength();

            // Execute a compaction.
            context.compactor.compact(context.segment, context.timer).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            // Check that the appropriate Table Segment attributes changed as expected.
            long newCompactionOffset = context.indexWriter.getCompactionOffset(context.segmentMetadata);
            Assert.assertEquals("Expected COMPACTION_OFFSET to have advanced.", newCompactionOffset, compactionOffset + readLength);
            long newTotalEntryCount = context.indexWriter.getTotalEntryCount(context.segmentMetadata);
            Assert.assertEquals("Expected TOTAL_ENTRY_COUNT to have decreased.", totalEntryCount - 1, newTotalEntryCount);

            // Check that these attributes have NOT changed.
            Assert.assertEquals("Not expecting LAST_INDEX_OFFSET to have changed.",
                    lastIndexedOffset, context.indexWriter.getLastIndexedOffset(context.segmentMetadata));
            Assert.assertEquals("Not expecting ENTRY_COUNT to have changed.",
                    entryCount, context.indexWriter.getEntryCount(context.segmentMetadata));

            if (entry.isActive) {
                // Check for copy
                Assert.assertEquals("Expecting this entry to have been copied.",
                        initialLength + readLength, context.segmentMetadata.getLength());
                // TODO: verify copied entry is correct.
            } else {
                Assert.assertEquals("Not expected this entry to have been copied.",
                        initialLength, context.segmentMetadata.getLength());
            }

            compactionOffset = newCompactionOffset;
            totalEntryCount = newTotalEntryCount;
        }

        // TODO: verify that TOTAL_ENTRY_COUNT == ENTRY_COUNT
    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction requires entries be copied.
     */
    @Test
    public void testCompactionMultipleEntries() {
        @Cleanup
        val context = new TestContext();

        // Generate and index the data.
        val keyData = populate(context);
        // TODO: maybe implement this test in a similar manner to the single entry one.

    }

    private Collection<KeyData> populate(TestContext context) {
        val rnd = new Random(0);

        // Generate keys.
        val keys = new ArrayList<KeyData>();
        for (int i = 0; i < KEY_COUNT; i++) {
            byte[] key = new byte[KEY_LENGTH];
            rnd.nextBytes(key);
            keys.add(new KeyData(new HashedArray(key)));
        }

        // Populate segment.
        int minIndex = 0;
        setSegmentState(0, 0, 0, 0, 0, context);
        while (minIndex < keys.size()) {
            int deleteCount = Math.min(DELETE_COUNT, keys.size() - minIndex);
            for (int i = 0; i < deleteCount; i++) {
                // Serialize removal and append it to the segment.
                val keyData = keys.get(minIndex);
                val key = TableKey.unversioned(keyData.key);
                val serialization = new byte[context.serializer.getRemovalLength(key)];
                context.serializer.serializeRemoval(Collections.singleton(key), serialization);
                val offset = context.segment.append(serialization, null, TIMEOUT).join();

                // Index it.
                val previousOffset = keyData.values.isEmpty() ? -1 : (long) keyData.values.lastKey();
                keyData.values.put(offset, null);
                minIndex++;
                val keyUpdate = new BucketUpdate.KeyUpdate(keyData.key, offset, offset, true);
                index(keyUpdate, offset, previousOffset, serialization.length, context);
            }

            // Update the rest.
            for (int keyIndex = minIndex; keyIndex < keys.size(); keyIndex++) {
                // Generate the value.
                val keyData = keys.get(keyIndex);
                byte[] valueData = new byte[VALUE_LENGTH];
                rnd.nextBytes(valueData);
                val value = new HashedArray(valueData);

                // Serialize and append it to the segment.
                val entry = TableEntry.unversioned(keyData.key, value);
                val serialization = new byte[context.serializer.getUpdateLength(entry)];
                context.serializer.serializeUpdate(Collections.singleton(entry), serialization);
                val offset = context.segment.append(serialization, null, TIMEOUT).join();

                // Index it.
                val previousOffset = keyData.values.isEmpty() ? -1 : (long) keyData.values.lastKey();
                keyData.values.put(offset, value);
                val keyUpdate = new BucketUpdate.KeyUpdate(keyData.key, offset, offset, false);
                index(keyUpdate, offset, previousOffset, serialization.length, context);
            }

            // Skip over the next few keys.
            minIndex += Math.min(SKIP_COUNT, keys.size() - minIndex);
        }

        // Sanity checks before we can move on with any test.
        Assert.assertEquals("Expecting the whole segment to have been indexed.",
                context.segmentMetadata.getLength(), context.indexWriter.getLastIndexedOffset(context.segmentMetadata));
        Assert.assertEquals("Not expecting any changes to the COMPACTION_OFFSET attribute.",
                0, context.indexWriter.getCompactionOffset(context.segmentMetadata));
        AssertExtensions.assertLessThan("Expecting fewer active Table Entries than keys.",
                keys.size(), context.indexWriter.getEntryCount(context.segmentMetadata));
        AssertExtensions.assertGreaterThan("Expecting more total Table Entries than keys.",
                keys.size(), context.indexWriter.getTotalEntryCount(context.segmentMetadata));
        return keys;
    }

    private void index(BucketUpdate.KeyUpdate keyUpdate, long offset, long previousOffset, int length, TestContext context) {
        val b = context.indexWriter.groupByBucket(context.segment, Collections.singleton(keyUpdate), context.timer).join()
                                   .stream().findFirst().get();
        if (previousOffset >= 0) {
            b.withExistingKey(new BucketUpdate.KeyInfo(keyUpdate.getKey(), previousOffset, previousOffset));
        }

        context.indexWriter.updateBuckets(context.segment, Collections.singleton(b.build()), offset, offset + length, 1, TIMEOUT).join();
    }

    private List<KeyInfo> sort(Collection<KeyData> keys) {
        val result = new ArrayList<KeyInfo>();
        for (val keyData : keys) {
            long lastOffset = keyData.values.lastKey();
            for (val e : keyData.values.entrySet()) {
                // An Entry is active only if it is the last indexed value for that key and it is not a deletion.
                boolean isActive = e.getKey() == lastOffset && e.getValue() != null;
                result.add(new KeyInfo(keyData.key, e.getKey(), isActive));
            }
        }

        result.sort(Comparator.comparingLong(k -> k.offset));
        return result;
    }

    private void setSegmentState(long compactionOffset, long indexOffset, long entryCount, long totalEntryCount, long utilizationThreshold, TestContext context) {
        context.segmentMetadata.updateAttributes(ImmutableMap.<UUID, Long>builder()
                .put(TableAttributes.COMPACTION_OFFSET, compactionOffset)
                .put(TableAttributes.INDEX_OFFSET, indexOffset)
                .put(TableAttributes.ENTRY_COUNT, entryCount)
                .put(TableAttributes.TOTAL_ENTRY_COUNT, totalEntryCount)
                .put(TableAttributes.MIN_UTILIZATION, utilizationThreshold)
                .build());
    }

    private void setMinUtilization(long utilizationThreshold, TestContext context) {
        context.segmentMetadata.updateAttributes(ImmutableMap.<UUID, Long>builder()
                .put(TableAttributes.MIN_UTILIZATION, utilizationThreshold)
                .build());
    }

    private void setCompactionOffset(long compactionOffset, TestContext context) {
        context.segmentMetadata.updateAttributes(ImmutableMap.<UUID, Long>builder()
                .put(TableAttributes.COMPACTION_OFFSET, compactionOffset)
                .build());
    }

    @RequiredArgsConstructor
    private static class KeyInfo {
        final HashedArray key;
        final long offset;
        final boolean isActive;

        @Override
        public String toString() {
            return String.format("%s: %s (%s)", this.key.hashCode(), this.offset, this.isActive ? "active" : "obsolete");
        }
    }

    private static class KeyData {
        final HashedArray key;
        final UUID keyHash;
        final SortedMap<Long, HashedArray> values = new TreeMap<>();

        KeyData(HashedArray key) {
            this.key = key;
            this.keyHash = KEY_HASHER.hash(key);
        }

        @Override
        public String toString() {
            return String.format("%s: %s", this.key.hashCode(),
                    this.values.entrySet().stream().map(e -> e.getKey() + (e.getValue() == null ? "[D]" : ""))
                               .collect(Collectors.joining(", ")));
        }
    }

    private class TestContext implements AutoCloseable {
        final StreamSegmentMetadata segmentMetadata;
        final SegmentMock segment;
        final EntrySerializer serializer;
        final TestConnector writerConnector;
        final IndexWriter indexWriter;
        final TableCompactor compactor;
        final TimeoutTimer timer;

        TestContext() {
            this(TableCompactor.DEFAULT_MAX_READ_LENGTH);
        }

        TestContext(int compactorMaxReadLength) {
            this.segmentMetadata = new StreamSegmentMetadata(SEGMENT_NAME, 1, 1);
            this.segment = new SegmentMock(this.segmentMetadata, executorService());
            this.indexWriter = new IndexWriter(KEY_HASHER, executorService());
            this.serializer = new EntrySerializer();
            this.writerConnector = new TestConnector(this.segment, this.serializer, KEY_HASHER);
            this.compactor = new TableCompactor(this.writerConnector, this.indexWriter, executorService(), compactorMaxReadLength);
            this.timer = new TimeoutTimer(TIMEOUT);
        }

        @Override
        public void close() {
            this.writerConnector.close();
        }
    }

    @Getter
    @RequiredArgsConstructor
    private class TestConnector implements TableWriterConnector {
        private final SegmentMock segment;
        private final EntrySerializer serializer;
        private final KeyHasher keyHasher;

        @Override
        public SegmentMetadata getMetadata() {
            return this.segment.getMetadata();
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return CompletableFuture.completedFuture(this.segment);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset) {
            throw new UnsupportedOperationException("not needed");
        }

        @Override
        public void close() {
            // Nothing to do.
        }
    }
}
