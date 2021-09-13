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

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMock;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link WriterTableProcessor} class.
 */
public class WriterTableProcessorTests extends ThreadPooledTestSuite {
    private static final long SEGMENT_ID = 12345L;
    private static final String SEGMENT_NAME = "Segment";
    private static final long INITIAL_LAST_INDEXED_OFFSET = 1000L;
    private static final int MAX_KEY_LENGTH = 1024;
    private static final int MAX_VALUE_LENGTH = 128; // We don't care about values that much here.
    private static final int UPDATE_COUNT = 10000;
    private static final int UPDATE_BATCH_SIZE = 689;
    private static final double REMOVE_FRACTION = 0.3; // 30% of generated operations are removes.
    private static final int MAX_COMPACT_LENGTH = (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * UPDATE_BATCH_SIZE;
    private static final int DEFAULT_MAX_FLUSH_SIZE = 128 * 1024 * 1024; // Default from TableWriterConnector.
    private static final int MAX_FLUSH_ATTEMPTS = 100; // To make sure we don't get stuck in an infinite flush loop.
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis() * 4, TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the {@link WriterTableProcessor#add} method and other general (non-flush) methods.
     */
    @Test
    public void testAdd() throws Exception {
        @Cleanup
        val context = new TestContext();
        Assert.assertFalse("Unexpected value from isClosed.", context.processor.isClosed());
        Assert.assertFalse("Unexpected value from mustFlush.", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN when no data in.", Operation.NO_SEQUENCE_NUMBER, context.processor.getLowestUncommittedSequenceNumber());

        // Mismatched segment ids.
        AssertExtensions.assertThrows(
                "add() worked with wrong segment id.",
                () -> context.processor.add(new StreamSegmentAppendOperation(SEGMENT_ID + 1, BufferView.empty(), null)),
                ex -> ex instanceof IllegalArgumentException);

        // Pre-last indexed offset.
        context.processor.add(generateRandomEntryAppend(0, context));
        Assert.assertFalse("Unexpected value from mustFlush after ignored add().", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN after ignored add().",
                Operation.NO_SEQUENCE_NUMBER, context.processor.getLowestUncommittedSequenceNumber());

        // Post-last indexed offset (not allowing gaps)
        AssertExtensions.assertThrows(
                "add() allowed first append to be after the last indexed offset.",
                () -> context.processor.add(generateRandomEntryAppend(INITIAL_LAST_INDEXED_OFFSET + 1, context)),
                ex -> ex instanceof DataCorruptionException);

        // Non-contiguous appends.
        val validAppend = generateRandomEntryAppend(INITIAL_LAST_INDEXED_OFFSET, context);
        context.processor.add(validAppend);
        Assert.assertTrue("Unexpected value from mustFlush after valid add().", context.processor.mustFlush());
        Assert.assertEquals("Unexpected LUSN after valid add().",
                validAppend.getSequenceNumber(), context.processor.getLowestUncommittedSequenceNumber());
        AssertExtensions.assertThrows(
                "add() allowed non-contiguous appends.",
                () -> context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset() + 1, context)),
                ex -> ex instanceof DataCorruptionException);

        // Delete the segment.
        context.metadata.markDeleted();
        Assert.assertFalse("Unexpected value from mustFlush after deletion.", context.processor.mustFlush());
        context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset(), context));
        Assert.assertEquals("Unexpected LUSN after ignored append due to deletion.",
                validAppend.getSequenceNumber(), context.processor.getLowestUncommittedSequenceNumber());

        // Close the processor and verify.
        context.processor.close();
        Assert.assertTrue("Unexpected value from isClosed after closing.", context.processor.isClosed());
        AssertExtensions.assertThrows(
                "add() worked after closing.",
                () -> context.processor.add(generateRandomEntryAppend(validAppend.getLastStreamSegmentOffset(), context)),
                ex -> ex instanceof ObjectClosedException);
    }

    /**
     * Tests the {@link WriterTableProcessor#flush} method using a non-collision-prone KeyHasher.
     */
    @Test
    public void testFlush() throws Exception {
        testFlushWithHasher(KeyHashers.DEFAULT_HASHER);
    }

    /**
     * Tests the {@link WriterTableProcessor#flush} method using a non-collision-prone KeyHasher.
     */
    @Test
    public void testFlushSmallBatches() throws Exception {
        int maxBatchSize = (MAX_KEY_LENGTH + MAX_VALUE_LENGTH) * 7;
        testFlushWithHasher(KeyHashers.DEFAULT_HASHER, 0, maxBatchSize);
    }

    /**
     * Tests the {@link WriterTableProcessor#flush} method using a collision-prone KeyHasher.
     */
    @Test
    public void testFlushCollisions() throws Exception {
        testFlushWithHasher(KeyHashers.COLLISION_HASHER);
    }

    /**
     * Tests the {@link WriterTableProcessor#flush} method using a non-collision-prone KeyHasher and forcing compactions.
     */
    @Test
    public void testFlushCompactions() throws Exception {
        testFlushWithHasher(KeyHashers.DEFAULT_HASHER, 99);
    }

    /**
     * Tests the ability to reconcile the {@link TableAttributes#INDEX_OFFSET} value when that changes behind the scenes.
     */
    @Test
    public void testReconcileTableIndexOffset() throws Exception {
        @Cleanup
        val context = new TestContext();

        // Generate two TableEntries, write them to the segment and queue them into the processor.
        val e1 = TableEntry.unversioned(new ByteArraySegment("k1".getBytes()), new ByteArraySegment("v1".getBytes()));
        val e2 = TableEntry.unversioned(new ByteArraySegment("k2".getBytes()), new ByteArraySegment("v2".getBytes()));
        val append1 = generateRawAppend(e1, INITIAL_LAST_INDEXED_OFFSET, context);
        val append2 = generateRawAppend(e2, append1.getLastStreamSegmentOffset(), context);
        context.segmentMock.append(append1.getData(), null, TIMEOUT).join();
        context.segmentMock.append(append2.getData(), null, TIMEOUT).join();
        context.processor.add(new CachedStreamSegmentAppendOperation(append1));
        context.processor.add(new CachedStreamSegmentAppendOperation(append2));

        // 1. INDEX_OFFSET changes to smaller than first append
        context.metadata.updateAttributes(Collections.singletonMap(TableAttributes.INDEX_OFFSET, INITIAL_LAST_INDEXED_OFFSET - 1));
        int attributeCountBefore = context.segmentMock.getAttributeCount();
        AssertExtensions.assertSuppliedFutureThrows(
                "flush() worked when INDEX_OFFSET decreased.",
                () -> context.processor.flush(TIMEOUT),
                ex -> ex instanceof DataCorruptionException);
        int attributeCountAfter = context.segmentMock.getAttributeCount();
        Assert.assertEquals("flush() seems to have modified the index after failed attempt", attributeCountBefore, attributeCountAfter);
        Assert.assertEquals("flush() seems to have modified the index after failed attempt.",
                INITIAL_LAST_INDEXED_OFFSET - 1, IndexReader.getLastIndexedOffset(context.metadata));

        // 2. INDEX_OFFSET changes to middle of append.
        context.metadata.updateAttributes(Collections.singletonMap(TableAttributes.INDEX_OFFSET, INITIAL_LAST_INDEXED_OFFSET + 1));
        attributeCountBefore = context.segmentMock.getAttributeCount();
        AssertExtensions.assertSuppliedFutureThrows(
                "flush() worked when INDEX_OFFSET changed to middle of append.",
                () -> context.processor.flush(TIMEOUT),
                ex -> ex instanceof DataCorruptionException);
        attributeCountAfter = context.segmentMock.getAttributeCount();
        Assert.assertEquals("flush() seems to have modified the index after failed attempt", attributeCountBefore, attributeCountAfter);
        Assert.assertEquals("flush() seems to have modified the index after failed attempt.",
                INITIAL_LAST_INDEXED_OFFSET + 1, IndexReader.getLastIndexedOffset(context.metadata));

        // 3. INDEX_OFFSET changes after the first append, but before the second one.
        context.metadata.updateAttributes(Collections.singletonMap(TableAttributes.INDEX_OFFSET, append2.getStreamSegmentOffset()));
        context.connector.refreshLastIndexedOffset();
        attributeCountBefore = context.segmentMock.getAttributeCount();
        context.processor.flush(TIMEOUT).join();
        attributeCountAfter = context.segmentMock.getAttributeCount();
        AssertExtensions.assertGreaterThan("flush() did not modify the index partial reconciliation.", attributeCountBefore, attributeCountAfter);
        Assert.assertEquals("flush() did not modify the index partial reconciliation.",
                append2.getLastStreamSegmentOffset(), IndexReader.getLastIndexedOffset(context.metadata));
        Assert.assertFalse("Unexpected result from mustFlush() after partial reconciliation.", context.processor.mustFlush());

        // 4. INDEX_OFFSET changes beyond the last append.
        val e3 = TableEntry.unversioned(new ByteArraySegment("k3".getBytes()), new ByteArraySegment("v3".getBytes()));
        val append3 = generateRawAppend(e3, append2.getLastStreamSegmentOffset(), context);
        context.segmentMock.append(append3.getData(), null, TIMEOUT).join();
        context.processor.add(new CachedStreamSegmentAppendOperation(append3));
        context.metadata.updateAttributes(Collections.singletonMap(TableAttributes.INDEX_OFFSET, append3.getLastStreamSegmentOffset() + 1));
        context.connector.refreshLastIndexedOffset();

        attributeCountBefore = context.segmentMock.getAttributeCount();
        context.processor.flush(TIMEOUT).join();
        attributeCountAfter = context.segmentMock.getAttributeCount();
        Assert.assertEquals("flush() seems to have modified the index after full reconciliation.", attributeCountBefore, attributeCountAfter);
        Assert.assertEquals("flush() did not properly update INDEX_OFFSET after full reconciliation.",
                append3.getLastStreamSegmentOffset() + 1, IndexReader.getLastIndexedOffset(context.metadata));
        Assert.assertFalse("Unexpected result from mustFlush() after full reconciliation.", context.processor.mustFlush());
    }

    /**
     * Tests {@link WriterTableProcessor.OperationAggregator}
     */
    @Test
    public void testOperationAggregator() {
        @Cleanup
        val context = new TestContext();
        val a = new WriterTableProcessor.OperationAggregator(123L);

        // Empty (nothing in it).
        Assert.assertEquals(123L, a.getLastIndexedOffset());
        Assert.assertEquals(-1L, a.getFirstOffset());
        Assert.assertEquals(-1L, a.getLastOffset());
        Assert.assertEquals(Operation.NO_SEQUENCE_NUMBER, a.getFirstSequenceNumber());
        Assert.assertTrue(a.isEmpty());
        Assert.assertEquals(0, a.size());
        Assert.assertEquals(-1L, a.getLastIndexToProcessAtOnce(12345));

        a.setLastIndexedOffset(124L);
        Assert.assertEquals(124L, a.getLastIndexedOffset());

        // Add one operation.
        val op1 = generateSimulatedAppend(123L, 1000, context);
        a.add(op1);
        Assert.assertEquals(124L, a.getLastIndexedOffset());
        Assert.assertEquals(op1.getStreamSegmentOffset(), a.getFirstOffset());
        Assert.assertEquals(op1.getLastStreamSegmentOffset(), a.getLastOffset());
        Assert.assertEquals(op1.getSequenceNumber(), a.getFirstSequenceNumber());
        Assert.assertFalse(a.isEmpty());
        Assert.assertEquals(1, a.size());
        Assert.assertEquals(op1.getLastStreamSegmentOffset(), a.getLastIndexToProcessAtOnce(12));
        Assert.assertEquals(op1.getLastStreamSegmentOffset(), a.getLastIndexToProcessAtOnce(123456));

        // Add a second operation.
        val op2 = generateSimulatedAppend(op1.getLastStreamSegmentOffset() + 1, 1000, context);
        a.add(op2);
        Assert.assertEquals(124L, a.getLastIndexedOffset());
        Assert.assertEquals(op1.getStreamSegmentOffset(), a.getFirstOffset());
        Assert.assertEquals(op2.getLastStreamSegmentOffset(), a.getLastOffset());
        Assert.assertEquals(op1.getSequenceNumber(), a.getFirstSequenceNumber());
        Assert.assertFalse(a.isEmpty());
        Assert.assertEquals(2, a.size());
        Assert.assertEquals(op1.getLastStreamSegmentOffset(), a.getLastIndexToProcessAtOnce(12));
        Assert.assertEquals(op1.getLastStreamSegmentOffset(), a.getLastIndexToProcessAtOnce((int) op1.getLength() + 1));
        Assert.assertEquals(op2.getLastStreamSegmentOffset(), a.getLastIndexToProcessAtOnce(123456));

        // Test setLastIndexedOffset.
        boolean r = a.setLastIndexedOffset(op1.getStreamSegmentOffset() + 1);
        Assert.assertFalse(r);
        Assert.assertEquals(124L, a.getLastIndexedOffset());
        Assert.assertEquals(2, a.size());

        r = a.setLastIndexedOffset(op2.getStreamSegmentOffset());
        Assert.assertTrue(r);
        Assert.assertEquals(op2.getStreamSegmentOffset(), a.getLastIndexedOffset());
        Assert.assertEquals(1, a.size());

        r = a.setLastIndexedOffset(op2.getLastStreamSegmentOffset() + 1);
        Assert.assertTrue(r);
        Assert.assertEquals(op2.getLastStreamSegmentOffset() + 1, a.getLastIndexedOffset());
        Assert.assertEquals(0, a.size());
    }

    private void testFlushWithHasher(KeyHasher hasher) throws Exception {
        testFlushWithHasher(hasher, 0);
    }

    private void testFlushWithHasher(KeyHasher hasher, int minSegmentUtilization) throws Exception {
        testFlushWithHasher(hasher, minSegmentUtilization, DEFAULT_MAX_FLUSH_SIZE);
    }

    private void testFlushWithHasher(KeyHasher hasher, int minSegmentUtilization, int maxFlushSize) throws Exception {
        // Generate a set of operations, each containing one or more entries. Each entry is an update or a remove.
        // Towards the beginning we have more updates than removes, then removes will prevail.
        @Cleanup
        val context = new TestContext(hasher);
        context.setMinUtilization(minSegmentUtilization);
        context.setMaxFlushSize(maxFlushSize);

        val batches = generateAndPopulateEntries(context);
        val allKeys = new HashMap<BufferView, UUID>(); // All keys, whether added or removed.

        TestBatchData lastBatch = null;
        for (val batch : batches) {
            for (val op : batch.operations) {
                context.processor.add(op);
            }

            // Pre-flush validation.
            Assert.assertTrue("Unexpected value from mustFlush() when there are outstanding operations.", context.processor.mustFlush());
            Assert.assertEquals("Unexpected LUSN before call to flush().",
                    batch.operations.get(0).getSequenceNumber(), context.processor.getLowestUncommittedSequenceNumber());

            // Flush at least once. If maxFlushSize is not the default, then we're in a test that wants to verify repeated
            // flushes; in that case we should flush until there's nothing more to flush.
            int remainingFlushes = MAX_FLUSH_ATTEMPTS;
            do {
                val initialNotifyCount = context.connector.notifyCount.get();
                val f1 = context.processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                AssertExtensions.assertGreaterThan("No calls to notifyIndexOffsetChanged().",
                        initialNotifyCount, context.connector.notifyCount.get());
                Assert.assertTrue(f1.isAnythingFlushed());
            } while (maxFlushSize < DEFAULT_MAX_FLUSH_SIZE && --remainingFlushes > 0 && context.processor.mustFlush());

            // Post-flush validation.
            Assert.assertFalse("Unexpected value from mustFlush() after call to flush().", context.processor.mustFlush());
            val f2 = context.processor.flush(false, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected LUSN after call to flush().",
                    Operation.NO_SEQUENCE_NUMBER, context.processor.getLowestUncommittedSequenceNumber());
            Assert.assertFalse(f2.isAnythingFlushed());

            // Verify correctness.
            batch.expectedEntries.keySet().forEach(k -> allKeys.put(k, context.keyHasher.hash(k)));
            checkIndex(batch.expectedEntries, allKeys, context);
            lastBatch = batch;
        }

        if (minSegmentUtilization > 0) {
            // We expect some compactions to happen. If this is the case, then we want to index all the moved entries
            // so that we may check compaction worked well.
            context.setMinUtilization(0); // disable compaction - we want to do a proper verification now.
            long compactionOffset = IndexReader.getCompactionOffset(context.metadata);
            AssertExtensions.assertGreaterThan("Expected at least one compaction.", 0, compactionOffset);

            // We need to simulate adding the compacted/copied entries to the index so that the WriterTableProcessor may
            // index them. As such, we add a new simulated append so that those entries can be indexed and the segment
            // truncated.
            long lIdx = IndexReader.getLastIndexedOffset(context.metadata);
            context.processor.add(generateSimulatedAppend(lIdx, (int) (context.metadata.getLength() - lIdx), context));
            context.processor.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

            final long truncationOffset = context.metadata.getStartOffset();
            long expectedTruncationOffset = compactionOffset;
            if (compactionOffset > truncationOffset) {
                // There is a valid case when the segment's truncation offset (start offset) can be smaller than the
                // compaction offset. This happens when there is at least one TableEntry that is marked as "removal"
                // exactly at the end of the compaction buffer. The TableCompactor will exclude that from the copy and
                // correctly set the Compaction Offset to the first byte after it, however the WriterTableProcessor will
                // never encounter this since it was never copied (hence it won't truncate the segment).
                expectedTruncationOffset -= getRemovalLengths(truncationOffset, batches, context);
            }
            Assert.assertEquals("Expected Segment's Start Offset to be the same as its COMPACTION_OFFSET.",
                    expectedTruncationOffset, truncationOffset);
            assert lastBatch != null;
            checkRelocatedIndex(lastBatch.expectedEntries, allKeys, context);
        } else {
            // No compaction was expected, however every test in this suite writes some garbage at the beginning of the
            // segment and instructs the indexer to begin from there. We want to verify that the initial data is truncated
            // away due to its irrelevance.
            Assert.assertEquals("Expected Segment's Start Offset to be the initial COMPACTED_OFFSET if no " +
                    "compaction took place.", INITIAL_LAST_INDEXED_OFFSET, context.metadata.getStartOffset());
        }
    }

    private long getRemovalLengths(long truncationOffset, List<TestBatchData> batches, TestContext context) throws Exception {
        val candidates = batches.stream().flatMap(b -> b.operations.stream())
                .filter(op -> op.getStreamSegmentOffset() >= truncationOffset)
                .iterator();
        val expectedEntries = batches.get(batches.size() - 1).expectedEntries;
        long result = 0;
        while (candidates.hasNext()) {
            val op = candidates.next();
            val opData = new byte[(int) op.getLength()];
            val bytesRead = context.segmentMock.read(op.getStreamSegmentOffset(), (int) op.getLength(), TIMEOUT).readRemaining(opData, TIMEOUT);
            assert bytesRead == opData.length;
            val entryHeader = context.serializer.readHeader(new ByteArraySegment(opData).getBufferViewReader());
            if (!expectedEntries.containsKey(new ByteArraySegment(opData, entryHeader.getKeyOffset(), entryHeader.getKeyLength()))) {
                result += op.getLength();
            } else {
                break;
            }
        }
        return result;
    }

    /**
     * Same outcome as {@link #checkIndex}, but does the verification by actually reading the Table Entries from the
     * segment. This method is slower than {@link #checkIndex} so it should only be used when needing access to the actual,
     * serialized Table Entry (such as in compaction testing).
     */
    private void checkRelocatedIndex(HashMap<BufferView, TableEntry> existingEntries, HashMap<BufferView, UUID> allKeys, TestContext context) throws Exception {
        // Get all the buckets associated with the given keys.
        val timer = new TimeoutTimer(TIMEOUT);
        val bucketsByHash = context.indexReader.locateBuckets(context.segmentMock, allKeys.values(), timer)
                                               .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        for (val e : allKeys.entrySet()) {
            val key = e.getKey();
            val expectedEntry = existingEntries.get(key);
            val bucket = bucketsByHash.get(e.getValue());
            Assert.assertNotNull("Test error: no bucket found.", bucket);
            if (expectedEntry != null) {
                // This key should exist.
                val actualEntry = TableBucketReader.entry(context.segmentMock, context.indexReader::getBackpointerOffset, executorService())
                                                   .find(key, bucket.getSegmentOffset(), timer)
                                                   .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertEquals("Unexpected entry.", expectedEntry, actualEntry);
            } else {
                // This key should not exist.
                if (bucket.exists()) {
                    val actualEntry = TableBucketReader.entry(context.segmentMock, context.indexReader::getBackpointerOffset, executorService())
                                                       .find(key, bucket.getSegmentOffset(), timer)
                                                       .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    Assert.assertNull(actualEntry);
                }
            }
        }
    }

    private void checkIndex(HashMap<BufferView, TableEntry> existingEntries, HashMap<BufferView, UUID> allKeys, TestContext context) throws Exception {
        // Get all the buckets associated with the given keys.
        val timer = new TimeoutTimer(TIMEOUT);
        val bucketsByHash = context.indexReader.locateBuckets(context.segmentMock, allKeys.values(), timer)
                                               .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Index the existing Keys by their current offsets.
        val keysByOffset = existingEntries.entrySet().stream()
                                          .collect(Collectors.toMap(e -> e.getValue().getKey().getVersion(), Map.Entry::getKey));

        // Load up all the offsets for all buckets.
        val buckets = bucketsByHash.values().stream().distinct()
                                   .collect(Collectors.toMap(b -> b, b -> context.indexReader.getBucketOffsets(context.segmentMock, b, timer).join()));

        // Loop through all the bucket's offsets and verify that those offsets do point to existing keys.
        for (val e : buckets.entrySet()) {
            val bucketOffsets = e.getValue();
            for (val offset : bucketOffsets) {
                Assert.assertTrue("Found Bucket Offset that points to non-existing key.", keysByOffset.containsKey(offset));
            }
        }

        // Loop through each key in allKeys and verify that if it's expected to exist, it is indeed included in the appropriate
        // TableBucket, otherwise it is not included in any bucket.
        for (val e : allKeys.entrySet()) {
            val key = e.getKey();
            val tableEntry = existingEntries.get(key);
            val bucket = bucketsByHash.get(e.getValue());
            Assert.assertNotNull("Test error: no bucket found.", bucket);
            val bucketOffsets = buckets.get(bucket);
            if (tableEntry != null) {
                // This key should exist: just verify the TableEntry's offset (Key Version) exists in the Bucket's offset list.
                Assert.assertTrue("Non-deleted key was not included in a Table Bucket.", bucketOffsets.contains(tableEntry.getKey().getVersion()));
            } else {
                // Verify that all the keys that the Table Bucket points to do not match our key. Use our existing offset-key cache for that.
                for (val offset : bucketOffsets) {
                    val keyAtOffset = keysByOffset.get(offset);
                    Assert.assertNotEquals("Deleted key was still included in a Table Bucket.", key, keyAtOffset);
                }
            }
        }
    }

    private ArrayList<TestBatchData> generateAndPopulateEntries(TestContext context) {
        val result = new ArrayList<TestBatchData>();
        int count = 0;
        while (count < UPDATE_COUNT) {
            int batchSize = Math.min(UPDATE_BATCH_SIZE, UPDATE_COUNT - count);
            Map<BufferView, TableEntry> prevState = result.isEmpty()
                    ? Collections.emptyMap()
                    : result.get(result.size() - 1).expectedEntries;
            result.add(generateAndPopulateEntriesBatch(batchSize, prevState, context));
            count += batchSize;
        }

        return result;
    }

    private TestBatchData generateAndPopulateEntriesBatch(int batchSize, Map<BufferView, TableEntry> initialState, TestContext context) {
        val result = new TestBatchData(new HashMap<>(initialState));
        val allKeys = new ArrayList<>(initialState.keySet()); // Need a list so we can efficiently pick removal candidates.
        for (int i = 0; i < batchSize; i++) {
            // We only generate a remove if we have something to remove.
            boolean remove = allKeys.size() > 0 && (context.random.nextDouble() < REMOVE_FRACTION);
            StreamSegmentAppendOperation append;
            if (remove) {
                val key = allKeys.get(context.random.nextInt(allKeys.size()));
                append = generateRawRemove(TableKey.unversioned(key), context.metadata.getLength(), context);
                result.expectedEntries.remove(key);
                allKeys.remove(key);
            } else {
                // Generate a new Table Entry.
                byte[] keyData = new byte[Math.max(1, context.random.nextInt(MAX_KEY_LENGTH))];
                context.random.nextBytes(keyData);
                byte[] valueData = new byte[context.random.nextInt(MAX_VALUE_LENGTH)];
                context.random.nextBytes(valueData);

                // Run the key through the external translator to ensure that we don't clash with internal keys by chance.
                // (this is done for us by ContainerTableExtensionImpl already, so we're only simulating the same behavior).
                val key = new ByteArraySegment(keyData);
                val offset = context.metadata.getLength();
                val entry = TableEntry.versioned(key, new ByteArraySegment(valueData), offset);
                append = generateRawAppend(entry, offset, context);
                result.expectedEntries.put(key, entry);
                allKeys.add(key);
            }

            // Add to segment.
            context.metadata.setLength(context.metadata.getLength() + append.getLength());
            context.segmentMock.append(append.getData(), null, TIMEOUT).join();

            // Add to result.
            result.operations.add(new CachedStreamSegmentAppendOperation(append));
        }

        return result;
    }

    private CachedStreamSegmentAppendOperation generateRandomEntryAppend(long offset, TestContext context) {
        return generateAppend(TableEntry.unversioned(new ByteArraySegment(new byte[1]), new ByteArraySegment(new byte[1])), offset, context);
    }

    private CachedStreamSegmentAppendOperation generateAppend(TableEntry entry, long offset, TestContext context) {
        return new CachedStreamSegmentAppendOperation(generateRawAppend(entry, offset, context));
    }

    private StreamSegmentAppendOperation generateRawAppend(TableEntry entry, long offset, TestContext context) {
        val data = context.serializer.serializeUpdate(Collections.singletonList(entry));
        val append = new StreamSegmentAppendOperation(SEGMENT_ID, data, null);
        append.setSequenceNumber(context.nextSequenceNumber());
        append.setStreamSegmentOffset(offset);
        return append;
    }

    private StreamSegmentAppendOperation generateRawRemove(TableKey key, long offset, TestContext context) {
        val data = context.serializer.serializeRemoval(Collections.singletonList(key));
        val append = new StreamSegmentAppendOperation(SEGMENT_ID, data, null);
        append.setSequenceNumber(context.nextSequenceNumber());
        append.setStreamSegmentOffset(offset);
        return append;
    }

    private CachedStreamSegmentAppendOperation generateSimulatedAppend(long offset, int length, TestContext context) {
        val op = new StreamSegmentAppendOperation(context.metadata.getId(), offset, new ByteArraySegment(new byte[length]), null);
        op.setSequenceNumber(context.nextSequenceNumber());
        return new CachedStreamSegmentAppendOperation(op);
    }

    @RequiredArgsConstructor
    private static class TestBatchData {
        final HashMap<BufferView, TableEntry> expectedEntries;
        final List<CachedStreamSegmentAppendOperation> operations = new ArrayList<>();
    }

    //region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableSegmentMetadata metadata;
        final EntrySerializer serializer;
        final KeyHasher keyHasher;
        final SegmentMock segmentMock;
        final TableWriterConnectorImpl connector;
        final WriterTableProcessor processor;
        final IndexReader indexReader;
        final Random random;
        final AtomicLong sequenceNumber;
        final AtomicInteger maxFlushSize = new AtomicInteger(128 * 1024 * 1024);

        TestContext() {
            this(KeyHashers.DEFAULT_HASHER);
        }

        TestContext(KeyHasher hasher) {
            this.metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, 0);
            this.serializer = new EntrySerializer();
            this.keyHasher = hasher;
            this.segmentMock = new SegmentMock(this.metadata, executorService());
            this.random = new Random(0);
            this.sequenceNumber = new AtomicLong(0);
            initializeSegment();
            this.indexReader = new IndexReader(executorService());
            this.connector = new TableWriterConnectorImpl();
            this.processor = new WriterTableProcessor(connector, executorService());
        }

        @Override
        public void close() {
            this.processor.close();
            Assert.assertTrue("WriterTableProcessor.close() did not close the connector.", this.connector.closed.get());
        }

        long nextSequenceNumber() {
            return this.sequenceNumber.incrementAndGet();
        }

        void setMaxFlushSize(int value) {
            this.maxFlushSize.set(value);
        }

        void setMinUtilization(int value) {
            Preconditions.checkArgument(value >= 0 && value <= 100);
            this.segmentMock.updateAttributes(
                    AttributeUpdateCollection.from(new AttributeUpdate(TableAttributes.MIN_UTILIZATION, AttributeUpdateType.Replace, value)), TIMEOUT).join();
        }

        private void initializeSegment() {
            // Populate table-related attributes.
            this.segmentMock.updateAttributes(TableAttributes.DEFAULT_VALUES);

            // Pre-populate the INDEX_OFFSET. We write some garbage at the beginning and want to make sure that the indexer
            // can begin from the appropriate index offset.
            this.segmentMock.updateAttributes(AttributeUpdateCollection.from(
                    new AttributeUpdate(TableAttributes.INDEX_OFFSET, AttributeUpdateType.Replace, INITIAL_LAST_INDEXED_OFFSET),
                    new AttributeUpdate(TableAttributes.COMPACTION_OFFSET, AttributeUpdateType.Replace, INITIAL_LAST_INDEXED_OFFSET)),
                    TIMEOUT).join();
            this.segmentMock.append(new ByteArraySegment(new byte[(int) INITIAL_LAST_INDEXED_OFFSET]), null, TIMEOUT).join();
        }

        private class TableWriterConnectorImpl implements TableWriterConnector {
            private final AtomicInteger notifyCount = new AtomicInteger(0);
            private final AtomicBoolean closed = new AtomicBoolean();
            private final AtomicLong previousLastIndexedOffset = new AtomicLong(-1);

            TableWriterConnectorImpl() {
                refreshLastIndexedOffset();
            }

            void refreshLastIndexedOffset() {
                this.previousLastIndexedOffset.set(IndexReader.getLastIndexedOffset(segmentMock.getInfo()));
            }

            @Override
            public SegmentMetadata getMetadata() {
                return metadata;
            }

            @Override
            public EntrySerializer getSerializer() {
                return serializer;
            }

            @Override
            public KeyHasher getKeyHasher() {
                return keyHasher;
            }

            @Override
            public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
                return CompletableFuture.supplyAsync(() -> segmentMock, executorService());
            }

            @Override
            public void notifyIndexOffsetChanged(long lastIndexedOffset, int processedSizeBytes) {
                Assert.assertEquals("Unexpected value for lastIndexedOffset.",
                        IndexReader.getLastIndexedOffset(segmentMock.getInfo()), lastIndexedOffset);

                AssertExtensions.assertGreaterThanOrEqual("Expecting processedSizeBytes to be positive", 0, processedSizeBytes);
                long expectedProcessedSize = Math.max(0, lastIndexedOffset - this.previousLastIndexedOffset.get());
                Assert.assertEquals("Unexpected processedSizeBytes.", expectedProcessedSize, processedSizeBytes);
                refreshLastIndexedOffset();
                this.notifyCount.incrementAndGet();
            }

            @Override
            public int getMaxCompactionSize() {
                return MAX_COMPACT_LENGTH;
            }

            @Override
            public int getMaxFlushSize() {
                return maxFlushSize.get();
            }

            @Override
            public void close() {
                this.closed.set(true);
            }
        }
    }

    //endregion
}
