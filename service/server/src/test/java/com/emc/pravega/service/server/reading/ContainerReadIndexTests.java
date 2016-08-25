/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.mocks.InMemoryCache;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for ContainerReadIndex class.
 */
public class ContainerReadIndexTests {
    private static final int SEGMENT_COUNT = 100;
    private static final int BATCHES_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int THREAD_POOL_SIZE = 50;
    private static final int CONTAINER_ID = 123;
    private static final ReadIndexConfig DEFAULT_CONFIG = ConfigHelpers.createReadIndexConfig(100, 1024);
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /**
     * Tests the basic append-read functionality of the ContainerReadIndex, with data fully in it (no tail reads).
     */
    @Test
    public void testAppendRead() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all batch names into the segment list. For this test, we do not care what kind of Segment we have.
        batchesBySegment.values().forEach(segmentIds::addAll);

        // Add a bunch of writes.
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    /**
     * Tests the merging of batches into their parent StreamSegments.
     */
    @Test
    public void testMerge() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Put all segment names into one list, for easier appends (but still keep the original lists at hand - we'll need them later).
        ArrayList<Long> allSegmentIds = new ArrayList<>(segmentIds);
        batchesBySegment.values().forEach(allSegmentIds::addAll);

        // Add a bunch of writes.
        appendData(allSegmentIds, segmentContents, context);

        // Begin-merge all batches (part 1/2), and check contents.
        beginMergeBatches(batchesBySegment, segmentContents, context);
        checkReadIndex("BeginMerge", segmentContents, context);

        // Complete the merger (part 2/2), and check contents.
        completeMergeBatches(batchesBySegment, context);
        checkReadIndex("CompleteMerge", segmentContents, context);
    }

    /**
     * Tests the behavior of Future Reads. Scenarios tested include:
     * * Regular appends
     * * Segment sealing
     * * Batch merging.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testFutureReads() throws Exception {
        final int nonSealReadLimit = APPENDS_PER_SEGMENT * 25; // About 40-50% of the entire segment length.
        final int triggerFutureReadsEvery = 3; // How many appends to trigger Future reads.
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        HashMap<Long, ByteArrayOutputStream> readContents = new HashMap<>();
        HashSet<Long> segmentsToSeal = new HashSet<>();
        HashMap<Long, AsyncReadResultProcessor> processorsBySegment = new HashMap<>();
        HashMap<Long, TestEntryHandler> entryHandlers = new HashMap<>();

        // 1. Put all segment names into one list, for easier appends (but still keep the original lists at hand - we'll need them later).
        ArrayList<Long> allSegmentIds = new ArrayList<>(segmentIds);
        batchesBySegment.values().forEach(allSegmentIds::addAll);

        AtomicInteger writeCount = new AtomicInteger();
        Runnable triggerFutureReadsCallback = () -> {
            if (writeCount.incrementAndGet() % triggerFutureReadsEvery == 0) {
                context.readIndex.triggerFutureReads(segmentIds);
            }
        };

        // 2. Setup tail reads.
        // First 1/2 of segments will try to read Int32.Max bytes, while the other half will try to read 100 bytes.
        // We will then seal the first 1/2 of the segments, which should cause the tail reads to stop (the remaining
        // should stop upon reaching the limit).
        for (int i = 0; i < segmentIds.size(); i++) {
            long segmentId = segmentIds.get(i);
            ByteArrayOutputStream readContentsStream = new ByteArrayOutputStream();
            readContents.put(segmentId, readContentsStream);

            ReadResult readResult;
            if (i < segmentIds.size() / 2) {
                // We're going to seal this one at one point.
                segmentsToSeal.add(segmentId);
                readResult = context.readIndex.read(segmentId, 0, Integer.MAX_VALUE, TIMEOUT);
            } else {
                // Just a regular one, nothing special.
                readResult = context.readIndex.read(segmentId, 0, nonSealReadLimit, TIMEOUT);
            }

            // The Read callback is only accumulating data in this test; we will then compare it against the real data.
            TestEntryHandler entryHandler = new TestEntryHandler(readContentsStream);
            entryHandlers.put(segmentId, entryHandler);
            AsyncReadResultProcessor readResultProcessor = new AsyncReadResultProcessor(readResult, entryHandler, context.executorService.get());
            readResultProcessor.startAsync().awaitRunning();
            processorsBySegment.put(segmentId, readResultProcessor);
        }

        // 3. Add a bunch of writes.
        appendData(allSegmentIds, segmentContents, context, triggerFutureReadsCallback);

        // 4. Merge all the batches.
        beginMergeBatches(batchesBySegment, segmentContents, context);
        completeMergeBatches(batchesBySegment, context);
        context.readIndex.triggerFutureReads(segmentIds);

        // 5. Add more appends (to the parent segments)
        for (int i = 0; i < 5; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeCount.incrementAndGet());

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getDurableLogLength();
                segmentMetadata.setDurableLogLength(offset + data.length);
                appendToReadIndex(context, segmentId, offset, data);
                recordAppend(segmentId, data, segmentContents);
                triggerFutureReadsCallback.run();
            }
        }

        // 6. Seal those segments that we need to seal.
        segmentsToSeal.forEach(segmentId -> context.metadata.getStreamSegmentMetadata(segmentId).markSealed());

        // Trigger future reads on all segments we know about; some may not have had a trigger in a while (see callback above).
        context.readIndex.triggerFutureReads(segmentIds);

        // Now wait for all the reads to complete, and verify their results against the expected output.
        ServiceShutdownListener.awaitShutdown(processorsBySegment.values(), TIMEOUT, true);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<Long, TestEntryHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().error.get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail("Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue().error.get());
                }
            }
        }

        // Compare, byte-by-byte, the outcome of the tail reads.
        Assert.assertEquals("Unexpected number of segments were read.", segmentContents.size(), readContents.size());
        for (long segmentId : segmentIds) {
            boolean isSealed = segmentsToSeal.contains(segmentId);

            byte[] expectedData = segmentContents.get(segmentId).toByteArray();
            byte[] actualData = readContents.get(segmentId).toByteArray();
            int expectedLength = isSealed ? (int) expectedData.length : nonSealReadLimit;
            Assert.assertEquals("Unexpected read length for segment " + expectedData.length, expectedLength, actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + expectedData, expectedData, 0, actualData, 0, actualData.length);
        }
    }

    /**
     * Tests the handling of invalid operations. Scenarios include:
     * * Appends at wrong offsets
     * * Bad SegmentIds
     * * Invalid merge operations or sequences (complete before merge, merging non-batches, etc.)
     * * Operations not allowed in or not in recovery
     */
    @Test
    public void testInvalidOperations() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Create a segment and a batch.
        long segmentId = 0;
        String segmentName = getSegmentName((int) segmentId);
        context.metadata.mapStreamSegmentId(segmentName, segmentId);
        initializeSegment(segmentId, context, 0, 0);

        long batchId = segmentId + 1;
        String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(segmentName);
        context.metadata.mapStreamSegmentId(batchName, batchId, segmentId);
        initializeSegment(batchId, context, 0, 0);

        byte[] appendData = "foo".getBytes();
        UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        long segmentOffset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(segmentOffset + appendData.length);
        appendToReadIndex(context, segmentId, segmentOffset, appendData);

        UpdateableSegmentMetadata batchMetadata = context.metadata.getStreamSegmentMetadata(batchId);
        long batchOffset = batchMetadata.getDurableLogLength();
        batchMetadata.setDurableLogLength(batchOffset + appendData.length);
        appendToReadIndex(context, batchId, batchOffset, appendData);

        // 1. Appends at wrong offsets.
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with an offset beyond the Segment's DurableLogOffset.",
                () -> appendToReadIndex(context, segmentId, Integer.MAX_VALUE, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid offset.",
                () -> appendToReadIndex(context, segmentId, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        // 2. Appends or reads with wrong SegmentIds
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid SegmentId.",
                () -> appendToReadIndex(context, batchId + 1, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "read did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.read(batchId + 1, 0, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // 3. TriggerFutureReads with wrong Segment Ids
        ArrayList<Long> badSegmentIds = new ArrayList<>();
        badSegmentIds.add(batchId + 1);
        AssertExtensions.assertThrows(
                "triggerFutureReads did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.triggerFutureReads(badSegmentIds),
                ex -> ex instanceof IllegalArgumentException);

        // 4. Merge with invalid arguments.
        long secondSegmentId = batchId + 1;
        context.metadata.mapStreamSegmentId(getSegmentName((int) secondSegmentId), secondSegmentId);
        initializeSegment(secondSegmentId, context, 0, 0);
        AssertExtensions.assertThrows(
                "beginMerge did not throw the correct exception when attempting to merge a stand-along Segment.",
                () -> context.readIndex.beginMerge(secondSegmentId, 0, segmentId),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "completeMerge did not throw the correct exception when called on a Batch that did not have beginMerge called for.",
                () -> context.readIndex.completeMerge(segmentId, batchId),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "beginMerge did not throw the correct exception when called on a Batch that was not sealed.",
                () -> context.readIndex.beginMerge(segmentId, 0, batchId),
                ex -> ex instanceof IllegalArgumentException);

        batchMetadata.markSealed();
        long mergeOffset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(mergeOffset + batchMetadata.getLength());
        context.readIndex.beginMerge(segmentId, mergeOffset, batchId);
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when called on a Batch that was already sealed.",
                () -> appendToReadIndex(context, batchId, batchMetadata.getLength(), "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the ability to read data from Storage.
     */
    @Test
    public void testStorageReads() throws Exception {
        // Create all the segments in the metadata.
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all batch names into the segment list. For this test, we do not care what kind of Segment we have.
        batchesBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Check all the appended data.
        checkReadIndex("StorageReads", segmentContents, context);

        // Pretty brutal, but will do the job for this test: delete all segments from the storage. This way, if something
        // wasn't cached properly in the last read, the ReadIndex would delegate to Storage, which would fail.
        for (long segmentId : segmentIds) {
            context.storage.delete(context.metadata.getStreamSegmentMetadata(segmentId).getName(), TIMEOUT).join();
        }

        // Now do the read again - if everything was cached properly in the previous call to 'checkReadIndex', no Storage
        // call should be executed.
        checkReadIndex("CacheReads", segmentContents, context);
    }

    /**
     * Tests the ability to handle Storage read failures.
     */
    @Test
    public void testStorageFailedReads() {
        // Create all segments (Storage and Metadata).
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        createSegmentsInStorage(context);

        // Read beyond Storage actual offset (metadata is corrupt)
        long testSegmentId = segmentIds.get(0);
        UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(testSegmentId);
        sm.setStorageLength(1024 * 1024);
        sm.setDurableLogLength(1024 * 1024);

        AssertExtensions.assertThrows(
                "Unexpected exception when attempting to read beyond the Segment length in Storage.",
                () -> {
                    @Cleanup
                    ReadResult readResult = context.readIndex.read(testSegmentId, 0, 100, TIMEOUT);
                    Assert.assertTrue("Unexpected value from hasNext() when there should be at least one ReadResultEntry.", readResult.hasNext());
                    ReadResultEntry entry = readResult.next();
                    Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage, entry.getType());
                    entry.requestContent(TIMEOUT);
                    entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                },
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        // Segment not exists (exists in metadata, but not in Storage)
        context.storage.delete(sm.getName(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Unexpected exception when attempting to from a segment that exists in Metadata, but not in Storage.",
                () -> {
                    @Cleanup
                    ReadResult readResult = context.readIndex.read(testSegmentId, 0, 100, TIMEOUT);
                    Assert.assertTrue("Unexpected value from hasNext() when there should be at least one ReadResultEntry.", readResult.hasNext());
                    ReadResultEntry entry = readResult.next();
                    Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage, entry.getType());
                    entry.requestContent(TIMEOUT);
                    entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                },
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests the ability to perform mixed reads (Storage and DurableLog-only data).
     */
    @Test
    public void testMixedReads() throws Exception {
        // Create all the segments in the metadata.
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> batchesBySegment = createBatches(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all batch names into the segment list. For this test, we do not care what kind of Segment we have.
        batchesBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Append data (in read index).
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    //region Helpers

    private void createSegmentsInStorage(TestContext context) {
        for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
            SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            context.storage.create(sm.getName(), TIMEOUT).join();
        }
    }

    private void appendToReadIndex(TestContext context, long segmentId, long offset, byte[] data) {
        CacheKey key = new CacheKey(segmentId, offset);
        context.cache.insert(key, data);
        try {
            context.readIndex.append(key, data.length);
        } catch (Throwable ex) {
            context.cache.remove(key);
            throw ex;
        }
    }

    private void appendData(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        appendData(segmentIds, segmentContents, context, null);
    }

    private void appendData(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context, Runnable callback) throws Exception {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getDurableLogLength();
                segmentMetadata.setDurableLogLength(offset + data.length);
                appendToReadIndex(context, segmentId, offset, data);
                recordAppend(segmentId, data, segmentContents);
                if (callback != null) {
                    callback.run();
                }
            }
        }
    }

    private void appendDataInStorage(TestContext context, HashMap<Long, ByteArrayOutputStream> segmentContents) throws Exception {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
                UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(sm.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = context.storage.getStreamSegmentInfo(sm.getName(), TIMEOUT).join().getLength();
                context.storage.write(sm.getName(), offset, new ByteArrayInputStream(data), data.length, TIMEOUT).join();

                // Update metadata appropriately.
                sm.setStorageLength(offset + data.length);
                if (sm.getStorageLength() > sm.getDurableLogLength()) {
                    sm.setDurableLogLength(sm.getStorageLength());
                }

                recordAppend(segmentId, data, segmentContents);
            }
        }
    }

    private byte[] getAppendData(String segmentName, long segmentId, int segmentAppendSeq, int writeId) {
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d", segmentName, segmentId, segmentAppendSeq, writeId).getBytes();
    }

    private void completeMergeBatches(HashMap<Long, ArrayList<Long>> batchesBySegment, TestContext context) throws Exception {
        for (Map.Entry<Long, ArrayList<Long>> e : batchesBySegment.entrySet()) {
            long parentId = e.getKey();
            for (long batchId : e.getValue()) {
                UpdateableSegmentMetadata batchMetadata = context.metadata.getStreamSegmentMetadata(batchId);
                batchMetadata.markDeleted();
                context.readIndex.completeMerge(parentId, batchId);
            }
        }
    }

    private void beginMergeBatches(HashMap<Long, ArrayList<Long>> batchesBySegment, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (Map.Entry<Long, ArrayList<Long>> e : batchesBySegment.entrySet()) {
            long parentId = e.getKey();
            UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);

            for (long batchId : e.getValue()) {
                UpdateableSegmentMetadata batchMetadata = context.metadata.getStreamSegmentMetadata(batchId);

                // Batch must be sealed first.
                batchMetadata.markSealed();

                // Update parent length.
                long offset = parentMetadata.getDurableLogLength();
                parentMetadata.setDurableLogLength(offset + batchMetadata.getDurableLogLength());

                // Do the ReadIndex merge.
                context.readIndex.beginMerge(parentId, offset, batchId);

                // Update the metadata.
                batchMetadata.markMerged();

                // Update parent contents.
                segmentContents.get(parentId).write(segmentContents.get(batchId).toByteArray());
                segmentContents.remove(batchId);
            }
        }
    }

    private void checkReadIndex(String testId, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (long segmentId : segmentContents.keySet()) {
            long segmentLength = context.metadata.getStreamSegmentMetadata(segmentId).getDurableLogLength();
            byte[] expectedData = segmentContents.get(segmentId).toByteArray();

            long expectedCurrentOffset = 0;
            @Cleanup
            ReadResult readResult = context.readIndex.read(segmentId, expectedCurrentOffset, (int) segmentLength, TIMEOUT);
            Assert.assertTrue(testId + ": Empty read result for segment " + segmentId, readResult.hasNext());

            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan(testId + ": getRequestedReadLength should be a positive integer for segment " + segmentId, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals(testId + ": Unexpected value from getStreamSegmentOffset for segment " + segmentId, expectedCurrentOffset, readEntry.getStreamSegmentOffset());

                // Since this is a non-sealed segment, we only expect Cache or Storage read result entries.
                Assert.assertTrue(testId + ": Unexpected type of ReadResultEntry for non-sealed segment " + segmentId, readEntry.getType() == ReadResultEntryType.Cache || readEntry.getType() == ReadResultEntryType.Storage);
                if (readEntry.getType() == ReadResultEntryType.Cache) {
                    Assert.assertTrue(testId + ": getContent() did not return a completed future (ReadResultEntryType.Cache) for segment" + segmentId, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                } else if (readEntry.getType() == ReadResultEntryType.Storage) {
                    Assert.assertFalse(testId + ": getContent() did not return a non-completed future (ReadResultEntryType.Storage) for segment" + segmentId, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                }

                // Request content, in case it wasn't returned yet.
                readEntry.requestContent(TIMEOUT);
                ReadResultEntryContents readEntryContents = readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                AssertExtensions.assertGreaterThan(testId + ": getContent() returned an empty result entry for segment " + segmentId, 0, readEntryContents.getLength());

                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals(testId + ": Unexpected data read from segment " + segmentId + " at offset " + expectedCurrentOffset, expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());

                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue(testId + ": ReadResult was not closed post-full-consumption for segment" + segmentId, readResult.isClosed());
        }
    }

    private <T> void recordAppend(T segmentIdentifier, byte[] data, HashMap<T, ByteArrayOutputStream> segmentContents) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentIdentifier, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentIdentifier, contents);
        }

        contents.write(data);
    }

    private ArrayList<Long> createSegments(TestContext context) {
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String name = getSegmentName(i);
            context.metadata.mapStreamSegmentId(name, i);
            initializeSegment(i, context, 0, 0);
            segmentIds.add((long) i);
        }

        return segmentIds;
    }

    private HashMap<Long, ArrayList<Long>> createBatches(Collection<Long> segmentIds, TestContext context) {
        // Create the batches.
        HashMap<Long, ArrayList<Long>> batches = new HashMap<>();
        long batchId = Integer.MAX_VALUE;
        for (long parentId : segmentIds) {
            ArrayList<Long> segmentBatches = new ArrayList<>();
            batches.put(parentId, segmentBatches);
            SegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);

            for (int i = 0; i < BATCHES_PER_SEGMENT; i++) {
                String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(parentMetadata.getName());
                context.metadata.mapStreamSegmentId(batchName, batchId, parentId);
                initializeSegment(batchId, context, 0, 0);
                segmentBatches.add(batchId);
                batchId++;
            }
        }

        return batches;
    }

    private String getSegmentName(int id) {
        return "Segment_" + id;
    }

    private void initializeSegment(long segmentId, TestContext context, long storageLength, long durableLogLength) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setDurableLogLength(durableLogLength);
        metadata.setStorageLength(storageLength);
    }

    //endregion

    //region TestContext

    private static class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final ContainerReadIndex readIndex;
        final CloseableExecutorService executorService;
        final Cache cache;
        final Storage storage;

        TestContext() {
            this.executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.cache = new InMemoryCache(Integer.toString(CONTAINER_ID));
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.storage = new InMemoryStorage();
            this.readIndex = new ContainerReadIndex(DEFAULT_CONFIG, this.metadata, this.cache, this.storage, this.executorService.get());
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.cache.close();
            this.storage.close();
            this.executorService.close();
        }
    }

    //endregion

    //region TestEntryHandler

    private static class TestEntryHandler implements AsyncReadResultEntryHandler {
        final AtomicReference<Throwable> error = new AtomicReference<>();
        private final ByteArrayOutputStream readContents;

        TestEntryHandler(ByteArrayOutputStream readContents) {
            this.readContents = readContents;
        }

        @Override
        public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
            return true;
        }

        @Override
        public boolean processEntry(ReadResultEntry e) {
            ReadResultEntryContents c = e.getContent().join();
            byte[] data = new byte[c.getLength()];
            try {
                StreamHelpers.readAll(c.getData(), data, 0, data.length);
                readContents.write(data);
                return true;
            } catch (Exception ex) {
                processError(e, ex);
                return false;
            }
        }

        @Override
        public void processError(ReadResultEntry entry, Throwable cause) {
            this.error.set(cause);
        }

        @Override
        public Duration getRequestContentTimeout() {
            return TIMEOUT;
        }
    }

    //endregion
}
