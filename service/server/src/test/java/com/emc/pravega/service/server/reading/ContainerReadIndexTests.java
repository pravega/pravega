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
import com.emc.pravega.common.util.PropertyBag;
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
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Unit tests for ContainerReadIndex class.
 */
public class ContainerReadIndexTests {
    private static final int SEGMENT_COUNT = 100;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int THREAD_POOL_SIZE = 50;
    private static final int CONTAINER_ID = 123;
    private static final ReadIndexConfig DEFAULT_CONFIG = ConfigHelpers.createReadIndexConfigWithInfiniteCachePolicy(
            PropertyBag.create().with(ReadIndexConfig.PROPERTY_STORAGE_READ_MIN_LENGTH, 100).with(
                    ReadIndexConfig.PROPERTY_STORAGE_READ_MAX_LENGTH, 1024));
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    /**
     * Tests the basic append-read functionality of the ContainerReadIndex, with data fully in it (no tail reads).
     */
    @Test
    public void testAppendRead() throws Exception {
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we
        // have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Add a bunch of writes.
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    /**
     * Tests the merging of Transactions into their parent StreamSegments.
     */
    @Test
    public void testMerge() throws Exception {
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Put all segment names into one list, for easier appends (but still keep the original lists at hand - we'll
        // need them later).
        ArrayList<Long> allSegmentIds = new ArrayList<>(segmentIds);
        transactionsBySegment.values().forEach(allSegmentIds::addAll);

        // Add a bunch of writes.
        appendData(allSegmentIds, segmentContents, context);

        // Begin-merge all Transactions (part 1/2), and check contents.
        beginMergeTransactions(transactionsBySegment, segmentContents, context);
        checkReadIndex("BeginMerge", segmentContents, context);

        // Complete the merger (part 2/2), and check contents.
        completeMergeTransactions(transactionsBySegment, context);
        checkReadIndex("CompleteMerge", segmentContents, context);
    }

    /**
     * Tests the behavior of Future Reads. Scenarios tested include:
     * * Regular appends
     * * Segment sealing
     * * Transaction merging.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testFutureReads() throws Exception {
        final int nonSealReadLimit = APPENDS_PER_SEGMENT * 25; // About 40-50% of the entire segment length.
        final int triggerFutureReadsEvery = 3; // How many appends to trigger Future reads.
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        HashMap<Long, ByteArrayOutputStream> readContents = new HashMap<>();
        HashSet<Long> segmentsToSeal = new HashSet<>();
        HashMap<Long, AsyncReadResultProcessor> processorsBySegment = new HashMap<>();
        HashMap<Long, TestEntryHandler> entryHandlers = new HashMap<>();

        // 1. Put all segment names into one list, for easier appends (but still keep the original lists at hand -
        // we'll need them later).
        ArrayList<Long> allSegmentIds = new ArrayList<>(segmentIds);
        transactionsBySegment.values().forEach(allSegmentIds::addAll);

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
            AsyncReadResultProcessor readResultProcessor = new AsyncReadResultProcessor(readResult, entryHandler,
                    context.executorService.get());
            readResultProcessor.startAsync().awaitRunning();
            processorsBySegment.put(segmentId, readResultProcessor);
        }

        // 3. Add a bunch of writes.
        appendData(allSegmentIds, segmentContents, context, triggerFutureReadsCallback);

        // 4. Merge all the Transactions.
        beginMergeTransactions(transactionsBySegment, segmentContents, context);
        completeMergeTransactions(transactionsBySegment, context);
        context.readIndex.triggerFutureReads(segmentIds);

        // 5. Add more appends (to the parent segments)
        for (int i = 0; i < 5; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeCount.incrementAndGet());

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getDurableLogLength();
                segmentMetadata.setDurableLogLength(offset + data.length);
                context.readIndex.append(segmentId, offset, data);
                recordAppend(segmentId, data, segmentContents);
                triggerFutureReadsCallback.run();
            }
        }

        // 6. Seal those segments that we need to seal.
        segmentsToSeal.forEach(segmentId -> context.metadata.getStreamSegmentMetadata(segmentId).markSealed());

        // Trigger future reads on all segments we know about; some may not have had a trigger in a while (see
        // callback above).
        context.readIndex.triggerFutureReads(segmentIds);

        // Now wait for all the reads to complete, and verify their results against the expected output.
        ServiceShutdownListener.awaitShutdown(processorsBySegment.values(), TIMEOUT, true);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<Long, TestEntryHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().error.get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if
                // the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail(
                            "Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue()
                                    .error.get());
                }
            }
        }

        // Compare, byte-by-byte, the outcome of the tail reads.
        Assert.assertEquals("Unexpected number of segments were read.", segmentContents.size(), readContents.size());
        for (long segmentId : segmentIds) {
            boolean isSealed = segmentsToSeal.contains(segmentId);

            byte[] expectedData = segmentContents.get(segmentId).toByteArray();
            byte[] actualData = readContents.get(segmentId).toByteArray();
            int expectedLength = isSealed ? expectedData.length : nonSealReadLimit;
            Assert.assertEquals("Unexpected read length for segment " + expectedData.length, expectedLength,
                    actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + segmentId, expectedData, 0,
                    actualData, 0, actualData.length);
        }
    }

    /**
     * Tests the handling of invalid operations. Scenarios include:
     * * Appends at wrong offsets
     * * Bad SegmentIds
     * * Invalid merge operations or sequences (complete before merge, merging non-Transactions, etc.)
     * * Operations not allowed in or not in recovery
     */
    @Test
    public void testInvalidOperations() throws Exception {
        @Cleanup TestContext context = new TestContext();

        // Create a segment and a Transaction.
        long segmentId = 0;
        String segmentName = getSegmentName((int) segmentId);
        context.metadata.mapStreamSegmentId(segmentName, segmentId);
        initializeSegment(segmentId, context);

        long transactionId = segmentId + 1;
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        context.metadata.mapStreamSegmentId(transactionName, transactionId, segmentId);
        initializeSegment(transactionId, context);

        byte[] appendData = "foo".getBytes();
        UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        long segmentOffset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(segmentOffset + appendData.length);
        context.readIndex.append(segmentId, segmentOffset, appendData);

        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        long transactionOffset = transactionMetadata.getDurableLogLength();
        transactionMetadata.setDurableLogLength(transactionOffset + appendData.length);
        context.readIndex.append(transactionId, transactionOffset, appendData);

        // 1. Appends at wrong offsets.
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with an offset beyond the Segment's " +
                        "DurableLogOffset.",
                () -> context.readIndex.append(segmentId, Integer.MAX_VALUE, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("append did not throw the correct exception when provided with invalid offset.",
                () -> context.readIndex.append(segmentId, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        // 2. Appends or reads with wrong SegmentIds
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.append(transactionId + 1, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("read did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.read(transactionId + 1, 0, 1, TIMEOUT),
                ex -> ex instanceof IllegalArgumentException);

        // 3. TriggerFutureReads with wrong Segment Ids
        ArrayList<Long> badSegmentIds = new ArrayList<>();
        badSegmentIds.add(transactionId + 1);
        AssertExtensions.assertThrows(
                "triggerFutureReads did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.triggerFutureReads(badSegmentIds),
                ex -> ex instanceof IllegalArgumentException);

        // 4. Merge with invalid arguments.
        long secondSegmentId = transactionId + 1;
        context.metadata.mapStreamSegmentId(getSegmentName((int) secondSegmentId), secondSegmentId);
        initializeSegment(secondSegmentId, context);
        AssertExtensions.assertThrows(
                "beginMerge did not throw the correct exception when attempting to merge a stand-along Segment.",
                () -> context.readIndex.beginMerge(secondSegmentId, 0, segmentId),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "completeMerge did not throw the correct exception when called on a Transaction that did not have " +
                        "beginMerge called for.",
                () -> context.readIndex.completeMerge(segmentId, transactionId),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "beginMerge did not throw the correct exception when called on a Transaction that was not sealed.",
                () -> context.readIndex.beginMerge(segmentId, 0, transactionId),
                ex -> ex instanceof IllegalArgumentException);

        transactionMetadata.markSealed();
        long mergeOffset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(mergeOffset + transactionMetadata.getLength());
        context.readIndex.beginMerge(segmentId, mergeOffset, transactionId);
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when called on a Transaction that was already sealed.",
                () -> context.readIndex.append(transactionId, transactionMetadata.getLength(), "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the ability to read data from Storage.
     */
    @Test
    public void testStorageReads() throws Exception {
        // Create all the segments in the metadata.
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we
        // have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Check all the appended data.
        checkReadIndex("StorageReads", segmentContents, context);

        // Pretty brutal, but will do the job for this test: delete all segments from the storage. This way, if
        // something
        // wasn't cached properly in the last read, the ReadIndex would delegate to Storage, which would fail.
        for (long segmentId : segmentIds) {
            context.storage.delete(context.metadata.getStreamSegmentMetadata(segmentId).getName(), TIMEOUT).join();
        }

        // Now do the read again - if everything was cached properly in the previous call to 'checkReadIndex', no
        // Storage
        // call should be executed.
        checkReadIndex("CacheReads", segmentContents, context);
    }

    /**
     * Tests the ability to handle Storage read failures.
     */
    @Test
    public void testStorageFailedReads() {
        // Create all segments (Storage and Metadata).
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        createSegmentsInStorage(context);

        // Read beyond Storage actual offset (metadata is corrupt)
        long testSegmentId = segmentIds.get(0);
        UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(testSegmentId);
        sm.setStorageLength(1024 * 1024);
        sm.setDurableLogLength(1024 * 1024);

        AssertExtensions.assertThrows(
                "Unexpected exception when attempting to read beyond the Segment length in Storage.", () -> {
                    @Cleanup ReadResult readResult = context.readIndex.read(testSegmentId, 0, 100, TIMEOUT);
                    Assert.assertTrue(
                            "Unexpected value from hasNext() when there should be at least one " + "ReadResultEntry.",
                            readResult.hasNext());
                    ReadResultEntry entry = readResult.next();
                    Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage,
                            entry.getType());
                    entry.requestContent(TIMEOUT);
                    entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }, ex -> ex instanceof ArrayIndexOutOfBoundsException);

        // Segment not exists (exists in metadata, but not in Storage)
        context.storage.delete(sm.getName(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "Unexpected exception when attempting to from a segment that exists in Metadata, but not in Storage.",
                () -> {
                    @Cleanup ReadResult readResult = context.readIndex.read(testSegmentId, 0, 100, TIMEOUT);
                    Assert.assertTrue(
                            "Unexpected value from hasNext() when there should be at least one " + "ReadResultEntry.",
                            readResult.hasNext());
                    ReadResultEntry entry = readResult.next();
                    Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage,
                            entry.getType());
                    entry.requestContent(TIMEOUT);
                    entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }, ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests the ability to perform mixed reads (Storage and DurableLog-only data).
     */
    @Test
    public void testMixedReads() throws Exception {
        // Create all the segments in the metadata.
        @Cleanup TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we
        // have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Append data (in read index).
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    /**
     * Tests the ability to evict entries from the ReadIndex under various conditions:
     * * If an entry is aged out
     * * If an entry is pushed out because of cache space pressure.
     * <p>
     * This also verifies that certain entries, such as RedirectReadIndexEntries and entries after the Storage Offset
     * are not removed.
     * <p>
     * The way this test goes is as follows (it's pretty subtle, because there aren't many ways to hook into the
     * ReadIndex and see what it's doing)
     * 1. It creates a bunch of segments, and populates them in storage (each) up to offset N/2-1 (this is called
     * pre-storage)
     * 2. It populates the ReadIndex for each of those segments from offset N/2 to offset N-1 (this is called
     * post-storage)
     * 3. It loads all the data from Storage into the ReadIndex, in entries of size equal to those already loaded in
     * step #2.
     * 3a. At this point, all the entries added in step #2 have Generations 0..A/4-1, and step #3 have generations
     * A/4..A-1.
     * 4. Append more data at the end. This forces the generation to increase to 1.25A.
     * 4a. Nothing should be evicted from the cache now, since the earliest items are all post-storage.
     * 5. We 'touch' (read) the first 1/3 of pre-storage entries (offsets 0..N/4).
     * 5a. At this point, those entries (offsets 0..N/6) will have the newest generations (1.25A..1.5A)
     * 6. We append more data (equivalent to the data we touched)
     * 6a. Nothing should be evicted, since those generations that were just eligible for removal were touched and
     * bumped up.
     * 7. We forcefully increase the current generation by 1 (without touching the ReadIndex)
     * 7a. At this point, we expect all the pre-storage items, except the touched ones, to be evicted. This is
     * generations 0.25A-0.75A.
     * 8. Update the metadata and indicate that all the post-storage entries are now pre-storage and bump the
     * generation by 0.75A.
     * 8a. At this point, we expect all former post-storage items and pre-storage items to be evicted (in this order).
     * <p>
     * The final order of eviction (in terms of offsets, for each segment), is:
     * * 0.25N-0.75N, 0.75N..N, N..1.25N, 0..0.25N, 1.25N..1.5N (remember that we added quite a bunch of items after
     * the initial run).
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testCacheEviction() throws Exception {
        // Create a CachePolicy with a set number of generations and a known max size.
        // Each generation contains exactly one entry, so the number of generations is also the number of entries.
        final int appendSize = 100;
        final int entriesPerSegment = 100; // This also doubles as number of generations (each generation, we add one
        // append for each segment).
        final int cacheMaxSize = SEGMENT_COUNT * entriesPerSegment * appendSize;
        final int postStorageEntryCount = entriesPerSegment / 4; // 25% of the entries are beyond the StorageOffset
        final int preStorageEntryCount = entriesPerSegment - postStorageEntryCount; // 75% of the entries are before
        // the StorageOffset.
        CachePolicy cachePolicy = new CachePolicy(cacheMaxSize, Duration.ofMillis(1000 * 2 * entriesPerSegment),
                Duration.ofMillis(1000));
        ReadIndexConfig config = ConfigHelpers.createReadIndexConfigWithInfiniteCachePolicy(
                PropertyBag.create().with(ReadIndexConfig.PROPERTY_STORAGE_READ_MIN_LENGTH, appendSize).with(
                        ReadIndexConfig.PROPERTY_STORAGE_READ_MAX_LENGTH, appendSize)); // To properly test
        // this, we want predictable storage reads.

        ArrayList<CacheKey> removedKeys = new ArrayList<>();
        @Cleanup TestContext context = new TestContext(config, cachePolicy);
        context.cache.removeCallback = removedKeys::add; // Record every cache removal.

        // Create the segments (metadata + storage).
        ArrayList<Long> segmentIds = createSegments(context);
        createSegmentsInStorage(context);

        // Populate the Storage with appropriate data.
        byte[] preStorageData = new byte[preStorageEntryCount * appendSize];
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            context.storage.write(sm.getName(), 0, new ByteArrayInputStream(preStorageData), preStorageData.length,
                    TIMEOUT).join();
            sm.setStorageLength(preStorageData.length);
            sm.setDurableLogLength(preStorageData.length);
        }

        // Callback that appends one entry at the end of the given segment id.
        Consumer<Long> appendOneEntry = segmentId -> {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            byte[] data = new byte[appendSize];
            long offset = sm.getDurableLogLength();
            sm.setDurableLogLength(offset + data.length);
            context.readIndex.append(segmentId, offset, data);
        };

        // Populate the ReadIndex with the Append entries (post-StorageOffset)
        for (int i = 0; i < postStorageEntryCount; i++) {
            segmentIds.forEach(appendOneEntry);

            // Each time we make a round of appends (one per segment), we increment the generation in the CacheManager.
            context.cacheManager.applyCachePolicy();
        }

        // Read all the data from Storage, making sure we carefully associate them with the proper generation.
        for (int i = 0; i < preStorageEntryCount; i++) {
            long offset = i * appendSize;
            for (long segmentId : segmentIds) {
                @Cleanup ReadResult result = context.readIndex.read(segmentId, offset, appendSize, TIMEOUT);
                ReadResultEntry resultEntry = result.next();
                Assert.assertEquals(
                        "Unexpected type of ReadResultEntry when trying to load up data into the " + "ReadIndex Cache.",
                        ReadResultEntryType.Storage, resultEntry.getType());
                resultEntry.requestContent(TIMEOUT);
                ReadResultEntryContents contents = resultEntry.getContent().get(TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);
                Assert.assertFalse("Not expecting more data to be available for reading.", result.hasNext());
                Assert.assertEquals(
                        "Unexpected ReadResultEntry length when trying to load up data into the ReadIndex" + " Cache.",
                        appendSize, contents.getLength());
            }

            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (cache is not full).", 0,
                removedKeys.size());

        // Append more data (equivalent to all post-storage entries), and verify that NO entries are being evicted
        // (we cannot evict post-storage entries).
        for (int i = 0; i < postStorageEntryCount; i++) {
            segmentIds.forEach(appendOneEntry);
            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals(
                "Not expecting any removed Cache entries at this point (only eligible entries were " + "post-storage).",
                0, removedKeys.size());

        // 'Touch' the first few entries read from storage. This should move them to the back of the queue (they
        // won't be the first ones to be evicted).
        int touchCount = preStorageEntryCount / 3;
        for (int i = 0; i < touchCount; i++) {
            long offset = i * appendSize;
            for (long segmentId : segmentIds) {
                @Cleanup ReadResult result = context.readIndex.read(segmentId, offset, appendSize, TIMEOUT);
                ReadResultEntry resultEntry = result.next();
                Assert.assertEquals(
                        "Unexpected type of ReadResultEntry when trying to load up data into the " + "ReadIndex Cache.",
                        ReadResultEntryType.Cache, resultEntry.getType());
            }
        }

        // Append more data (equivalent to the amount of data we 'touched'), and verify that the entries we just
        // touched are not being removed..
        for (int i = 0; i < touchCount; i++) {
            segmentIds.forEach(appendOneEntry);
            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals(
                "Not expecting any removed Cache entries at this point (we touched old entries and they " + "now " +
                        "have" + " the newest generation).",
                0, removedKeys.size());

        // Increment the generations so that we are caught up to just before the generation where the "touched" items
        // now live.
        context.cacheManager.applyCachePolicy();

        // We expect all but the 'touchCount' pre-Storage entries to be removed.
        int expectedRemovalCount = (preStorageEntryCount - touchCount) * SEGMENT_COUNT;
        Assert.assertEquals("Unexpected number of removed entries after having forced out all pre-storage entries.",
                expectedRemovalCount, removedKeys.size());

        // Now update the metadata and indicate that all the post-storage data has been moved to storage.
        segmentIds.forEach(segmentId -> {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            sm.setStorageLength(sm.getDurableLogLength());
        });

        // We add one artificial entry, which we'll be touching forever and ever; this forces the CacheManager to
        // update its current generation every time. We will be ignoring this entry for our test.
        SegmentMetadata readSegment = context.metadata.getStreamSegmentMetadata(segmentIds.get(0));
        appendOneEntry.accept(readSegment.getId());

        // Now evict everything (whether by size of by aging out).
        for (int i = 0; i < cachePolicy.getMaxGenerations(); i++) {
            @Cleanup ReadResult result = context.readIndex.read(readSegment.getId(),
                    readSegment.getDurableLogLength() - appendSize, appendSize, TIMEOUT);
            result.next();
            context.cacheManager.applyCachePolicy();
        }

        int expectedRemovalCountPerSegment = entriesPerSegment + touchCount + postStorageEntryCount;
        int expectedTotalRemovalCount = SEGMENT_COUNT * expectedRemovalCountPerSegment;
        Assert.assertEquals("Unexpected number of removed entries after having forced out all the entries.",
                expectedTotalRemovalCount, removedKeys.size());

        // Finally, verify that the evicted items are in the correct order (for each segment). See this test's
        // description for details.
        for (long segmentId : segmentIds) {
            List<CacheKey> segmentRemovedKeys = removedKeys.stream().filter(
                    key -> key.getStreamSegmentId() == segmentId).collect(Collectors.toList());
            Assert.assertEquals("Unexpected number of removed entries for segment " + segmentId,
                    expectedRemovalCountPerSegment, segmentRemovedKeys.size());

            // The correct order of eviction (N=entriesPerSegment) is: 0.25N-0.75N, 0.75N..N, N..1.25N, 0..0.25N,
            // 1.25N..1.5N.
            // This is equivalent to the following tests
            // 0.25N-1.25N
            checkOffsets(segmentRemovedKeys, segmentId, 0, entriesPerSegment, entriesPerSegment * appendSize / 4,
                    appendSize);

            // 0..0.25N
            checkOffsets(segmentRemovedKeys, segmentId, entriesPerSegment, entriesPerSegment / 4, 0, appendSize);

            //1.25N..1.5N
            checkOffsets(segmentRemovedKeys, segmentId, entriesPerSegment + entriesPerSegment / 4,
                    entriesPerSegment / 4, (int) (entriesPerSegment * appendSize * 1.25), appendSize);
        }
    }

    //region Helpers

    private void checkOffsets(List<CacheKey> removedKeys, long segmentId, int startIndex, int count, int startOffset,
                              int stepIncrease) {
        int expectedStartOffset = startOffset;
        for (int i = 0; i < count; i++) {
            int listIndex = startIndex + i;
            CacheKey currentKey = removedKeys.get(startIndex + i);
            Assert.assertEquals(
                    String.format("Unexpected CacheKey.SegmentOffset at index %d for SegmentId %d.", listIndex,
                            segmentId), expectedStartOffset, currentKey.getOffset());
            expectedStartOffset += stepIncrease;
        }
    }

    private void createSegmentsInStorage(TestContext context) {
        for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
            SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            context.storage.create(sm.getName(), TIMEOUT).join();
        }
    }

    private void appendData(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents,
                            TestContext context) {
        appendData(segmentIds, segmentContents, context, null);
    }

    private void appendData(Collection<Long> segmentIds, HashMap<Long, ByteArrayOutputStream> segmentContents,
                            TestContext context, Runnable callback) {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getDurableLogLength();
                segmentMetadata.setDurableLogLength(offset + data.length);
                context.readIndex.append(segmentId, offset, data);
                recordAppend(segmentId, data, segmentContents);
                if (callback != null) {
                    callback.run();
                }
            }
        }
    }

    private void appendDataInStorage(TestContext context, HashMap<Long, ByteArrayOutputStream> segmentContents) {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
                UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(sm.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the DurableLogLength prior to appending; the ReadIndex checks for this.
                long offset = context.storage.getStreamSegmentInfo(sm.getName(), TIMEOUT).join().getLength();
                context.storage.write(sm.getName(), offset, new ByteArrayInputStream(data), data.length,
                        TIMEOUT).join();

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
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d", segmentName, segmentId,
                segmentAppendSeq, writeId).getBytes();
    }

    private void completeMergeTransactions(HashMap<Long, ArrayList<Long>> transactionsBySegment, TestContext context) {
        for (Map.Entry<Long, ArrayList<Long>> e : transactionsBySegment.entrySet()) {
            long parentId = e.getKey();
            for (long transactionId : e.getValue()) {
                UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(
                        transactionId);
                transactionMetadata.markDeleted();
                context.readIndex.completeMerge(parentId, transactionId);
            }
        }
    }

    private void beginMergeTransactions(HashMap<Long, ArrayList<Long>> transactionsBySegment, HashMap<Long,
            ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (Map.Entry<Long, ArrayList<Long>> e : transactionsBySegment.entrySet()) {
            long parentId = e.getKey();
            UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);

            for (long transactionId : e.getValue()) {
                UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(
                        transactionId);

                // Transaction must be sealed first.
                transactionMetadata.markSealed();

                // Update parent length.
                long offset = parentMetadata.getDurableLogLength();
                parentMetadata.setDurableLogLength(offset + transactionMetadata.getDurableLogLength());

                // Do the ReadIndex merge.
                context.readIndex.beginMerge(parentId, offset, transactionId);

                // Update the metadata.
                transactionMetadata.markMerged();

                // Update parent contents.
                segmentContents.get(parentId).write(segmentContents.get(transactionId).toByteArray());
                segmentContents.remove(transactionId);
            }
        }
    }

    private void checkReadIndex(String testId, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext
            context) throws Exception {
        for (long segmentId : segmentContents.keySet()) {
            long segmentLength = context.metadata.getStreamSegmentMetadata(segmentId).getDurableLogLength();
            byte[] expectedData = segmentContents.get(segmentId).toByteArray();

            long expectedCurrentOffset = 0;
            @Cleanup ReadResult readResult = context.readIndex.read(segmentId, expectedCurrentOffset,
                    (int) segmentLength, TIMEOUT);
            Assert.assertTrue(testId + ": Empty read result for segment " + segmentId, readResult.hasNext());

            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan(
                        testId + ": getRequestedReadLength should be a positive integer " + "for segment " + segmentId,
                        0, readEntry.getRequestedReadLength());
                Assert.assertEquals(testId + ": Unexpected value from getStreamSegmentOffset for segment " + segmentId,
                        expectedCurrentOffset, readEntry.getStreamSegmentOffset());

                // Since this is a non-sealed segment, we only expect Cache or Storage read result entries.
                Assert.assertTrue(testId + ": Unexpected type of ReadResultEntry for non-sealed segment " + segmentId,
                        readEntry.getType() == ReadResultEntryType.Cache || readEntry.getType() ==
                                ReadResultEntryType.Storage);
                if (readEntry.getType() == ReadResultEntryType.Cache) {
                    Assert.assertTrue(
                            testId + ": getContent() did not return a completed future (ReadResultEntryType" + "" + "" +
                                    ".Cache) for segment" + segmentId,
                            readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                } else if (readEntry.getType() == ReadResultEntryType.Storage) {
                    Assert.assertFalse(
                            testId + ": getContent() did not return a non-completed future " + "(ReadResultEntryType"
                                    + ".Storage) for segment" + segmentId,
                            readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                }

                // Request content, in case it wasn't returned yet.
                readEntry.requestContent(TIMEOUT);
                ReadResultEntryContents readEntryContents = readEntry.getContent().get(TIMEOUT.toMillis(),
                        TimeUnit.MILLISECONDS);
                AssertExtensions.assertGreaterThan(
                        testId + ": getContent() returned an empty result entry for " + "segment " + segmentId, 0,
                        readEntryContents.getLength());

                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals(
                        testId + ": Unexpected data read from segment " + segmentId + " at" + " offset " +
                                expectedCurrentOffset,
                        expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());

                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue(testId + ": ReadResult was not closed post-full-consumption for segment" + segmentId,
                    readResult.isClosed());
        }
    }

    private <T> void recordAppend(T segmentIdentifier, byte[] data, HashMap<T, ByteArrayOutputStream> segmentContents) {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentIdentifier, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentIdentifier, contents);
        }

        try {
            contents.write(data);
        } catch (IOException ex) {
            Assert.fail(ex.toString());
        }
    }

    private ArrayList<Long> createSegments(TestContext context) {
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String name = getSegmentName(i);
            context.metadata.mapStreamSegmentId(name, i);
            initializeSegment(i, context);
            segmentIds.add((long) i);
        }

        return segmentIds;
    }

    private HashMap<Long, ArrayList<Long>> createTransactions(Collection<Long> segmentIds, TestContext context) {
        // Create the Transactions.
        HashMap<Long, ArrayList<Long>> transactions = new HashMap<>();
        long transactionId = Integer.MAX_VALUE;
        for (long parentId : segmentIds) {
            ArrayList<Long> segmentTransactions = new ArrayList<>();
            transactions.put(parentId, segmentTransactions);
            SegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);

            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(parentMetadata.getName(),
                        UUID.randomUUID());
                context.metadata.mapStreamSegmentId(transactionName, transactionId, parentId);
                initializeSegment(transactionId, context);
                segmentTransactions.add(transactionId);
                transactionId++;
            }
        }

        return transactions;
    }

    private String getSegmentName(int id) {
        return "Segment_" + id;
    }

    private void initializeSegment(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setDurableLogLength(0);
        metadata.setStorageLength(0);
    }

    //endregion

    //region TestContext

    private static class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final ContainerReadIndex readIndex;
        final CloseableExecutorService executorService;
        final TestCacheManager cacheManager;
        final TestCache cache;
        final Storage storage;

        TestContext() {
            this(DEFAULT_CONFIG, DEFAULT_CONFIG.getCachePolicy());
        }

        TestContext(ReadIndexConfig readIndexConfig, CachePolicy cachePolicy) {
            this.executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.cache = new TestCache(Integer.toString(CONTAINER_ID));
            this.metadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.storage = new InMemoryStorage();
            this.cacheManager = new TestCacheManager(cachePolicy, this.executorService.get());
            this.readIndex = new ContainerReadIndex(readIndexConfig, this.metadata, this.cache, this.storage,
                    this.cacheManager, this.executorService.get());
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.cache.close();
            this.storage.close();
            this.cacheManager.close();
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

    private static class TestCache extends InMemoryCache {
        Consumer<CacheKey> removeCallback;

        TestCache(String id) {
            super(id);
        }

        @Override
        public void remove(Cache.Key key) {
            Consumer<CacheKey> callback = this.removeCallback;
            if (callback != null) {
                callback.accept((CacheKey) key);
            }

            super.remove(key);
        }
    }
}
