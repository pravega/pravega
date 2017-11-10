/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.CacheKey;
import io.pravega.segmentstore.server.ConfigHelpers;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.mocks.InMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.shared.segment.StreamSegmentNameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for ContainerReadIndex class.
 */
public class ContainerReadIndexTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_COUNT = 100;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int CONTAINER_ID = 123;

    private static final ReadIndexConfig DEFAULT_CONFIG = ConfigHelpers
            .withInfiniteCachePolicy(ReadIndexConfig.builder()
                                                    .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 0) // Default: Off (we have a special test for this).
                                                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024))
            .build();
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the basic append-read functionality of the ContainerReadIndex, with data fully in it (no tail reads).
     */
    @Test
    public void testAppendRead() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Add a bunch of writes.
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    /**
     * Tests the ability for the ReadIndex to batch multiple index entries together into a bigger read. This test
     * writes a lot of very small appends to the index, then issues a full read (from the beginning) while configuring
     * the read index to return results of no less than a particular size. As an added bonus, it also forces a Storage
     * Read towards the end to make sure the ReadIndex doesn't coalesce those into the result as well.
     */
    @Test
    public void testBatchedRead() throws Exception {
        final int totalAppendLength = 500 * 1000;
        final int maxAppendLength = 100;
        final int minReadLength = 16 * 1024;
        final byte[] segmentData = new byte[totalAppendLength];
        final Random rnd = new Random(0);
        rnd.nextBytes(segmentData);

        final ReadIndexConfig config = ConfigHelpers
                .withInfiniteCachePolicy(ReadIndexConfig.builder().with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, minReadLength))
                .build();

        @Cleanup
        TestContext context = new TestContext(config, config.getCachePolicy());

        // Create the segment in Storage and populate it with all the data (one segment is sufficient for this test).
        final long segmentId = createSegment(0, context);
        createSegmentsInStorage(context);
        final UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        val writeHandle = context.storage.openWrite(segmentMetadata.getName()).join();
        context.storage.write(writeHandle, 0, new ByteArrayInputStream(segmentData), segmentData.length, TIMEOUT).join();
        segmentMetadata.setStorageLength(segmentData.length);

        // Add the contents of the segment to the read index using very small appends (same data as in Storage).
        int writtenLength = 0;
        int remainingLength = totalAppendLength;
        int lastCacheOffset = -1;
        while (remainingLength > 0) {
            int appendLength = rnd.nextInt(maxAppendLength) + 1;
            if (appendLength < remainingLength) {
                // Make another append.
                byte[] appendData = new byte[appendLength];
                System.arraycopy(segmentData, writtenLength, appendData, 0, appendLength);
                appendSingleWrite(segmentId, appendData, context);
                writtenLength += appendLength;
                remainingLength -= appendLength;
            } else {
                // This would be the last append. Don't add it, so force the read index to load it from Storage.
                lastCacheOffset = writtenLength;
                appendLength = remainingLength;
                writtenLength += appendLength;
                remainingLength = 0;
                segmentMetadata.setLength(writtenLength);
            }
        }

        // Check all the appended data.
        @Cleanup
        ReadResult readResult = context.readIndex.read(segmentId, 0, totalAppendLength, TIMEOUT);
        long expectedCurrentOffset = 0;
        boolean encounteredStorageRead = false;
        while (readResult.hasNext()) {
            ReadResultEntry entry = readResult.next();
            if (entry.getStreamSegmentOffset() < lastCacheOffset) {
                Assert.assertEquals("Expecting only a Cache entry before switch offset.", ReadResultEntryType.Cache, entry.getType());
            } else {
                Assert.assertEquals("Expecting only a Storage entry on or after switch offset.", ReadResultEntryType.Storage, entry.getType());
                entry.requestContent(TIMEOUT);
                entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                encounteredStorageRead = true;
            }

            // Check the entry contents.
            byte[] entryData = new byte[entry.getContent().join().getLength()];
            StreamHelpers.readAll(entry.getContent().join().getData(), entryData, 0, entryData.length);
            AssertExtensions.assertArrayEquals("Unexpected data read at offset " + expectedCurrentOffset, segmentData, (int) expectedCurrentOffset, entryData, 0, entryData.length);
            expectedCurrentOffset += entryData.length;

            // Check the entry length. Every result entry should have at least the min length, unless it was prematurely
            // cut short by the storage entry.
            if (expectedCurrentOffset < lastCacheOffset) {
                AssertExtensions.assertGreaterThanOrEqual("Expecting a ReadResultEntry of a minimum length for cache hit.", minReadLength, entryData.length);
            }
        }

        Assert.assertEquals("Not encountered any storage reads, even though one was forced.", lastCacheOffset > 0, encounteredStorageRead);
    }

    /**
     * Tests the readDirect() method on the ReadIndex.
     */
    @Test
    public void testReadDirect() throws Exception {
        final int randomAppendLength = 1024;

        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = new ArrayList<>();
        final long segmentId = createSegment(0, context);
        final UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        segmentIds.add(segmentId);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, 1, context);
        final long mergedTxId = transactionsBySegment.get(segmentId).get(0);

        // Add data to all segments.
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        transactionsBySegment.values().forEach(segmentIds::addAll);
        appendData(segmentIds, segmentContents, context);

        // Mark everything so far (minus a few bytes) as being written to storage.
        segmentMetadata.setStorageLength(segmentMetadata.getLength() - 100);

        // Now partially merge a second transaction
        final long mergedTxOffset = beginMergeTransaction(mergedTxId, segmentMetadata, segmentContents, context);

        // Add one more append after all of this.
        final long endOfMergedDataOffset = segmentMetadata.getLength();
        byte[] appendData = new byte[randomAppendLength];
        new Random(0).nextBytes(appendData);
        appendSingleWrite(segmentId, appendData, context);
        recordAppend(segmentId, appendData, segmentContents);

        // At this point, in our (parent) segment:
        // * [0 .. StorageLength): no reads allowed.
        // * [StorageLength .. mergedTxOffset): should be fully available.
        // * [mergedTxOffset .. endOfMergedDataOffset): no reads allowed
        // * [endOfMergedDataOffset .. Length): should be fully available.

        // Verify we are not allowed to read from the range which has already been committed to Storage (invalid arguments).
        for (AtomicLong offset = new AtomicLong(0); offset.get() < segmentMetadata.getStorageLength(); offset.incrementAndGet()) {
            AssertExtensions.assertThrows(
                    String.format("readDirect allowed reading from an illegal offset (%s).", offset),
                    () -> context.readIndex.readDirect(segmentId, offset.get(), 1),
                    ex -> ex instanceof IllegalArgumentException);
        }

        // Verify that any reads overlapping a merged transaction return null (that is, we cannot retrieve the requested data).
        for (long offset = mergedTxOffset - 1; offset < endOfMergedDataOffset; offset++) {
            InputStream resultStream = context.readIndex.readDirect(segmentId, offset, 2);
            Assert.assertNull("readDirect() returned data overlapping a partially merged transaction", resultStream);
        }

        // Verify that we can read from any other offset.
        final byte[] expectedData = segmentContents.get(segmentId).toByteArray();
        BiConsumer<Long, Long> verifyReadResult = (startOffset, endOffset) -> {
            int readLength = (int) (endOffset - startOffset);
            while (readLength > 0) {
                InputStream actualDataStream = context.readIndex.readDirect(segmentId, startOffset, readLength);
                Assert.assertNotNull(
                        String.format("Unexpected result when data is readily available for Offset = %s, Length = %s.", startOffset, readLength),
                        actualDataStream);

                byte[] actualData = new byte[readLength];
                try {
                    int bytesCopied = StreamHelpers.readAll(actualDataStream, actualData, 0, readLength);
                    Assert.assertEquals(
                            String.format("Unexpected number of bytes read for Offset = %s, Length = %s (pre-partial-merge).", startOffset, readLength),
                            readLength, bytesCopied);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex); // Technically not possible.
                }

                AssertExtensions.assertArrayEquals("Unexpected data read from the segment at offset " + startOffset,
                        expectedData, startOffset.intValue(), actualData, 0, actualData.length);

                // Setup the read for the next test (where we read 1 less byte than now).
                readLength--;
                if (readLength % 2 == 0) {
                    // For every 2 bytes of decreased read length, increase the start offset by 1. This allows for a greater
                    // number of combinations to be tested.
                    startOffset++;
                }
            }
        };

        // Verify that we can read the cached data just after the StorageLength but before the merged transaction.
        verifyReadResult.accept(segmentMetadata.getStorageLength(), mergedTxOffset);

        // Verify that we can read the cached data just after the merged transaction but before the end of the segment.
        verifyReadResult.accept(endOfMergedDataOffset, segmentMetadata.getLength());
    }

    /**
     * Tests a scenario of truncation that does not happen concurrently with reading (segments are pre-truncated).
     */
    @Test
    public void testTruncate() throws Exception {
        // We use a custom ReadIndexConfig that allows more than one generation. This helps us verify that truncated entries
        // are actually evicted.
        val config = ReadIndexConfig.builder()
                                    .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, DEFAULT_CONFIG.getMemoryReadMinLength())
                                    .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, DEFAULT_CONFIG.getStorageReadAlignment())
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_SIZE, Long.MAX_VALUE)
                                    .with(ReadIndexConfig.CACHE_POLICY_MAX_TIME, 1000000)
                                    .with(ReadIndexConfig.CACHE_POLICY_GENERATION_TIME, 10000)
                                    .build();

        @Cleanup
        TestContext context = new TestContext(config, config.getCachePolicy());
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendData(segmentIds, segmentContents, context);

        // Truncate all segments at their mid-points.
        for (int i = 0; i < segmentIds.size(); i++) {
            val sm = context.metadata.getStreamSegmentMetadata(segmentIds.get(i));
            sm.setStartOffset(sm.getLength() / 2);
            if (i % 2 == 0) {
                sm.setStorageLength(sm.getStartOffset());
            } else {
                sm.setStorageLength(sm.getStartOffset() / 2);
            }
        }

        // Check all the appended data. This includes verifying access to already truncated offsets.
        checkReadIndex("PostTruncate", segmentContents, context);
        checkReadIndexDirect(segmentContents, context);

        // Verify that truncated data is eligible for eviction, by checking that at least one Cache Entry is being removed.
        for (long segmentId : segmentIds) {
            val sm = context.metadata.getStreamSegmentMetadata(segmentId);
            sm.setStorageLength(sm.getLength()); // We need to set this in order to verify cache evictions.
        }

        HashSet<CacheKey> removedKeys = new HashSet<>();
        context.cacheFactory.cache.removeCallback = removedKeys::add;
        context.cacheManager.applyCachePolicy();
        AssertExtensions.assertGreaterThan("Expected at least one cache entry to be removed.", 0, removedKeys.size());
    }

    /**
     * Tests a scenario of truncation that happens concurrently with reading (segment is truncated while reading).
     */
    @Test
    public void testTruncateConcurrently() {
        @Cleanup
        TestContext context = new TestContext();
        List<Long> segmentIds = createSegments(context).subList(0, 1);
        long segmentId = segmentIds.get(0);
        ByteArrayOutputStream segmentContents = new ByteArrayOutputStream();
        appendData(segmentIds, Collections.singletonMap(segmentId, segmentContents), context);

        // Begin a read result.
        UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
        @Cleanup
        ReadResult rr = context.readIndex.read(segmentId, 0, (int) sm.getLength(), TIMEOUT);
        ReadResultEntry firstEntry = rr.next();
        firstEntry.requestContent(TIMEOUT);
        int firstEntryLength = firstEntry.getContent().join().getLength();
        AssertExtensions.assertLessThan("Unexpected length of the first read result entry.", sm.getLength(), firstEntryLength);

        // Truncate the segment just after the end of the first returned read result.
        sm.setStartOffset(firstEntryLength + 1);
        ReadResultEntry secondEntry = rr.next();
        Assert.assertTrue("Unexpected ReadResultEntryType.isTerminal of truncated result entry.", secondEntry.getType().isTerminal());
        Assert.assertEquals("Unexpected ReadResultEntryType of truncated result entry.", ReadResultEntryType.Truncated, secondEntry.getType());
        AssertExtensions.assertThrows(
                "Expecting getContent() to return a failed CompletableFuture.",
                secondEntry::getContent,
                ex -> ex instanceof StreamSegmentTruncatedException);
        Assert.assertFalse("Unexpected result from hasNext after processing terminal result entry.", rr.hasNext());
    }

    /**
     * Tests the merging of Transactions into their parent StreamSegments.
     */
    @Test
    public void testMerge() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Put all segment names into one list, for easier appends (but still keep the original lists at hand - we'll need them later).
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
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();
        HashMap<Long, ByteArrayOutputStream> readContents = new HashMap<>();
        HashSet<Long> segmentsToSeal = new HashSet<>();
        ArrayList<AsyncReadResultProcessor> readProcessors = new ArrayList<>();
        HashMap<Long, TestReadResultHandler> entryHandlers = new HashMap<>();

        // 1. Put all segment names into one list, for easier appends (but still keep the original lists at hand - we'll need them later).
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
            TestReadResultHandler entryHandler = new TestReadResultHandler(readContentsStream, TIMEOUT);
            entryHandlers.put(segmentId, entryHandler);
            readProcessors.add(AsyncReadResultProcessor.process(readResult, entryHandler, executorService()));
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

                // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getLength();
                segmentMetadata.setLength(offset + data.length);
                context.readIndex.append(segmentId, offset, data);
                recordAppend(segmentId, data, segmentContents);
                triggerFutureReadsCallback.run();
            }
        }

        // 6. Seal those segments that we need to seal.
        segmentsToSeal.forEach(segmentId -> context.metadata.getStreamSegmentMetadata(segmentId).markSealed());

        // Trigger future reads on all segments we know about; some may not have had a trigger in a while (see callback above).
        context.readIndex.triggerFutureReads(segmentIds);

        // Now wait for all the reads to complete, and verify their results against the expected output.
        Futures.allOf(entryHandlers.values().stream().map(TestReadResultHandler::getCompleted).collect(Collectors.toList())).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        readProcessors.forEach(AsyncReadResultProcessor::close);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<Long, TestReadResultHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().getError().get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail("Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue().getError().get());
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
            Assert.assertEquals("Unexpected read length for segment " + expectedData.length, expectedLength, actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + segmentId, expectedData, 0, actualData, 0, actualData.length);
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
        @Cleanup
        TestContext context = new TestContext();

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
        long segmentOffset = segmentMetadata.getLength();
        segmentMetadata.setLength(segmentOffset + appendData.length);
        context.readIndex.append(segmentId, segmentOffset, appendData);

        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        long transactionOffset = transactionMetadata.getLength();
        transactionMetadata.setLength(transactionOffset + appendData.length);
        context.readIndex.append(transactionId, transactionOffset, appendData);

        // 1. Appends at wrong offsets.
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with an offset beyond the Segment's DurableLogOffset.",
                () -> context.readIndex.append(segmentId, Integer.MAX_VALUE, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid offset.",
                () -> context.readIndex.append(segmentId, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        // 2. Appends or reads with wrong SegmentIds
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.append(transactionId + 1, 0, "foo".getBytes()),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "read did not throw the correct exception when provided with invalid SegmentId.",
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
                "completeMerge did not throw the correct exception when called on a Transaction that did not have beginMerge called for.",
                () -> context.readIndex.completeMerge(segmentId, transactionId),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "beginMerge did not throw the correct exception when called on a Transaction that was not sealed.",
                () -> context.readIndex.beginMerge(segmentId, 0, transactionId),
                ex -> ex instanceof IllegalArgumentException);

        transactionMetadata.markSealed();
        long mergeOffset = segmentMetadata.getLength();
        segmentMetadata.setLength(mergeOffset + transactionMetadata.getLength());
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
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Check all the appended data.
        checkReadIndex("StorageReads", segmentContents, context);

        // Pretty brutal, but will do the job for this test: delete all segments from the storage. This way, if something
        // wasn't cached properly in the last read, the ReadIndex would delegate to Storage, which would fail.
        for (long segmentId : segmentIds) {
            val handle = context.storage.openWrite(context.metadata.getStreamSegmentMetadata(segmentId).getName()).join();
            context.storage.delete(handle, TIMEOUT).join();
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
        sm.setLength(1024 * 1024);

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
        val handle = context.storage.openWrite(sm.getName()).join();
        context.storage.delete(handle, TIMEOUT).join();
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
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Merge all Transaction names into the segment list. For this test, we do not care what kind of Segment we have.
        transactionsBySegment.values().forEach(segmentIds::addAll);

        // Create all the segments in storage.
        createSegmentsInStorage(context);

        // Append data (in storage).
        appendDataInStorage(context, segmentContents);

        // Append data (in read index - this is at offsets after the data we appended in Storage).
        appendData(segmentIds, segmentContents, context);

        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }

    /**
     * Tests the ability to evict entries from the ReadIndex under various conditions:
     * * If an entry is aged out
     * * If an entry is pushed out because of cache space pressure.
     * <p>
     * This also verifies that certain entries, such as RedirectReadIndexEntries and entries after the Storage Offset are
     * not removed.
     * <p>
     * The way this test goes is as follows (it's pretty subtle, because there aren't many ways to hook into the ReadIndex and see what it's doing)
     * 1. It creates a bunch of segments, and populates them in storage (each) up to offset N/2-1 (this is called pre-storage)
     * 2. It populates the ReadIndex for each of those segments from offset N/2 to offset N-1 (this is called post-storage)
     * 3. It loads all the data from Storage into the ReadIndex, in entries of size equal to those already loaded in step #2.
     * 3a. At this point, all the entries added in step #2 have Generations 0..A/4-1, and step #3 have generations A/4..A-1
     * 4. Append more data at the end. This forces the generation to increase to 1.25A.
     * 4a. Nothing should be evicted from the cache now, since the earliest items are all post-storage.
     * 5. We 'touch' (read) the first 1/3 of pre-storage entries (offsets 0..N/4).
     * 5a. At this point, those entries (offsets 0..N/6) will have the newest generations (1.25A..1.5A)
     * 6. We append more data (equivalent to the data we touched)
     * 6a. Nothing should be evicted, since those generations that were just eligible for removal were touched and bumped up.
     * 7. We forcefully increase the current generation by 1 (without touching the ReadIndex)
     * 7a. At this point, we expect all the pre-storage items, except the touched ones, to be evicted. This is generations 0.25A-0.75A.
     * 8. Update the metadata and indicate that all the post-storage entries are now pre-storage and bump the generation by 0.75A.
     * 8a. At this point, we expect all former post-storage items and pre-storage items to be evicted (in this order).
     * <p>
     * The final order of eviction (in terms of offsets, for each segment), is:
     * * 0.25N-0.75N, 0.75N..N, N..1.25N, 0..0.25N, 1.25N..1.5N (remember that we added quite a bunch of items after the initial run).
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testCacheEviction() throws Exception {
        // Create a CachePolicy with a set number of generations and a known max size.
        // Each generation contains exactly one entry, so the number of generations is also the number of entries.
        final int appendSize = 100;
        final int entriesPerSegment = 100; // This also doubles as number of generations (each generation, we add one append for each segment).
        final int cacheMaxSize = SEGMENT_COUNT * entriesPerSegment * appendSize;
        final int postStorageEntryCount = entriesPerSegment / 4; // 25% of the entries are beyond the StorageOffset
        final int preStorageEntryCount = entriesPerSegment - postStorageEntryCount; // 75% of the entries are before the StorageOffset.
        CachePolicy cachePolicy = new CachePolicy(cacheMaxSize, Duration.ofMillis(1000 * 2 * entriesPerSegment), Duration.ofMillis(1000));

        // To properly test this, we want predictable storage reads.
        ReadIndexConfig config = ConfigHelpers
                .withInfiniteCachePolicy(ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, appendSize))
                .build();

        ArrayList<CacheKey> removedKeys = new ArrayList<>();
        @Cleanup
        TestContext context = new TestContext(config, cachePolicy);
        context.cacheFactory.cache.removeCallback = removedKeys::add; // Record every cache removal.

        // Create the segments (metadata + storage).
        ArrayList<Long> segmentIds = createSegments(context);
        createSegmentsInStorage(context);

        // Populate the Storage with appropriate data.
        byte[] preStorageData = new byte[preStorageEntryCount * appendSize];
        for (long segmentId : segmentIds) {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            val handle = context.storage.openWrite(sm.getName()).join();
            context.storage.write(handle, 0, new ByteArrayInputStream(preStorageData), preStorageData.length, TIMEOUT).join();
            sm.setStorageLength(preStorageData.length);
            sm.setLength(preStorageData.length);
        }

        // Callback that appends one entry at the end of the given segment id.
        Consumer<Long> appendOneEntry = segmentId -> {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            byte[] data = new byte[appendSize];
            long offset = sm.getLength();
            sm.setLength(offset + data.length);
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
                @Cleanup
                ReadResult result = context.readIndex.read(segmentId, offset, appendSize, TIMEOUT);
                ReadResultEntry resultEntry = result.next();
                Assert.assertEquals("Unexpected type of ReadResultEntry when trying to load up data into the ReadIndex Cache.", ReadResultEntryType.Storage, resultEntry.getType());
                resultEntry.requestContent(TIMEOUT);
                ReadResultEntryContents contents = resultEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertFalse("Not expecting more data to be available for reading.", result.hasNext());
                Assert.assertEquals("Unexpected ReadResultEntry length when trying to load up data into the ReadIndex Cache.", appendSize, contents.getLength());
            }

            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (cache is not full).", 0, removedKeys.size());

        // Append more data (equivalent to all post-storage entries), and verify that NO entries are being evicted (we cannot evict post-storage entries).
        for (int i = 0; i < postStorageEntryCount; i++) {
            segmentIds.forEach(appendOneEntry);
            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (only eligible entries were post-storage).", 0, removedKeys.size());

        // 'Touch' the first few entries read from storage. This should move them to the back of the queue (they won't be the first ones to be evicted).
        int touchCount = preStorageEntryCount / 3;
        for (int i = 0; i < touchCount; i++) {
            long offset = i * appendSize;
            for (long segmentId : segmentIds) {
                @Cleanup
                ReadResult result = context.readIndex.read(segmentId, offset, appendSize, TIMEOUT);
                ReadResultEntry resultEntry = result.next();
                Assert.assertEquals("Unexpected type of ReadResultEntry when trying to load up data into the ReadIndex Cache.", ReadResultEntryType.Cache, resultEntry.getType());
            }
        }

        // Append more data (equivalent to the amount of data we 'touched'), and verify that the entries we just touched are not being removed..
        for (int i = 0; i < touchCount; i++) {
            segmentIds.forEach(appendOneEntry);
            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (we touched old entries and they now have the newest generation).", 0, removedKeys.size());

        // Increment the generations so that we are caught up to just before the generation where the "touched" items now live.
        context.cacheManager.applyCachePolicy();

        // We expect all but the 'touchCount' pre-Storage entries to be removed.
        int expectedRemovalCount = (preStorageEntryCount - touchCount) * SEGMENT_COUNT;
        Assert.assertEquals("Unexpected number of removed entries after having forced out all pre-storage entries.", expectedRemovalCount, removedKeys.size());

        // Now update the metadata and indicate that all the post-storage data has been moved to storage.
        segmentIds.forEach(segmentId -> {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            sm.setStorageLength(sm.getLength());
        });

        // We add one artificial entry, which we'll be touching forever and ever; this forces the CacheManager to
        // update its current generation every time. We will be ignoring this entry for our test.
        SegmentMetadata readSegment = context.metadata.getStreamSegmentMetadata(segmentIds.get(0));
        appendOneEntry.accept(readSegment.getId());

        // Now evict everything (whether by size of by aging out).
        for (int i = 0; i < cachePolicy.getMaxGenerations(); i++) {
            @Cleanup
            ReadResult result = context.readIndex.read(readSegment.getId(), readSegment.getLength() - appendSize, appendSize, TIMEOUT);
            result.next();
            context.cacheManager.applyCachePolicy();
        }

        int expectedRemovalCountPerSegment = entriesPerSegment + touchCount + postStorageEntryCount;
        int expectedTotalRemovalCount = SEGMENT_COUNT * expectedRemovalCountPerSegment;
        Assert.assertEquals("Unexpected number of removed entries after having forced out all the entries.", expectedTotalRemovalCount, removedKeys.size());

        // Finally, verify that the evicted items are in the correct order (for each segment). See this test's description for details.
        for (long segmentId : segmentIds) {
            List<CacheKey> segmentRemovedKeys = removedKeys.stream().filter(key -> key.getStreamSegmentId() == segmentId).collect(Collectors.toList());
            Assert.assertEquals("Unexpected number of removed entries for segment " + segmentId, expectedRemovalCountPerSegment, segmentRemovedKeys.size());

            // The correct order of eviction (N=entriesPerSegment) is: 0.25N-0.75N, 0.75N..N, N..1.25N, 0..0.25N, 1.25N..1.5N.
            // This is equivalent to the following tests
            // 0.25N-1.25N
            checkOffsets(segmentRemovedKeys, segmentId, 0, entriesPerSegment, entriesPerSegment * appendSize / 4, appendSize);

            // 0..0.25N
            checkOffsets(segmentRemovedKeys, segmentId, entriesPerSegment, entriesPerSegment / 4, 0, appendSize);

            //1.25N..1.5N
            checkOffsets(segmentRemovedKeys, segmentId, entriesPerSegment + entriesPerSegment / 4, entriesPerSegment / 4, (int) (entriesPerSegment * appendSize * 1.25), appendSize);
        }
    }

    // region Scenario-based tests

    /**
     * Tests the following Scenario, where the ReadIndex would either read from a bad offset or fail with an invalid offset
     * when reading in certain conditions:
     * * A segment has a transaction, which has N bytes written to it.
     * * The transaction is merged into its parent segment at offset M > N.
     * * At least one byte of the transaction is evicted from the cache
     * * A read is issued to the parent segment for that byte that was evicted
     * * The ReadIndex is supposed to issue a Storage Read with an offset inside the transaction range (so translate
     * from the parent's offset to the transaction's offset). However, after the read, it is supposed to look like the
     * data was read from the parent segment, so it should not expose the adjusted offset at all.
     * <p>
     * This very specific unit test is a result of a regression found during testing.
     */
    @Test
    public void testStorageReadTransactionNoCache() throws Exception {
        CachePolicy cachePolicy = new CachePolicy(1, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);

        // Create parent segment and one transaction
        long parentId = createSegment(0, context);
        UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
        long transactionId = createTransaction(parentMetadata, 1, context);
        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        createSegmentsInStorage(context);
        ByteArrayOutputStream writtenStream = new ByteArrayOutputStream();

        // Write something to the transaction, and make sure it also makes its way to Storage.
        byte[] transactionWriteData = getAppendData(transactionMetadata.getName(), transactionId, 0, 0);
        appendSingleWrite(transactionId, transactionWriteData, context);
        val handle = context.storage.openWrite(transactionMetadata.getName()).join();
        context.storage.write(handle, 0, new ByteArrayInputStream(transactionWriteData), transactionWriteData.length, TIMEOUT).join();
        transactionMetadata.setStorageLength(transactionMetadata.getLength());

        // Write some data to the parent, and make sure it is more than what we write to the transaction (hence the 10).
        for (int i = 0; i < 10; i++) {
            byte[] parentWriteData = getAppendData(parentMetadata.getName(), parentId, i, i);
            appendSingleWrite(parentId, parentWriteData, context);
            writtenStream.write(parentWriteData);
        }

        // Seal & Begin-merge the transaction (do not seal in storage).
        transactionMetadata.markSealed();
        long mergeOffset = parentMetadata.getLength();
        parentMetadata.setLength(mergeOffset + transactionMetadata.getLength());
        context.readIndex.beginMerge(parentId, mergeOffset, transactionId);
        transactionMetadata.markMerged();
        writtenStream.write(transactionWriteData);

        // Clear the cache.
        context.cacheManager.applyCachePolicy();

        // Issue read from the parent.
        ReadResult rr = context.readIndex.read(parentId, mergeOffset, transactionWriteData.length, TIMEOUT);
        Assert.assertTrue("Parent Segment read indicates no data available.", rr.hasNext());
        ByteArrayOutputStream readStream = new ByteArrayOutputStream();
        long expectedOffset = mergeOffset;
        while (rr.hasNext()) {
            ReadResultEntry entry = rr.next();
            Assert.assertEquals("Unexpected offset for read result entry.", expectedOffset, entry.getStreamSegmentOffset());
            Assert.assertEquals("Served read result entry is not from storage.", ReadResultEntryType.Storage, entry.getType());

            // Request contents and store for later use.
            entry.requestContent(TIMEOUT);
            ReadResultEntryContents contents = entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            byte[] readBuffer = new byte[contents.getLength()];
            StreamHelpers.readAll(contents.getData(), readBuffer, 0, readBuffer.length);
            readStream.write(readBuffer);
            expectedOffset += contents.getLength();
        }

        byte[] readData = readStream.toByteArray();
        Assert.assertArrayEquals("Unexpected data read back.", transactionWriteData, readData);
    }

    /**
     * Tests the following scenario, where the Read Index has a read from a portion in a parent segment where a transaction
     * was just merged (fully in storage), but the read request might result in either an ObjectClosedException or
     * StreamSegmentNotExistsException:
     * * A Parent Segment has a Transaction with some data in it, and at least 1 byte of data not in cache.
     * * The Transaction is begin-merged in the parent (Tier 1 only).
     * * A Read Request is issued to the Parent for the range of data from the Transaction, which includes the 1 byte not in cache.
     * * The Transaction is fully merged (Tier 2).
     * * The Read Request is invoked and its content requested. This should correctly retrieve the data from the Parent
     * Segment in Storage, and not attempt to access the now-defunct Transaction segment.
     */
    @Test
    public void testConcurrentReadTransactionStorageMerge() throws Exception {
        CachePolicy cachePolicy = new CachePolicy(1, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);

        // Create parent segment and one transaction
        long parentId = createSegment(0, context);
        UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
        long transactionId = createTransaction(parentMetadata, 1, context);
        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        createSegmentsInStorage(context);

        // Write something to the transaction, and make sure it also makes its way to Storage.
        byte[] writeData = getAppendData(transactionMetadata.getName(), transactionId, 0, 0);
        appendSingleWrite(transactionId, writeData, context);
        val transactionWriteHandle = context.storage.openWrite(transactionMetadata.getName()).join();
        context.storage.write(transactionWriteHandle, 0, new ByteArrayInputStream(writeData), writeData.length, TIMEOUT).join();
        transactionMetadata.setStorageLength(transactionMetadata.getLength());

        // Seal & Begin-merge the transaction (do not seal in storage).
        transactionMetadata.markSealed();
        parentMetadata.setLength(transactionMetadata.getLength());
        context.readIndex.beginMerge(parentId, 0, transactionId);
        transactionMetadata.markMerged();

        // Clear the cache.
        context.cacheManager.applyCachePolicy();

        // Issue read from the parent and fetch the first entry (there should only be one).
        ReadResult rr = context.readIndex.read(parentId, 0, writeData.length, TIMEOUT);
        Assert.assertTrue("Parent Segment read indicates no data available.", rr.hasNext());
        ReadResultEntry entry = rr.next();
        Assert.assertEquals("Unexpected offset for read result entry.", 0, entry.getStreamSegmentOffset());
        Assert.assertEquals("Served read result entry is not from storage.", ReadResultEntryType.Storage, entry.getType());

        // Merge the transaction in storage & complete-merge it.
        transactionMetadata.markSealed();
        transactionMetadata.markSealedInStorage();
        transactionMetadata.markDeleted();
        context.storage.seal(transactionWriteHandle, TIMEOUT).join();
        val parentWriteHandle = context.storage.openWrite(parentMetadata.getName()).join();
        context.storage.concat(parentWriteHandle, 0, transactionWriteHandle.getSegmentName(), TIMEOUT).join();
        parentMetadata.setStorageLength(parentMetadata.getLength());

        context.readIndex.completeMerge(parentId, transactionId);

        // Attempt to extract data from the read.
        entry.requestContent(TIMEOUT);
        ReadResultEntryContents contents = entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        byte[] readData = new byte[contents.getLength()];
        StreamHelpers.readAll(contents.getData(), readData, 0, readData.length);

        Assert.assertArrayEquals("Unexpected data read from parent segment.", writeData, readData);
    }

    //endregion

    //region Helpers

    private void checkOffsets(List<CacheKey> removedKeys, long segmentId, int startIndex, int count, int startOffset, int stepIncrease) {
        int expectedStartOffset = startOffset;
        for (int i = 0; i < count; i++) {
            int listIndex = startIndex + i;
            CacheKey currentKey = removedKeys.get(startIndex + i);
            Assert.assertEquals(
                    String.format("Unexpected CacheKey.SegmentOffset at index %d for SegmentId %d.", listIndex, segmentId),
                    expectedStartOffset,
                    currentKey.getOffset());
            expectedStartOffset += stepIncrease;
        }
    }

    private void createSegmentsInStorage(TestContext context) {
        for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
            SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            context.storage.create(sm.getName(), TIMEOUT).join();
        }
    }

    private void appendData(Collection<Long> segmentIds, Map<Long, ByteArrayOutputStream> segmentContents, TestContext context) {
        appendData(segmentIds, segmentContents, context, null);
    }

    private void appendData(Collection<Long> segmentIds, Map<Long, ByteArrayOutputStream> segmentContents, TestContext context, Runnable callback) {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(segmentMetadata.getName(), segmentId, i, writeId);
                writeId++;

                appendSingleWrite(segmentId, data, context);
                recordAppend(segmentId, data, segmentContents);
                if (callback != null) {
                    callback.run();
                }
            }
        }
    }

    private void appendSingleWrite(long segmentId, byte[] data, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);

        // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
        long offset = segmentMetadata.getLength();
        segmentMetadata.setLength(offset + data.length);
        context.readIndex.append(segmentId, offset, data);
    }

    private void appendDataInStorage(TestContext context, HashMap<Long, ByteArrayOutputStream> segmentContents) {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
                UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
                byte[] data = getAppendData(sm.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
                long offset = context.storage.getStreamSegmentInfo(sm.getName(), TIMEOUT).join().getLength();
                val handle = context.storage.openWrite(sm.getName()).join();
                context.storage.write(handle, offset, new ByteArrayInputStream(data), data.length, TIMEOUT).join();

                // Update metadata appropriately.
                sm.setStorageLength(offset + data.length);
                if (sm.getStorageLength() > sm.getLength()) {
                    sm.setLength(sm.getStorageLength());
                }

                recordAppend(segmentId, data, segmentContents);
            }
        }
    }

    private byte[] getAppendData(String segmentName, long segmentId, int segmentAppendSeq, int writeId) {
        return String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d", segmentName, segmentId, segmentAppendSeq, writeId).getBytes();
    }

    private void completeMergeTransactions(HashMap<Long, ArrayList<Long>> transactionsBySegment, TestContext context) {
        for (Map.Entry<Long, ArrayList<Long>> e : transactionsBySegment.entrySet()) {
            long parentId = e.getKey();
            for (long transactionId : e.getValue()) {
                UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
                transactionMetadata.markDeleted();
                context.readIndex.completeMerge(parentId, transactionId);
            }
        }
    }

    private void beginMergeTransactions(HashMap<Long, ArrayList<Long>> transactionsBySegment, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (Map.Entry<Long, ArrayList<Long>> e : transactionsBySegment.entrySet()) {
            UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(e.getKey());
            for (long transactionId : e.getValue()) {
                beginMergeTransaction(transactionId, parentMetadata, segmentContents, context);
            }
        }
    }

    private long beginMergeTransaction(long transactionId, UpdateableSegmentMetadata parentMetadata, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);

        // Transaction must be sealed first.
        transactionMetadata.markSealed();

        // Update parent length.
        long mergeOffset = parentMetadata.getLength();
        parentMetadata.setLength(mergeOffset + transactionMetadata.getLength());

        // Do the ReadIndex merge.
        context.readIndex.beginMerge(parentMetadata.getId(), mergeOffset, transactionId);

        // Update the metadata.
        transactionMetadata.markMerged();

        // Update parent contents.
        segmentContents.get(parentMetadata.getId()).write(segmentContents.get(transactionId).toByteArray());
        segmentContents.remove(transactionId);
        return mergeOffset;
    }

    private void checkReadIndex(String testId, HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (long segmentId : segmentContents.keySet()) {
            long startOffset = context.metadata.getStreamSegmentMetadata(segmentId).getStartOffset();
            long segmentLength = context.metadata.getStreamSegmentMetadata(segmentId).getLength();
            byte[] expectedData = segmentContents.get(segmentId).toByteArray();

            if (startOffset > 0) {
                @Cleanup
                ReadResult truncatedResult = context.readIndex.read(segmentId, 0, 1, TIMEOUT);
                val first = truncatedResult.next();
                Assert.assertEquals("Read request for a truncated offset did not start with a Truncated ReadResultEntryType.",
                        ReadResultEntryType.Truncated, first.getType());
                AssertExtensions.assertThrows(
                        "Truncate ReadResultEntryType did not throw when getContent() was invoked.",
                        () -> {
                            first.requestContent(TIMEOUT);
                            return first.getContent();
                        },
                        ex -> ex instanceof StreamSegmentTruncatedException);
            }

            long expectedCurrentOffset = startOffset;
            @Cleanup
            ReadResult readResult = context.readIndex.read(segmentId, expectedCurrentOffset, (int) (segmentLength - expectedCurrentOffset), TIMEOUT);
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

    private void checkReadIndexDirect(HashMap<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        for (long segmentId : segmentContents.keySet()) {
            val sm = context.metadata.getStreamSegmentMetadata(segmentId);
            long segmentLength = sm.getLength();
            long startOffset = Math.min(sm.getStartOffset(), sm.getStorageLength());
            byte[] expectedData = segmentContents.get(segmentId).toByteArray();

            if (startOffset > 0) {
                AssertExtensions.assertThrows(
                        "Read request for a truncated offset was not rejected.",
                        () -> context.readIndex.readDirect(segmentId, 0, 1),
                        ex -> ex instanceof IllegalArgumentException);
            }

            int readLength = (int) (segmentLength - startOffset);
            InputStream readData = context.readIndex.readDirect(segmentId, startOffset, readLength);
            byte[] actualData = StreamHelpers.readAll(readData, readLength);
            AssertExtensions.assertArrayEquals("Unexpected data read.", expectedData, (int) startOffset, actualData, 0, actualData.length);
        }
    }

    private <T> void recordAppend(T segmentIdentifier, byte[] data, Map<T, ByteArrayOutputStream> segmentContents) {
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
            segmentIds.add(createSegment(i, context));
        }

        return segmentIds;
    }

    private long createSegment(int id, TestContext context) {
        String name = getSegmentName(id);
        context.metadata.mapStreamSegmentId(name, id);
        initializeSegment(id, context);
        return id;
    }

    private HashMap<Long, ArrayList<Long>> createTransactions(Collection<Long> segmentIds, TestContext context) {
        return createTransactions(segmentIds, TRANSACTIONS_PER_SEGMENT, context);
    }

    private HashMap<Long, ArrayList<Long>> createTransactions(Collection<Long> segmentIds, int transactionsPerSegment, TestContext context) {
        // Create the Transactions.
        HashMap<Long, ArrayList<Long>> transactions = new HashMap<>();
        long transactionId = Integer.MAX_VALUE;
        for (long parentId : segmentIds) {
            ArrayList<Long> segmentTransactions = new ArrayList<>();
            transactions.put(parentId, segmentTransactions);
            SegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);

            for (int i = 0; i < transactionsPerSegment; i++) {
                segmentTransactions.add(createTransaction(parentMetadata, transactionId, context));
                transactionId++;
            }
        }

        return transactions;
    }

    private long createTransaction(SegmentMetadata parentMetadata, long transactionId, TestContext context) {
        String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(parentMetadata.getName(), UUID.randomUUID());
        context.metadata.mapStreamSegmentId(transactionName, transactionId, parentMetadata.getId());
        initializeSegment(transactionId, context);
        return transactionId;
    }

    private String getSegmentName(int id) {
        return "Segment_" + id;
    }

    private void initializeSegment(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setLength(0);
        metadata.setStorageLength(0);
    }

    //endregion

    //region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final ContainerReadIndex readIndex;
        final TestCacheManager cacheManager;
        final TestCacheFactory cacheFactory;
        final Storage storage;

        TestContext() {
            this(DEFAULT_CONFIG, DEFAULT_CONFIG.getCachePolicy());
        }

        TestContext(ReadIndexConfig readIndexConfig, CachePolicy cachePolicy) {
            this.cacheFactory = new TestCacheFactory();
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            this.storage = InMemoryStorageFactory.newStorage(executorService());
            this.storage.initialize(1);
            this.cacheManager = new TestCacheManager(cachePolicy, executorService());
            this.readIndex = new ContainerReadIndex(readIndexConfig, this.metadata, this.cacheFactory, this.storage, this.cacheManager, executorService());
        }

        @Override
        public void close() {
            this.readIndex.close();
            this.cacheFactory.close();
            this.storage.close();
            this.cacheManager.close();
        }
    }

    //endregion

    //region TestCache

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

    private static class TestCacheFactory implements CacheFactory {
        final TestCache cache = new TestCache("Test");

        @Override
        public Cache getCache(String id) {
            return this.cache;
        }

        @Override
        public void close() {
            this.cache.close();
        }
    }

    //endregion
}
