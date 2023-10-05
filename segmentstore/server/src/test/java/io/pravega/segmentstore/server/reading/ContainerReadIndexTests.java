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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.ReusableLatch;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.CachePolicy;
import io.pravega.segmentstore.server.EvictableMetadata;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.TestCacheManager;
import io.pravega.segmentstore.server.TestStorage;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import io.pravega.segmentstore.storage.cache.CacheState;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Unit tests for ContainerReadIndex class.
 */
public class ContainerReadIndexTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_COUNT = 10;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 1000;
    private static final int CONTAINER_ID = 123;

    private static final ReadIndexConfig DEFAULT_CONFIG = ReadIndexConfig
            .builder()
            .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 0) // Default: Off (we have a special test for this).
            .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024)
            .build();
    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration SHORT_TIMEOUT = Duration.ofMillis(20);

    @Rule
    public Timeout globalTimeout = Timeout.seconds(2 * TIMEOUT.getSeconds());

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
     * Tests the basic append-read functionality of the ContainerReadIndex using appends larger than the maximum allowed
     * by the Cache Storage.
     */
    @Test
    public void testLargeAppends() throws Exception {
        final int maxEntryLength = 64 * 1024;
        final int appendCount = 10;
        final Random rnd = new Random(0);
        @Cleanup
        TestContext context = new TestContext();
        context.cacheStorage.maxEntryLength = maxEntryLength;
        long segmentId = createSegments(context).get(0);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Add a bunch of writes.
        for (int i = 0; i < appendCount; i++) {
            val data = new ByteArraySegment(new byte[maxEntryLength + i * 10240]);
            rnd.nextBytes(data.array());

            appendSingleWrite(segmentId, data, context);
            recordAppend(segmentId, data, segmentContents);
        }
        // Check all the appended data.
        checkReadIndex("PostAppend", segmentContents, context);
    }
    
    @Test
    public void testModifyUnderlyingBuffer() {
        CachePolicy cachePolicy = new CachePolicy(100 * 1024 * 1024, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        val cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
        val metadata = new MetadataBuilder(CONTAINER_ID).build();
        @Cleanup
        val storage = new TestStorage(new InMemoryStorage(), executorService());
        storage.initialize(1);
        @Cleanup
        val cacheManager = new CacheManager(cachePolicy, cacheStorage, executorService());
        @Cleanup
        val readIndex = new ContainerReadIndex(DEFAULT_CONFIG, metadata, storage, cacheManager, executorService());
        int a = cacheManager.getCacheStorage().getBlockAlignment();
        int numWriters = 25;
        ReusableLatch latch = new ReusableLatch();
        
        @Cleanup("shutdown")
        ExecutorService executorService = ExecutorServiceHelpers.newScheduledThreadPool(numWriters, "test");

        Consumer<Long> writer = (Long segmentId) -> {
            String name = getSegmentName(segmentId);
            metadata.mapStreamSegmentId(name, segmentId);
            val segmentMetadata = metadata.getStreamSegmentMetadata(segmentId);
            segmentMetadata.setLength(0);
            segmentMetadata.setStorageLength(0);

            int dataLength = 16;
            long segmentLength = 0;
            HashHelper hasher = HashHelper.seededWith("Seed");
            for (int writes = 0; writes < 1000000; writes++) {
                byte[] data = hasher.numToBytes(writes);
                val append = new ByteArraySegment(data);
                segmentLength += data.length;
                segmentMetadata.setLength(segmentLength);
                try {
                    readIndex.append(segmentId, segmentLength - data.length, append);
                } catch (StreamSegmentNotExistsException e) {
                    Exceptions.sneakyThrow(e);
                }
            }
            latch.awaitUninterruptibly();
            for (int reads = 0; reads < 100000; reads++) {
                int randNum = Integer.hashCode(reads);
                int readOffset = (randNum % 1000000) * dataLength;
                try {
                    ReadResult readResult = readIndex.read((randNum / 1000000) % numWriters, readOffset, dataLength, Duration.ofSeconds(10));
                    byte[] readData = new byte[dataLength];
                    readResult.readRemaining(readData, Duration.ofSeconds(10));
                    Preconditions.checkState(Arrays.equals(hasher.numToBytes(readOffset / dataLength), readData));
                } catch (StreamSegmentNotExistsException e) {
                    Exceptions.sneakyThrow(e);
                }
            }
        };
        List<CompletableFuture<Void>> writerFutures = new ArrayList<>();
        for (int i = 0; i < numWriters; i++) {
            long segmentNumber = i;
            writerFutures.add(CompletableFuture.runAsync(() -> writer.accept(segmentNumber), executorService));
        }
        latch.release();
        Futures.allOf(writerFutures).join();
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

        final ReadIndexConfig config = ReadIndexConfig.builder().with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, minReadLength).build();

        @Cleanup
        TestContext context = new TestContext(config, CachePolicy.INFINITE);

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
                appendSingleWrite(segmentId, new ByteArraySegment(appendData), context);
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
            byte[] entryData = entry.getContent().join().getCopy();
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
        appendSingleWrite(segmentId, new ByteArraySegment(appendData), context);
        recordAppend(segmentId, new ByteArraySegment(appendData), segmentContents);

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
            val resultData = context.readIndex.readDirect(segmentId, offset, 2);
            Assert.assertNull("readDirect() returned data overlapping a partially merged transaction", resultData);
        }

        // Verify that we can read from any other offset.
        final byte[] expectedData = segmentContents.get(segmentId).toByteArray();
        BiConsumer<Long, Long> verifyReadResult = (startOffset, endOffset) -> {
            int readLength = (int) (endOffset - startOffset);
            while (readLength > 0) {
                BufferView actualDataBuffer;
                try {
                    actualDataBuffer = context.readIndex.readDirect(segmentId, startOffset, readLength);
                } catch (StreamSegmentNotExistsException ex) {
                    throw new CompletionException(ex);
                }
                Assert.assertNotNull(
                        String.format("Unexpected result when data is readily available for Offset = %s, Length = %s.", startOffset, readLength),
                        actualDataBuffer);

                byte[] actualData = actualDataBuffer.getCopy();
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
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, new CachePolicy(Long.MAX_VALUE, Duration.ofMillis(1000000), Duration.ofMillis(10000)));
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

        HashSet<Integer> deletedEntries = new HashSet<>();
        context.cacheStorage.deleteCallback = deletedEntries::add;
        context.cacheManager.applyCachePolicy();
        AssertExtensions.assertGreaterThan("Expected at least one cache entry to be removed.", 0, deletedEntries.size());
    }

    /**
     * Tests a scenario of truncation that happens concurrently with reading (segment is truncated while reading).
     */
    @Test
    public void testTruncateConcurrently() throws Exception {
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
        AssertExtensions.assertSuppliedFutureThrows(
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
     * Tests the merging of empty Segments.
     */
    @Test
    public void testMergeEmptySegment() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        Collection<Long> segmentIds = Collections.singleton(createSegment(0, context));
        HashMap<Long, ArrayList<Long>> transactionsBySegment = createTransactions(segmentIds, 1, context);
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // Add a bunch of writes.
        appendData(segmentIds, segmentContents, context);

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
                ByteArraySegment data = getAppendData(segmentMetadata.getName(), segmentId, i, writeCount.incrementAndGet());

                // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
                long offset = segmentMetadata.getLength();
                segmentMetadata.setLength(offset + data.getLength());
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
                // Check to see if the exception we got was expected due to segment being sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(isExpectedAfterSealed(err) && segmentsToSeal.contains(e.getKey()))) {
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
            Assert.assertEquals("Unexpected read length for " + (isSealed ? "sealed " : "") + "segment " + expectedData.length, expectedLength, actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + segmentId, expectedData, 0, actualData, 0, actualData.length);
        }

        // Finally, pick a non-sealed segment, issue a Future Read from it and then close the index - verify the Future
        // Read is cancelled.
        Long notSealedId = segmentIds.stream().filter(id -> !segmentsToSeal.contains(id)).findFirst().orElse(null);
        Assert.assertNotNull("Expecting at least one non-sealed segment.", notSealedId);
        @Cleanup
        ReadResult rr = context.readIndex.read(notSealedId, context.metadata.getStreamSegmentMetadata(notSealedId).getLength(), 1, TIMEOUT);
        ReadResultEntry fe = rr.next();
        Assert.assertEquals("Expecting a Future Read.", ReadResultEntryType.Future, fe.getType());
        Assert.assertFalse("Not expecting Future Read to be completed.", fe.getContent().isDone());
        context.readIndex.close();
        Assert.assertTrue("Expected the Future Read to have been cancelled when the ReadIndex was closed.", fe.getContent().isCancelled());
    }

    /**
     * Tests the behavior of Future Reads on an empty index that is sealed.
     */
    @Test
    public void testFutureReadsEmptyIndex() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Create an empty segment. This is the easiest way to ensure the Read Index is empty.
        long segmentId = createSegment(0, context);
        @Cleanup
        val rr = context.readIndex.read(segmentId, 0, 1, TIMEOUT);
        val futureReadEntry = rr.next();
        Assert.assertEquals("Unexpected entry type.", ReadResultEntryType.Future, futureReadEntry.getType());
        Assert.assertFalse("ReadResultEntry is completed.", futureReadEntry.getContent().isDone());

        // Seal the segment. This should complete all future reads.
        context.metadata.getStreamSegmentMetadata(segmentId).markSealed();
        context.readIndex.triggerFutureReads(Collections.singleton(segmentId));
        Assert.assertTrue("Expected future read to be failed after sealing.", futureReadEntry.getContent().isCompletedExceptionally());
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected future read to be failed with appropriate exception.",
                futureReadEntry::getContent,
                ex -> ex instanceof StreamSegmentSealedException);
    }


    /**
     * Tests the ability to auto-unregister Future Reads if they are cancelled externally.
     */
    @Test
    public void testFutureReadsCancelled() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Create an empty segment. This is the easiest way to ensure the Read Index is empty.
        long segmentId = createSegment(0, context);
        @Cleanup
        val rr = context.readIndex.read(segmentId, 0, 1, TIMEOUT);
        val futureReadEntry = rr.next();
        Assert.assertEquals("Unexpected entry type.", ReadResultEntryType.Future, futureReadEntry.getType());
        Assert.assertFalse("ReadResultEntry is completed.", futureReadEntry.getContent().isDone());

        rr.close();
        Assert.assertTrue(futureReadEntry.getContent().isCancelled());

        AssertExtensions.assertEventuallyEquals("FutureReadResultEntry not unregistered after owning ReadResult closed.",
                0, () -> context.readIndex.getIndex(segmentId).getFutureReadCount(),
                10, TIMEOUT.toMillis());
    }

    /**
     * Tests the following scenario:
     * 1. We have a future read registered at offset N.
     * 2. (Thread A) Segment's length has been updated (via appends) to M (N < M).
     * 3. (Thread A) A call to {@link ContainerReadIndex#append} is made for range [N-x, N).
     * 4. (Thread B) Segment has been sealed with length M (asynchronously).
     * 5. (Thread A) A call to {@link ContainerReadIndex#triggerFutureReads} is invoked for the appends at steps 2-3.
     * 6. (Thread A) The appends at step 2. are added to the Read index. (This mimics the OperationProcessor behavior that adds
     * entries to the read index asynchronously, after updating the metadata).
     * <p>
     * The Future Read Result at step 1 should correctly return the data from the appends that are added to the index at step 4.
     */
    @Test
    public void testFutureReadsSealAppend() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // 1. Register a future read.
        long segmentId = createSegment(0, context);
        val segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);

        val append1 = getAppendData(segmentMetadata.getName(), segmentId, 0, 0);
        val append2 = getAppendData(segmentMetadata.getName(), segmentId, 1, 1);

        @Cleanup
        val rr = context.readIndex.read(segmentId, append1.getLength(), append2.getLength(), TIMEOUT);
        val index = context.readIndex.getIndex(segmentId);
        val futureReadEntry = rr.next();
        Assert.assertEquals("Unexpected entry type.", ReadResultEntryType.Future, futureReadEntry.getType());
        Assert.assertFalse("ReadResultEntry is completed.", futureReadEntry.getContent().isDone());
        Assert.assertEquals("Expected future read to have been registered.", 1, index.getFutureReadCount());

        // 2. Set the segment's length.
        segmentMetadata.setLength(append1.getLength() + append2.getLength());

        // 3. Make the initial append.
        index.append(0, append1);

        // 4. Seal the segment
        segmentMetadata.markSealed();

        // 5. First triggerFutureReads (due to append1)
        index.triggerFutureReads();
        Assert.assertFalse("Not expecting future read to have been completed yet.", futureReadEntry.getContent().isDone());
        Assert.assertEquals("Expected original future read to still be registered.", 1, index.getFutureReadCount());

        // 6. Make append2.
        index.append(append1.getLength(), append2);
        index.triggerFutureReads();
        val readContent = futureReadEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected data read back from future read.", append2, readContent);
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
        String transactionName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
        context.metadata.mapStreamSegmentId(transactionName, transactionId);
        initializeSegment(transactionId, context);

        ByteArraySegment appendData = new ByteArraySegment("foo".getBytes());
        UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        long segmentOffset = segmentMetadata.getLength();
        segmentMetadata.setLength(segmentOffset + appendData.getLength());
        context.readIndex.append(segmentId, segmentOffset, appendData);

        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        long transactionOffset = transactionMetadata.getLength();
        transactionMetadata.setLength(transactionOffset + appendData.getLength());
        context.readIndex.append(transactionId, transactionOffset, appendData);

        // 1. Appends at wrong offsets.
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with an offset beyond the Segment's DurableLogOffset.",
                () -> context.readIndex.append(segmentId, Integer.MAX_VALUE, appendData),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid offset.",
                () -> context.readIndex.append(segmentId, 0, appendData),
                ex -> ex instanceof IllegalArgumentException);

        // 2. Appends or reads with wrong SegmentIds
        AssertExtensions.assertThrows(
                "append did not throw the correct exception when provided with invalid SegmentId.",
                () -> context.readIndex.append(transactionId + 1, 0, appendData),
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
                () -> context.readIndex.append(transactionId, transactionMetadata.getLength(), appendData),
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
     * Tests a scenario where two concurrent Storage reads for the same offset execute, and the second ends up overwriting
     * the first one.
     */
    @Test
    public void testStorageReadsConcurrentWithOverwrite() throws Exception {
        testStorageReadsConcurrentWithOverwrite(0);
        testStorageReadsConcurrentWithOverwrite(1);
    }

    private void testStorageReadsConcurrentWithOverwrite(int offsetDeltaBetweenReads) throws Exception {
        testStorageReadsConcurrent(offsetDeltaBetweenReads, 1,
                (context, metadata) -> {
                    // Do nothing.
                },
                (context, metadata) -> {
                    // Check all the appended data. It must not have been overridden.
                    Assert.assertEquals("Not expecting any extra data in this test.", metadata.getLength(), metadata.getStorageLength());
                    val readResult = context.readIndex.read(metadata.getId(), 0, (int) metadata.getStorageLength(), TIMEOUT);

                    // Read from segment.
                    byte[] segmentData = new byte[(int) metadata.getStorageLength()];
                    readResult.readRemaining(segmentData, TIMEOUT);

                    // Then from Storage.
                    byte[] storageData = new byte[segmentData.length];
                    context.storage.openRead(metadata.getName())
                            .thenCompose(handle -> context.storage.read(handle, 0, storageData, 0, storageData.length, TIMEOUT))
                            .join();

                    Assert.assertArrayEquals("Unexpected appended data read back.", storageData, segmentData);

                    // The cleanup is async, so we must keep trying to check until it is done.
                    AssertExtensions.assertEventuallyEquals("Unexpected number of bytes in the cache.",
                            (long) storageData.length,
                            () -> context.cacheStorage.getState().getStoredBytes(),
                            10, TIMEOUT.toMillis());

                });
    }

    /**
     * Tests a scenario where two concurrent Storage reads for the same offset execute, but the first one completes first,
     * then a new append is added to the segment, and when the second read completes it should be discarded (as opposed
     * from overwriting the Read Index Entry).
     */
    @Test
    public void testStorageReadsConcurrentNoOverwrite() throws Exception {
        testStorageReadsConcurrentNoOverwrite(0);
        testStorageReadsConcurrentNoOverwrite(1);
    }

    private void testStorageReadsConcurrentNoOverwrite(int offsetDeltaBetweenReads) throws Exception {
        val appendedData = new AtomicReference<ByteArraySegment>();
        testStorageReadsConcurrent(offsetDeltaBetweenReads, 0,
                (context, metadata) -> {
                    // Now perform an append.
                    appendedData.set(getAppendData(metadata.getName(), metadata.getId(), 1, 1));
                    metadata.setLength(metadata.getLength() + appendedData.get().getLength());
                    context.readIndex.append(metadata.getId(), metadata.getStorageLength(), appendedData.get());
                },
                (context, metadata) -> {
                    // Check all the appended data. It must not have been overridden.
                    val appendedDataStream = context.readIndex.readDirect(metadata.getId(), metadata.getStorageLength(), appendedData.get().getLength());
                    Assert.assertNotNull("Unable to read appended data.", appendedDataStream);
                    val actualAppendedData = appendedDataStream.getCopy();
                    AssertExtensions.assertArrayEquals("Unexpected appended data read back.",
                            appendedData.get().array(), 0, actualAppendedData, 0, appendedData.get().getLength());

                    // The cleanup is async, so we must keep trying to check until it is done.
                    AssertExtensions.assertEventuallyEquals("Unexpected number of bytes in the cache.",
                            metadata.getLength(),
                            () -> context.cacheStorage.getState().getStoredBytes(),
                            10, TIMEOUT.toMillis());
                });
    }

    private void testStorageReadsConcurrent(
            int offsetDeltaBetweenReads,
            int extraAllowedStorageReads,
            BiConsumerWithException<TestContext, UpdateableSegmentMetadata> executeBetweenReads,
            BiConsumerWithException<TestContext, UpdateableSegmentMetadata> finalCheck) throws Exception {
        val maxAllowedStorageReads = 2 + extraAllowedStorageReads;

        // Set a cache size big enough to prevent the Cache Manager from enabling "essential-only" mode due to over-utilization.
        val cachePolicy = new CachePolicy(10000, 0.01, 1.0, Duration.ofMillis(10), Duration.ofMillis(10));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);

        // Create the segment
        val segmentId = createSegment(0, context);
        val metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        context.storage.create(metadata.getName(), TIMEOUT).join();

        // Append some data to the Read Index.
        val dataInStorage = getAppendData(metadata.getName(), segmentId, 0, 0);
        metadata.setLength(dataInStorage.getLength());
        context.readIndex.append(segmentId, 0, dataInStorage);

        // Then write to Storage.
        context.storage.openWrite(metadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, dataInStorage.getReader(), dataInStorage.getLength(), TIMEOUT))
                .join();
        metadata.setStorageLength(dataInStorage.getLength());

        // Then evict it from the cache.
        boolean evicted = context.cacheManager.applyCachePolicy();
        Assert.assertTrue("Expected an eviction.", evicted);

        @Cleanup("release")
        val firstReadBlocker = new ReusableLatch();
        @Cleanup("release")
        val firstRead = new ReusableLatch();
        @Cleanup("release")
        val secondReadBlocker = new ReusableLatch();
        @Cleanup("release")
        val secondRead = new ReusableLatch();
        val cacheInsertCount = new AtomicInteger();
        context.cacheStorage.insertCallback = address -> {
            if (cacheInsertCount.incrementAndGet() > 1) {
                Assert.fail("Too many cache inserts.");
            }
        };

        val storageReadCount = new AtomicInteger();
        context.storage.setReadInterceptor((segment, wrappedStorage) -> {
            int readCount = storageReadCount.incrementAndGet();
            if (readCount == 1) {
                firstRead.release();
                Exceptions.handleInterrupted(firstReadBlocker::await);
            } else if (readCount == 2) {
                secondRead.release();
                Exceptions.handleInterrupted(secondReadBlocker::await);
            } else if (readCount > maxAllowedStorageReads) {
                Assert.fail("Too many storage reads. Max allowed = " + maxAllowedStorageReads);
            }
        });

        // Initiate the first Storage Read.
        val read1Result = context.readIndex.read(segmentId, 0, dataInStorage.getLength(), TIMEOUT);
        val read1Data = new byte[dataInStorage.getLength()];
        val read1Future = CompletableFuture.runAsync(() -> read1Result.readRemaining(read1Data, TIMEOUT), executorService());

        // Wait for it to process.
        firstRead.await();

        // Initiate the second storage read.
        val read2Length = dataInStorage.getLength() - offsetDeltaBetweenReads;
        val read2Result = context.readIndex.read(segmentId, offsetDeltaBetweenReads, read2Length, TIMEOUT);
        val read2Data = new byte[read2Length];
        val read2Future = CompletableFuture.runAsync(() -> read2Result.readRemaining(read2Data, TIMEOUT), executorService());

        secondRead.await();

        // Unblock the first Storage Read and wait for it to complete.
        firstReadBlocker.release();
        read1Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the data from the first read to be fully added to the cache. Without this the subsequent append will not write to this entry.
        TestUtils.await(
                () -> {
                    try {
                        return context.readIndex.read(0, 0, dataInStorage.getLength(), TIMEOUT).next().getType() == ReadResultEntryType.Cache;
                    } catch (StreamSegmentNotExistsException ex) {
                        throw new CompletionException(ex);
                    }
                }, 10, TIMEOUT.toMillis());

        // If there's anything to do between the two reads, do it now.
        executeBetweenReads.accept(context, metadata);

        // Unblock second Storage Read.
        secondReadBlocker.release();
        read2Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Perform final check.
        finalCheck.accept(context, metadata);
        Assert.assertEquals("Unexpected number of storage reads.", maxAllowedStorageReads, storageReadCount.get());
        Assert.assertEquals("Unexpected number of cache inserts.", 1, cacheInsertCount.get());
    }

    /**
     * Tests a scenario where two concurrent Storage reads for overlapping ranges execute, with the smaller one
     * completing first. This test validates that no data duplication occurs in the cache and that the index is properly
     * updated in this case.
     */
    @Test
    public void testStorageReadsConcurrentSmallLarge() throws Exception {
        // Read 1 is wholly contained within Read 2.
        testStorageReadsConcurrentSmallLarge(20000, 5000, 5000, 0, 20000);

        // Read 1 and 2 overlap, but Read 2 begins before Read 1.
        testStorageReadsConcurrentSmallLarge(20000, 5000, 5000, 3900, 16100);

        // These are values from a real-world test, that lead to the discovery of the issue verified here.
        testStorageReadsConcurrentSmallLarge(1011221, 397254, 8209, 0, 1011221);

        // Read 1 and 2 overlap, but Read 2 begins before Read 1.
        testStorageReadsConcurrentSmallLarge(1011221, 397254, 8209, 393149, 618072);
    }

    private void testStorageReadsConcurrentSmallLarge(int segmentLength, int read1Offset, int read1Length, int read2Offset, int read2Length) throws Exception {
        // We only expect 2 Storage reads for this test.
        val expectedStorageReadCount = 2;
        val cachePolicy = new CachePolicy(100, 0.01, 1.0, Duration.ofMillis(10), Duration.ofMillis(10));

        val config = ReadIndexConfig
                .builder()
                .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, DEFAULT_CONFIG.getMemoryReadMinLength())
                .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, segmentLength)
                .build();

        @Cleanup
        TestContext context = new TestContext(config, cachePolicy);

        // Create the segment
        val segmentId = createSegment(0, context);
        val metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        context.storage.create(metadata.getName(), TIMEOUT).join();

        // Write some data to the segment in Storage.
        val rnd = new Random(0);
        val segmentData = new ByteArraySegment(new byte[segmentLength]);
        rnd.nextBytes(segmentData.array());
        context.storage.openWrite(metadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, segmentData.getReader(), segmentData.getLength(), TIMEOUT))
                .join();
        metadata.setLength(segmentData.getLength());
        metadata.setStorageLength(segmentData.getLength());

        @Cleanup("release")
        val firstReadBlocker = new ReusableLatch();
        @Cleanup("release")
        val firstRead = new ReusableLatch();
        @Cleanup("release")
        val secondReadBlocker = new ReusableLatch();
        @Cleanup("release")
        val secondRead = new ReusableLatch();
        val cacheInsertCount = new AtomicInteger();
        context.cacheStorage.insertCallback = address -> cacheInsertCount.incrementAndGet();

        val storageReadCount = new AtomicInteger();
        context.storage.setReadInterceptor((segment, wrappedStorage) -> {
            int readCount = storageReadCount.incrementAndGet();
            if (readCount == 1) {
                firstRead.release();
                Exceptions.handleInterrupted(firstReadBlocker::await);
            } else if (readCount == 2) {
                secondRead.release();
                Exceptions.handleInterrupted(secondReadBlocker::await);
            }
        });

        // Initiate the first Storage Read.
        val read1Result = context.readIndex.read(segmentId, read1Offset, read1Length, TIMEOUT);
        val read1Data = new byte[read1Length];
        val read1Future = CompletableFuture.runAsync(() -> read1Result.readRemaining(read1Data, TIMEOUT), executorService());

        // Wait for it to process.
        firstRead.await();

        // Initiate the second storage read.
        val read2Result = context.readIndex.read(segmentId, read2Offset, read2Length, TIMEOUT);
        val read2Data = new byte[read2Length];
        val read2Future = CompletableFuture.runAsync(() -> read2Result.readRemaining(read2Data, TIMEOUT), executorService());

        secondRead.await();

        // Unblock the first Storage Read and wait for it to complete.
        firstReadBlocker.release();
        read1Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Unblock second Storage Read.
        secondReadBlocker.release();
        read2Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the data from the second read to be fully added to the cache (background task).
        TestUtils.await(
                () -> {
                    try {
                        return context.readIndex.read(0, read2Offset, read2Length, TIMEOUT).next().getType() == ReadResultEntryType.Cache;
                    } catch (StreamSegmentNotExistsException ex) {
                        throw new CompletionException(ex);
                    }
                }, 10, TIMEOUT.toMillis());

        // Verify that the initial read requests retrieved the data correctly.
        Assert.assertEquals("Initial Read 1 (Storage)", segmentData.slice(read1Offset, read1Length), new ByteArraySegment(read1Data));
        Assert.assertEquals("Initial Read 2 (Storage)", segmentData.slice(read2Offset, read2Length), new ByteArraySegment(read2Data));

        // Re-issue the read requests for the exact same offsets. This time it should be from the cache.
        val read1Data2 = new byte[read1Length];
        context.readIndex.read(segmentId, read1Offset, read1Length, TIMEOUT).readRemaining(read1Data2, TIMEOUT);
        Assert.assertArrayEquals("Reissued Read 1 (Cache)", read1Data, read1Data2);

        val read2Data2 = new byte[read2Length];
        context.readIndex.read(segmentId, read2Offset, read2Length, TIMEOUT).readRemaining(read2Data2, TIMEOUT);
        Assert.assertArrayEquals("Reissued Read 2 (Cache)", read2Data, read2Data2);

        // Verify that we did the expected number of Storage/Cache operations.
        Assert.assertEquals("Unexpected number of storage reads.", expectedStorageReadCount, storageReadCount.get());
        Assert.assertTrue("Unexpected number of cache inserts.", cacheInsertCount.get() == 3 || cacheInsertCount.get() == 1);
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
     * Tests the ability to handle Cache/Index Update failures post a successful Storage Read.
     */
    @Test
    public void testStorageFailedCacheInsert() throws Exception {
        final int segmentLength = 1024;
        // Create a segment and write some data in Storage for it.
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);
        createSegmentsInStorage(context);
        val testSegmentId = segmentIds.get(0);
        UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(testSegmentId);
        sm.setStorageLength(segmentLength);
        sm.setLength(segmentLength);
        context.storage.openWrite(sm.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[segmentLength]), segmentLength, TIMEOUT))
                .join();

        // Keep track of inserted/deleted calls to the Cache, and "fail" the insert call.
        val inserted = new ReusableLatch();
        val insertedAddress = new AtomicInteger(CacheStorage.NO_ADDRESS);
        val deletedAddress = new AtomicInteger(Integer.MAX_VALUE);
        context.cacheStorage.insertCallback = address -> {
            context.cacheStorage.delete(address); // Immediately delete this data (prevent leaks).
            Assert.assertTrue(insertedAddress.compareAndSet(CacheStorage.NO_ADDRESS, address));
            inserted.release();
            throw new IntentionalException();
        };
        context.cacheStorage.deleteCallback = deletedAddress::set;

        // Trigger a read. The first read call will be served with data directly from Storage, so we expect it to be successful.
        @Cleanup
        ReadResult readResult = context.readIndex.read(testSegmentId, 0, segmentLength, TIMEOUT);
        ReadResultEntry entry = readResult.next();
        Assert.assertEquals("Unexpected ReadResultEntryType.", ReadResultEntryType.Storage, entry.getType());
        entry.requestContent(TIMEOUT);
        entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // This should complete without issues.

        // Verify that the cache insert attempt has been made
        inserted.await();
        Assert.assertNotEquals("Expected an insert attempt to have been made.", CacheStorage.NO_ADDRESS, insertedAddress.get());
        AssertExtensions.assertEventuallyEquals(CacheStorage.NO_ADDRESS, deletedAddress::get, TIMEOUT.toMillis());
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
        HashMap<Long, ByteArrayOutputStream> segmentContents = new HashMap<>();

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
     * Tests the ability to return a copy of the data from the ReadIndex as opposed from a direct reference.
     */
    @Test
    public void testCopyOnRead() throws Exception {
        final long segmentId = 0;
        final int appendLength = 100;
        final byte[] data1 = new byte[appendLength];
        final byte[] data2 = new byte[appendLength];
        final Random rnd = new Random(0);
        rnd.nextBytes(data1);
        rnd.nextBytes(data2);

        // Create all the segments in the metadata.
        @Cleanup
        TestContext context = new TestContext();

        createSegment(0, context);

        // Append some data and intercept the address it was written to.
        val address = new AtomicInteger(-1);
        context.cacheStorage.insertCallback = address::set;
        context.metadata.getStreamSegmentMetadata(segmentId).setLength(appendLength);
        context.readIndex.append(segmentId, 0, new ByteArraySegment(data1));
        Assert.assertNotEquals(-1, address.get());

        // Initiates the reads and collect the result as a composite BufferView. Do not copy the data yet.
        val rr1 = context.readIndex.read(segmentId, 0, appendLength, TIMEOUT);
        rr1.setCopyOnRead(true);
        val read1Builder = BufferView.builder();
        rr1.forEachRemaining(rre -> read1Builder.add(rre.getContent().join()));
        val read1Buffer = read1Builder.build();

        val rr2 = context.readIndex.read(segmentId, 0, appendLength, TIMEOUT);
        rr2.setCopyOnRead(false);
        val read2Builder = BufferView.builder();
        rr2.forEachRemaining(rre -> read2Builder.add(rre.getContent().join()));
        val read2Buffer = read2Builder.build();

        // Pretty brutal, but this simulates cache evicted and cache block reused. Delete data at its address and
        // insert new one.
        context.cacheStorage.delete(address.get());
        val address2 = context.cacheStorage.insert(new ByteArraySegment(data2));
        Assert.assertEquals(address.get(), address2);

        // Now make use of the generated BufferViews. One should return the original data since we made a copy before
        // we changed the back-end cache, and the second one should return the current contents of the cache block.
        val read1 = read1Buffer.getCopy();
        val read2 = read2Buffer.getCopy();
        Assert.assertArrayEquals("Copy-on-read data not preserved.", data1, read1);
        Assert.assertArrayEquals("Not expected copy-on-read data.", data2, read2);
    }

    /**
     * Tests the ability to evict entries from the ReadIndex under various conditions:
     * * If an entry is aged out
     * * If an entry is pushed out because of cache space pressure.
     *
     * This also verifies that certain entries, such as RedirectReadIndexEntries and entries after the Storage Offset are
     * not removed.
     *
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
        // We append one byte at each time. This allows us to test edge cases as well by having the finest precision when
        // it comes to selecting which bytes we want evicted and which kept.
        final int appendSize = 1;
        final int entriesPerSegment = 100; // This also doubles as number of generations (each generation, we add one append for each segment).
        final int cacheMaxSize = SEGMENT_COUNT * entriesPerSegment * appendSize;
        final int postStorageEntryCount = entriesPerSegment / 4; // 25% of the entries are beyond the StorageOffset
        final int preStorageEntryCount = entriesPerSegment - postStorageEntryCount; // 75% of the entries are before the StorageOffset.
        CachePolicy cachePolicy = new CachePolicy(cacheMaxSize, 1.0, 1.0, Duration.ofMillis(1000 * 2 * entriesPerSegment), Duration.ofMillis(1000));

        // To properly test this, we want predictable storage reads.
        ReadIndexConfig config = ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, appendSize).build();

        ArrayList<Integer> removedEntries = new ArrayList<>();
        @Cleanup
        TestContext context = new TestContext(config, cachePolicy);
        // To ease our testing, we disable appends and instruct the TestCache to report the same value for UsedBytes as it
        // has for StoredBytes. This shields us from having to know internal details about the layout of the cache.
        context.cacheStorage.usedBytesSameAsStoredBytes = true;
        context.cacheStorage.disableAppends = true;
        context.cacheStorage.deleteCallback = removedEntries::add; // Record every cache removal.

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

        val cacheMappings = new HashMap<Integer, SegmentOffset>();

        // Callback that appends one entry at the end of the given segment id.
        Consumer<Long> appendOneEntry = segmentId -> {
            UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            byte[] data = new byte[appendSize];
            long offset = sm.getLength();
            sm.setLength(offset + data.length);
            try {
                context.cacheStorage.insertCallback = address -> cacheMappings.put(address, new SegmentOffset(segmentId, offset));
                context.readIndex.append(segmentId, offset, new ByteArraySegment(data));
            } catch (StreamSegmentNotExistsException ex) {
                throw new CompletionException(ex);
            }
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
                CompletableFuture<Void> insertedInCache = new CompletableFuture<>();
                context.cacheStorage.insertCallback = address -> {
                    cacheMappings.put(address, new SegmentOffset(segmentId, offset));
                    insertedInCache.complete(null);
                };
                resultEntry.requestContent(TIMEOUT);
                BufferView contents = resultEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertFalse("Not expecting more data to be available for reading.", result.hasNext());
                Assert.assertEquals("Unexpected ReadResultEntry length when trying to load up data into the ReadIndex Cache.", appendSize, contents.getLength());

                // Wait for the entry to be inserted into the cache before moving on.
                insertedInCache.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            }

            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (cache is not full).", 0, removedEntries.size());

        // Append more data (equivalent to all post-storage entries), and verify that NO entries are being evicted (we cannot evict post-storage entries).
        for (int i = 0; i < postStorageEntryCount; i++) {
            segmentIds.forEach(appendOneEntry);
            context.cacheManager.applyCachePolicy();
        }

        Assert.assertEquals("Not expecting any removed Cache entries at this point (only eligible entries were post-storage).", 0, removedEntries.size());

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

        Assert.assertEquals("Not expecting any removed Cache entries at this point (we touched old entries and they now have the newest generation).", 0, removedEntries.size());

        // Increment the generations so that we are caught up to just before the generation where the "touched" items now live.
        context.cacheManager.applyCachePolicy();

        // We expect all but the 'touchCount' pre-Storage entries to be removed.
        int expectedRemovalCount = (preStorageEntryCount - touchCount) * SEGMENT_COUNT;
        Assert.assertEquals("Unexpected number of removed entries after having forced out all pre-storage entries.", expectedRemovalCount, removedEntries.size());

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
        Assert.assertEquals("Unexpected number of removed entries after having forced out all the entries.", expectedTotalRemovalCount, removedEntries.size());

        // Finally, verify that the evicted items are in the correct order (for each segment). See this test's description for details.
        for (long segmentId : segmentIds) {
            List<SegmentOffset> segmentRemovedKeys = removedEntries.stream()
                    .map(cacheMappings::get)
                    .filter(e -> e.segmentId == segmentId)
                    .collect(Collectors.toList());
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

    /**
     * Tests the {@link ContainerReadIndex#trimCache()} method.
     */
    @Test
    public void testTrimCache() throws Exception {
        // Create a CachePolicy with a set number of generations and a known max size.
        // Each generation contains exactly one entry, so the number of generations is also the number of entries.
        // We append one byte at each time. This allows us to test edge cases as well by having the finest precision when
        // it comes to selecting which bytes we want evicted and which kept.
        final int appendCount = 100;
        final int segmentId = 123;
        final byte[] appendData = new byte[2];

        val removedEntryCount = new AtomicInteger();
        @Cleanup
        TestContext context = new TestContext();
        context.metadata.enterRecoveryMode();
        context.readIndex.enterRecoveryMode(context.metadata);

        // To ease our testing, we disable appends and instruct the TestCache to report the same value for UsedBytes as it
        // has for StoredBytes. This shields us from having to know internal details about the layout of the cache.
        context.cacheStorage.usedBytesSameAsStoredBytes = true;
        context.cacheStorage.disableAppends = true;
        context.cacheStorage.deleteCallback = e -> removedEntryCount.incrementAndGet();

        createSegment(segmentId, context);
        val metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setLength(appendCount * appendData.length);
        for (int i = 0; i < appendCount; i++) {
            long offset = i * appendData.length;
            context.readIndex.append(segmentId, offset, new ByteArraySegment(appendData));
        }

        // Gradually increase the StorageLength of the segment and invoke trimCache twice at every step. We want to verify
        // that it also does not evict more than it should if it has nothing to do.
        int deltaIncrease = 0;
        while (metadata.getStorageLength() < metadata.getLength()) {
            val trim1 = context.readIndex.trimCache();
            Assert.assertEquals("Not expecting any bytes trimmed.", 0, trim1);

            // Every time we trim, increase the StorageLength by a bigger amount - but make sure we don't exceed the length of the segment.
            deltaIncrease = (int) Math.min(metadata.getLength() - metadata.getStorageLength(), deltaIncrease + appendData.length);
            metadata.setStorageLength(Math.min(metadata.getLength(), metadata.getStorageLength() + deltaIncrease));
            removedEntryCount.set(0);
            val trim2 = context.readIndex.trimCache();
            Assert.assertEquals("Unexpected number of bytes trimmed.", deltaIncrease, trim2);
            Assert.assertEquals("Unexpected number of cache entries evicted.", deltaIncrease / appendData.length, removedEntryCount.get());
        }

        // Take the index out of recovery mode.
        context.metadata.exitRecoveryMode();
        context.readIndex.exitRecoveryMode(true);

        // Verify that the entries have actually been evicted.
        for (int i = 0; i < appendCount; i++) {
            long offset = i * appendData.length;
            @Cleanup
            val readResult = context.readIndex.read(segmentId, offset, appendData.length, TIMEOUT);
            val first = readResult.next();
            Assert.assertEquals("", ReadResultEntryType.Storage, first.getType());
        }

        // Verify trimCache() doesn't work when we are not in recovery mode.
        AssertExtensions.assertThrows(
                "trimCache worked in non-recovery mode.",
                context.readIndex::trimCache,
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the {@link ContainerReadIndex#cleanup} method as well as its handling of inactive segments.
     */
    @Test
    public void testCleanup() throws Exception {
        // Create all the segments in the metadata.
        @Cleanup
        TestContext context = new TestContext();
        ArrayList<Long> segmentIds = createSegments(context);

        final long activeSegmentId = segmentIds.get(0); // Always stays active.
        final long inactiveSegmentId1 = segmentIds.get(1); // Becomes inactive - used for cleanup()
        final long inactiveSegmentId2 = segmentIds.get(2); // Becomes inactive - used for getOrCreateIndex() (any op).
        final long reactivatedSegmentId1 = segmentIds.get(3); // Becomes inactive and then active again (for cleanup)
        final long reactivatedSegmentId2 = segmentIds.get(4); // Becomes inactive and then active again (for getOrCreateIndex()).

        // Add a zero-byte append, which ensures the Segments' Read Indices are initialized.
        for (val id : segmentIds) {
            context.readIndex.append(id, 0, BufferView.empty());
        }

        // Mark 2 segments as inactive, but do not evict them yet. We simulate a concurrent eviction, when the segment is
        // first marked as inactive and the Read Index receives a request for it before it gets evicted.
        markInactive(inactiveSegmentId1, context);
        markInactive(inactiveSegmentId2, context);

        // Evict 2 segments (which also marks them as inactive), then re-map them as active segments (which gives them
        // new instances of their Segment Metadatas).
        val reactivatedSegment2OldIndex = context.readIndex.getIndex(reactivatedSegmentId2);
        evict(reactivatedSegmentId1, context);
        evict(reactivatedSegmentId2, context);
        createSegment(reactivatedSegmentId1, context);
        createSegment(reactivatedSegmentId2, context);

        // Test cleanup().
        context.readIndex.cleanup(Arrays.asList(activeSegmentId, inactiveSegmentId1, reactivatedSegmentId1));
        Assert.assertNotNull("Active segment's index removed during cleanup.", context.readIndex.getIndex(activeSegmentId));
        Assert.assertNull("Inactive segment's index not removed during cleanup.", context.readIndex.getIndex(inactiveSegmentId1));
        Assert.assertNull("Reactivated segment's index not removed during cleanup.", context.readIndex.getIndex(reactivatedSegmentId1));

        // Test getOrCreateIndex() via append() (any other operation could be used for this, but this is the simplest to setup).
        AssertExtensions.assertThrows(
                "Appending to inactive segment succeeded.",
                () -> context.readIndex.append(inactiveSegmentId2, 0, BufferView.empty()),
                ex -> ex instanceof IllegalArgumentException);

        // This should re-create the index.
        context.readIndex.append(reactivatedSegmentId2, 0, BufferView.empty());
        Assert.assertNotEquals("Reactivated Segment's ReadIndex was not re-created.",
                reactivatedSegment2OldIndex, context.readIndex.getIndex(reactivatedSegmentId2));
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
        long transactionId = createTransaction(1, context);
        createSegmentsInStorage(context);
        ByteArrayOutputStream writtenStream = new ByteArrayOutputStream();

        // Write something to the transaction, and make sure it also makes its way to Storage.
        UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        ByteArraySegment transactionWriteData = getAppendData(transactionMetadata.getName(), transactionId, 0, 0);
        appendSingleWrite(transactionId, transactionWriteData, context);
        val handle = context.storage.openWrite(transactionMetadata.getName()).join();
        context.storage.write(handle, 0, transactionWriteData.getReader(), transactionWriteData.getLength(), TIMEOUT).join();
        transactionMetadata.setStorageLength(transactionMetadata.getLength());

        // Write some data to the parent, and make sure it is more than what we write to the transaction (hence the 10).
        for (int i = 0; i < 10; i++) {
            ByteArraySegment parentWriteData = getAppendData(parentMetadata.getName(), parentId, i, i);
            appendSingleWrite(parentId, parentWriteData, context);
            parentWriteData.copyTo(writtenStream);
        }

        // Seal & Begin-merge the transaction (do not seal in storage).
        transactionMetadata.markSealed();
        long mergeOffset = parentMetadata.getLength();
        parentMetadata.setLength(mergeOffset + transactionMetadata.getLength());
        context.readIndex.beginMerge(parentId, mergeOffset, transactionId);
        transactionMetadata.markMerged();
        transactionWriteData.copyTo(writtenStream);

        // Clear the cache.
        boolean evicted = context.cacheManager.applyCachePolicy();
        Assert.assertTrue("Expected an eviction.", evicted);

        // Issue read from the parent.
        ReadResult rr = context.readIndex.read(parentId, mergeOffset, transactionWriteData.getLength(), TIMEOUT);
        Assert.assertTrue("Parent Segment read indicates no data available.", rr.hasNext());
        ByteArrayOutputStream readStream = new ByteArrayOutputStream();
        long expectedOffset = mergeOffset;
        while (rr.hasNext()) {
            ReadResultEntry entry = rr.next();
            Assert.assertEquals("Unexpected offset for read result entry.", expectedOffset, entry.getStreamSegmentOffset());
            Assert.assertEquals("Served read result entry is not from storage.", ReadResultEntryType.Storage, entry.getType());

            // Request contents and store for later use.
            entry.requestContent(TIMEOUT);
            BufferView contents = entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            contents.copyTo(readStream);
            expectedOffset += contents.getLength();
        }

        byte[] readData = readStream.toByteArray();
        Assert.assertArrayEquals("Unexpected data read back.", transactionWriteData.getCopy(), readData);
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
        long transactionId = createTransaction(1, context);
        createSegmentsInStorage(context);

        ByteArraySegment writeData = getAppendData(context.metadata.getStreamSegmentMetadata(transactionId).getName(), transactionId, 0, 0);
        ReadResultEntry entry = setupMergeRead(parentId, transactionId, writeData.getCopy(), context);
        context.readIndex.completeMerge(parentId, transactionId);

        BufferView contents = entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        byte[] readData = contents.getCopy();
        Assert.assertArrayEquals("Unexpected data read from parent segment.", writeData.getCopy(), readData);
    }

    /**
     * Tests a scenario where a call to {@link StreamSegmentReadIndex#completeMerge} executes concurrently with a
     * CacheManager eviction. The Cache Manager must not evict the data for recently transferred entries, even if they
     * would otherwise be eligible for eviction in the source segment.
     */
    @Test
    public void testConcurrentEvictionTransactionStorageMerge() throws Exception {
        val mergeOffset = 1;
        val appendLength = 1;
        CachePolicy cachePolicy = new CachePolicy(1, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);

        // Create parent segment and one transaction
        long targetId = createSegment(0, context);
        long sourceId = createTransaction(1, context);
        val targetMetadata = context.metadata.getStreamSegmentMetadata(targetId);
        val sourceMetadata = context.metadata.getStreamSegmentMetadata(sourceId);
        createSegmentsInStorage(context);

        // Write something to the parent segment.
        appendSingleWrite(targetId, new ByteArraySegment(new byte[mergeOffset]), context);
        context.storage.openWrite(targetMetadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[mergeOffset]), mergeOffset, TIMEOUT)).join();

        // Write something to the transaction, but do not write anything in Storage - we want to verify we don't even
        // try to reach in there.
        val sourceContents = getAppendData(context.metadata.getStreamSegmentMetadata(sourceId).getName(), sourceId, 0, 0);
        appendSingleWrite(sourceId, sourceContents, context);
        sourceMetadata.setStorageLength(sourceMetadata.getLength());

        // Seal & Begin-merge the transaction (do not seal in storage).
        sourceMetadata.markSealed();
        targetMetadata.setLength(sourceMetadata.getLength() + mergeOffset);
        context.readIndex.beginMerge(targetId, mergeOffset, sourceId);
        sourceMetadata.markMerged();
        sourceMetadata.markDeleted();

        // Trigger a Complete Merge. We want to intercept and pause it immediately before it is unregistered from the
        // Cache Manager.
        @Cleanup("release")
        val unregisterCalled = new ReusableLatch();
        @Cleanup("release")
        val unregisterBlocker = new ReusableLatch();
        context.cacheManager.setUnregisterInterceptor(c -> {
            unregisterCalled.release();
            Exceptions.handleInterrupted(unregisterBlocker::await);
        });

        val completeMerge = CompletableFuture.runAsync(() -> {
            try {
                context.readIndex.completeMerge(targetId, sourceId);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, executorService());

        // Clear the cache. The source Read index is still registered in the Cache Manager - we want to ensure that any
        // eviction happening at this point will not delete anything from the Cache that we don't want deleted.
        unregisterCalled.await();
        context.cacheManager.applyCachePolicy();

        // Wait for the operation to complete.
        unregisterBlocker.release();
        completeMerge.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify that we can append (appending will modify the last cache entry; if it had been modified this would not
        // work anymore).
        val appendOffset = (int) targetMetadata.getLength();
        val appendData = new byte[appendLength];
        appendData[0] = (byte) 23;
        targetMetadata.setLength(appendOffset + appendLength);
        context.readIndex.append(targetId, appendOffset, new ByteArraySegment(appendData));

        // Issue a read and verify we can read everything that we wrote. If it had been evicted or erroneously deleted
        // from the cache this would result in an error.
        byte[] expectedData = new byte[appendOffset + appendLength];
        sourceContents.copyTo(expectedData, mergeOffset, sourceContents.getLength());
        System.arraycopy(appendData, 0, expectedData, appendOffset, appendLength);

        ReadResult rr = context.readIndex.read(targetId, 0, expectedData.length, TIMEOUT);
        Assert.assertTrue("Parent Segment read indicates no data available.", rr.hasNext());
        byte[] actualData = new byte[expectedData.length];
        rr.readRemaining(actualData, TIMEOUT);
        Assert.assertArrayEquals("Unexpected data read back.", expectedData, actualData);
    }

    /**
     * Tests the following scenario:
     * 1. Segment B has been merged into A
     * 2. We are executing a read on Segment A over a portion where B was merged into A.
     * 3. Concurrently with 2, a read on Segment B that went to LTS (possibly from the same result as before) wants to
     * insert into the Cache, but the cache is full. The Cache Manager would want to clean up the cache.
     * <p>
     * We want to ensure that there is no deadlock for this scenario.
     */
    @Test
    public void testConcurrentReadTransactionStorageReadCacheFull() throws Exception {
        val appendLength = 4 * 1024; // Must equal Cache Block size for easy eviction.
        val maxCacheSize = 2 * 1024 * 1024;
        // We set the policy's max size to a much higher value to avoid entering "essential-only" state.
        CachePolicy cachePolicy = new CachePolicy(2 * maxCacheSize, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy, maxCacheSize);
        val rnd = new Random(0);

        // Create parent segment and one transaction
        long targetId = createSegment(0, context);
        long sourceId = createTransaction(1, context);
        val targetMetadata = context.metadata.getStreamSegmentMetadata(targetId);
        val sourceMetadata = context.metadata.getStreamSegmentMetadata(sourceId);
        createSegmentsInStorage(context);

        // Write something to the transaction; and immediately evict it.
        val append1 = new byte[appendLength];
        val append2 = new byte[appendLength];
        rnd.nextBytes(append1);
        rnd.nextBytes(append2);
        val allData = BufferView.builder().add(new ByteArraySegment(append1)).add(new ByteArraySegment(append2)).build();

        appendSingleWrite(sourceId, new ByteArraySegment(append1), context);
        sourceMetadata.setStorageLength(sourceMetadata.getLength());
        context.cacheManager.applyCachePolicy(); // Increment the generation.

        // Write a second thing to the transaction, and do not evict it.
        appendSingleWrite(sourceId, new ByteArraySegment(append2), context);
        context.storage.openWrite(sourceMetadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, allData.getReader(), allData.getLength(), TIMEOUT))
                .join();

        // Seal & Begin-merge the transaction (do not seal in storage).
        sourceMetadata.markSealed();
        targetMetadata.setLength(sourceMetadata.getLength());
        context.readIndex.beginMerge(targetId, 0L, sourceId);
        sourceMetadata.markMerged();
        sourceMetadata.markDeleted();

        // At this point, the first append in the transaction should be evicted, while the second one should still be there.
        @Cleanup
        val rr = context.readIndex.read(targetId, 0, (int) targetMetadata.getLength(), TIMEOUT);
        @Cleanup
        val cacheCleanup = new AutoCloseObject();
        @Cleanup("release")
        val insertingInCache = new ReusableLatch();
        @Cleanup("release")
        val finishInsertingInCache = new ReusableLatch();
        context.cacheStorage.beforeInsert = () -> {
            context.cacheStorage.beforeInsert = null; // Prevent a stack overflow.

            // Fill up the cache with garbage - this will cause an unrecoverable Cache Full event (which is what we want).
            int toFill = (int) (context.cacheStorage.getState().getMaxBytes() - context.cacheStorage.getState().getUsedBytes());
            int address = context.cacheStorage.insert(new ByteArraySegment(new byte[toFill]));
            cacheCleanup.onClose = () -> context.cacheStorage.delete(address);
            insertingInCache.release(); // Notify that we have inserted.
            Exceptions.handleInterrupted(finishInsertingInCache::await); // Block (while holding locks) until notified.
        };

        // Begin a read process.
        // First read must be a storage read.
        val storageRead = rr.next();
        Assert.assertEquals(ReadResultEntryType.Storage, storageRead.getType());
        storageRead.requestContent(TIMEOUT);

        // Copy contents out; this is not affected by our cache insert block.
        byte[] readData1 = storageRead.getContent().join().slice(0, appendLength).getCopy();

        // Wait for the insert callback to be blocked on our latch.
        insertingInCache.await();

        // Continue with the read. We are now expecting a Cache Read. Do it asynchronously (new thread).
        val cacheReadFuture = CompletableFuture.supplyAsync(rr::next, executorService());

        // Notify the cache insert that it's time to release now.
        finishInsertingInCache.release();

        // Wait for the async read to finish and grab its contents.
        val cacheRead = cacheReadFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals(ReadResultEntryType.Cache, cacheRead.getType());
        byte[] readData2 = cacheRead.getContent().join().slice(0, appendLength).getCopy();

        // Validate data was read correctly.
        val readData = BufferView.builder()
                .add(new ByteArraySegment(readData1))
                .add(new ByteArraySegment(readData2))
                .build();
        Assert.assertEquals("Unexpected data written.", allData, readData);
    }

    /**
     * Verifies that any FutureRead that resulted from a partial merge operation is cancelled when the ReadIndex is closed.
     */
    @Test
    public void testMergeFutureReadCancelledOnClose() throws Exception {
        CachePolicy cachePolicy = new CachePolicy(1, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);

        // Create parent segment and one transaction
        long parentId = createSegment(0, context);
        long transactionId = createTransaction(1, context);
        createSegmentsInStorage(context);

        ByteArraySegment writeData = getAppendData(context.metadata.getStreamSegmentMetadata(transactionId).getName(), transactionId, 0, 0);
        RedirectedReadResultEntry entry = (RedirectedReadResultEntry) setupMergeRead(parentId, transactionId, writeData.getCopy(), context);

        // There are a number of async tasks going on here. One of them is in RedirectedReadResultEntry which needs to switch
        // from the first attempt to a second one. Since we have no hook to know when that happens exactly, the only thing
        // we can do is check periodically until that is done.
        TestUtils.await(entry::hasSecondEntrySet, 10, TIMEOUT.toMillis());

        // Close the index.
        context.readIndex.close();

        // Verify the entry is cancelled. Invoke get() since the cancellation is asynchronous so it may not yet have
        // been executed; get() will block until that happens.
        AssertExtensions.assertThrows(
                "Expected entry to have been cancelled upon closing",
                () -> entry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof CancellationException);
    }

    /**
     * Tests a case when a Read Result is sitting on an incomplete {@link ReadResultEntry} that points to a recently
     * merged segment, and the Read Result is closed. In this case. The entry should be failed.
     */
    @Test
    public void testMergeReadResultCancelledOnClose() throws Exception {
        @Cleanup
        TestContext context = new TestContext();

        // Create parent segment and one transaction
        long targetSegmentId = createSegment(0, context);
        long sourceSegmentId = createTransaction(1, context);
        createSegmentsInStorage(context);

        val targetMetadata = context.metadata.getStreamSegmentMetadata(targetSegmentId);
        val sourceMetadata = context.metadata.getStreamSegmentMetadata(sourceSegmentId);

        // Write something to the transaction - only in Storage.
        val txnData = getAppendData(context.metadata.getStreamSegmentMetadata(sourceSegmentId).getName(), sourceSegmentId, 0, 0);
        val transactionWriteHandle = context.storage.openWrite(sourceMetadata.getName()).join();
        context.storage.write(transactionWriteHandle, 0, txnData.getReader(), txnData.getLength(), TIMEOUT).join();

        sourceMetadata.setLength(txnData.getLength());
        sourceMetadata.setStorageLength(txnData.getLength());
        sourceMetadata.markSealed();
        targetMetadata.setLength(sourceMetadata.getLength());
        context.readIndex.beginMerge(targetSegmentId, 0, sourceSegmentId);
        sourceMetadata.markMerged();

        // Setup a Read Result, verify that it is indeed returning a RedirectedReadResultEntry, and immediately close it.
        // Then verify that the entry itself has been cancelled.
        @Cleanup
        val rr = context.readIndex.read(targetSegmentId, 0, txnData.getLength(), TIMEOUT);
        val entry = rr.next();
        Assert.assertTrue(entry instanceof RedirectedReadResultEntry);
        rr.close();
        Assert.assertTrue(entry.getContent().isCancelled());
    }

    /**
     * Tests a case when a Read Result about to fetch a {@link RedirectedReadResultEntry} at the same time as the source
     * {@link StreamSegmentReadIndex} is about to close.
     * Note: this test overlaps with {@link #testMergeReadResultCancelledOnClose()}, but this one is more complex so it
     * is still worth it to keep the other one for simplicity.
     */
    @Test
    public void testMergeReadResultConcurrentCompleteMerge() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        val spiedIndex = Mockito.spy(context.readIndex);

        // Create parent segment and one transaction
        long targetSegmentId = createSegment(0, context);
        long sourceSegmentId = createTransaction(1, context);
        createSegmentsInStorage(context);

        val targetMetadata = context.metadata.getStreamSegmentMetadata(targetSegmentId);
        val sourceMetadata = context.metadata.getStreamSegmentMetadata(sourceSegmentId);

        // Write something to the source segment - only in Storage.
        val sourceData = getAppendData(context.metadata.getStreamSegmentMetadata(sourceSegmentId).getName(), sourceSegmentId, 0, 0);
        val sourceWriteHandle = context.storage.openWrite(sourceMetadata.getName()).join();
        context.storage.write(sourceWriteHandle, 0, sourceData.getReader(), sourceData.getLength(), TIMEOUT).join();

        // Update the source metadata to reflect the Storage situation.
        sourceMetadata.setLength(sourceData.getLength());
        sourceMetadata.setStorageLength(sourceData.getLength());
        sourceMetadata.markSealed();

        // Intercept the ContainerReadIndex.createSegmentIndex to wrap the actual StreamSegmentReadIndex with a spy.
        val spiedIndices = Collections.synchronizedMap(new HashMap<Long, StreamSegmentReadIndex>());
        Mockito.doAnswer(arg1 -> {
            val sm = (SegmentMetadata) arg1.getArgument(1);
            StreamSegmentReadIndex result = (StreamSegmentReadIndex) arg1.callRealMethod();
            if (result == null) {
                spiedIndices.remove(sm.getId());
            } else if (spiedIndices.get(sm.getId()) == null) {
                result = Mockito.spy(result);
                spiedIndices.put(sm.getId(), result);
            }

            return spiedIndices.get(sm.getId());
        }).when(spiedIndex).createSegmentIndex(Mockito.any(ReadIndexConfig.class), Mockito.any(SegmentMetadata.class),
                Mockito.any(CacheStorage.class), Mockito.any(ReadOnlyStorage.class), Mockito.any(ScheduledExecutorService.class), Mockito.anyBoolean());

        // Initiate the merge.
        targetMetadata.setLength(sourceMetadata.getLength());
        spiedIndex.beginMerge(targetSegmentId, 0, sourceSegmentId);
        sourceMetadata.markMerged();

        // Sanity check. Before completeMerge, we should get a RedirectedReadResultEntry.
        @Cleanup
        val rrBeforeComplete = spiedIndex.read(targetSegmentId, 0, sourceData.getLength(), TIMEOUT);
        val reBeforeComplete = rrBeforeComplete.next();
        Assert.assertTrue(reBeforeComplete instanceof RedirectedReadResultEntry);
        rrBeforeComplete.close();
        Assert.assertTrue(reBeforeComplete.getContent().isCancelled());

        // Intercept the source segment's index getSingleReadResultEntry to "concurrently" complete its merger.
        Mockito.doAnswer(arg2 -> {
            // Simulate Storage merger.
            val targetWriteHandle = context.storage.openWrite(targetMetadata.getName()).join();
            context.storage.write(targetWriteHandle, 0, sourceData.getReader(), sourceData.getLength(), TIMEOUT).join();

            // Update metadata.
            sourceMetadata.markDeleted();
            targetMetadata.setStorageLength(sourceMetadata.getStorageLength());
            spiedIndex.completeMerge(targetSegmentId, sourceSegmentId);

            return arg2.callRealMethod();
        }).when(spiedIndices.get(sourceSegmentId)).getSingleReadResultEntry(Mockito.anyLong(), Mockito.anyInt(), Mockito.anyBoolean());

        // Setup a Read Result, verify that it is indeed returning a RedirectedReadResultEntry, and immediately close it.
        // Then verify that the entry itself has been cancelled.
        @Cleanup
        val rrAfterComplete = spiedIndex.read(targetSegmentId, 0, sourceData.getLength(), TIMEOUT);
        val reAfterComplete = rrAfterComplete.next();
        Assert.assertTrue(reAfterComplete instanceof StorageReadResultEntry);

        reAfterComplete.requestContent(TIMEOUT);
        reAfterComplete.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Sets up a partial-merge Future Read for the two segments:
     * 1. Writes some data to transactionId (both Read Index and Storage).
     * 2. Calls beginMerge(parentId, transactionId)
     * 3. Executes a Storage concat (transactionId -> parentId)
     * 4. Clears the cache and returns a ReadResultEntry that requests data at the first offset of the merged transaction.
     * NOTE: this does not call completeMerge().
     */
    private ReadResultEntry setupMergeRead(long parentId, long transactionId, byte[] txnData, TestContext context) throws Exception {
        int mergeOffset = 1;
        UpdateableSegmentMetadata parentMetadata = context.metadata.getStreamSegmentMetadata(parentId);
        UpdateableSegmentMetadata transactionMetadata = context.metadata.getStreamSegmentMetadata(transactionId);
        appendSingleWrite(parentId, new ByteArraySegment(new byte[mergeOffset]), context);
        context.storage.openWrite(parentMetadata.getName())
                       .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[mergeOffset]), mergeOffset, TIMEOUT)).join();

        // Write something to the transaction, and make sure it also makes its way to Storage.
        appendSingleWrite(transactionId, new ByteArraySegment(txnData), context);
        val transactionWriteHandle = context.storage.openWrite(transactionMetadata.getName()).join();
        context.storage.write(transactionWriteHandle, 0, new ByteArrayInputStream(txnData), txnData.length, TIMEOUT).join();
        transactionMetadata.setStorageLength(transactionMetadata.getLength());

        // Seal & Begin-merge the transaction (do not seal in storage).
        transactionMetadata.markSealed();
        parentMetadata.setLength(transactionMetadata.getLength() + mergeOffset);
        context.readIndex.beginMerge(parentId, mergeOffset, transactionId);
        transactionMetadata.markMerged();

        // Clear the cache.
        context.cacheManager.applyCachePolicy();

        // Issue read from the parent and fetch the first entry (there should only be one).
        ReadResult rr = context.readIndex.read(parentId, mergeOffset, txnData.length, TIMEOUT);
        Assert.assertTrue("Parent Segment read indicates no data available.", rr.hasNext());
        ReadResultEntry entry = rr.next();
        Assert.assertEquals("Unexpected offset for read result entry.", mergeOffset, entry.getStreamSegmentOffset());
        Assert.assertEquals("Served read result entry is not from storage.", ReadResultEntryType.Storage, entry.getType());

        // Merge the transaction in storage. Do not complete-merge it.
        transactionMetadata.markSealed();
        transactionMetadata.markSealedInStorage();
        transactionMetadata.markDeleted();
        context.storage.seal(transactionWriteHandle, TIMEOUT).join();
        val parentWriteHandle = context.storage.openWrite(parentMetadata.getName()).join();
        context.storage.concat(parentWriteHandle, 1, transactionWriteHandle.getSegmentName(), TIMEOUT).join();
        parentMetadata.setStorageLength(parentMetadata.getLength());

        // Attempt to extract data from the read.
        entry.requestContent(TIMEOUT);
        Assert.assertFalse("Not expecting the read to be completed.", entry.getContent().isDone());
        return entry;
    }

    /**
     * Tests a scenario where a call to {@link StreamSegmentReadIndex#append} executes concurrently with a Cache Manager
     * eviction. In particular, this tests the following scenario:
     * - We have a Cache Entry E1 with Generation G1, and its entire contents is in Storage.
     * - E1 maps to the end of the Segment.
     * - We initiate an append A1, which will update the contents of E1.
     * - The Cache Manager executes.
     * - E1 would be eligible for eviction prior to the Cache Manager run, but not after.
     * - We need to validate that E1 is not evicted and that A2 is immediately available for reading, and so is the data
     * prior to it.
     */
    @Test
    public void testConcurrentEvictAppend() throws Exception {
        val rnd = new Random(0);
        CachePolicy cachePolicy = new CachePolicy(1, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);
        final int blockSize = context.cacheStorage.getBlockAlignment();
        context.cacheStorage.appendReturnBlocker = null; // Not blocking anything now.

        // Create segment and make one append, less than the cache block size.
        long segmentId = createSegment(0, context);
        val segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        createSegmentsInStorage(context);
        val append1 = new ByteArraySegment(new byte[blockSize / 2]);
        rnd.nextBytes(append1.array());
        segmentMetadata.setLength(append1.getLength());
        context.readIndex.append(segmentId, 0, append1);
        segmentMetadata.setStorageLength(append1.getLength());

        // Block further cache appends. This will give us time to execute cache eviction.
        context.cacheStorage.appendReturnBlocker = new ReusableLatch();
        context.cacheStorage.appendComplete = new ReusableLatch();

        // Initiate append 2. The append should be written to the Cache Storage, but its invocation should block until
        // we release the above latch.
        val append2 = new ByteArraySegment(new byte[blockSize - append1.getLength() - 1]);
        rnd.nextBytes(append2.array());
        segmentMetadata.setLength(append1.getLength() + append2.getLength());
        val append2Future = CompletableFuture.runAsync(() -> {
            try {
                context.readIndex.append(segmentId, append1.getLength(), append2);
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, executorService());
        context.cacheStorage.appendComplete.await();

        // Execute cache eviction. Append 2 is suspended at the point when we return from the cache call. This is the
        // closest we can come to simulating eviction racing with appending.
        val evictionFuture = CompletableFuture.supplyAsync(context.cacheManager::applyCachePolicy, this.executorService());

        // We want to verify that the cache eviction is blocked on the append - they should not run concurrently. The only
        // "elegant" way of verifying this is by waiting a short amount of time and checking that it didn't execute.
        AssertExtensions.assertThrows(
                "Expecting cache eviction to block.",
                () -> evictionFuture.get(SHORT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ex instanceof TimeoutException);

        // Release the second append, which should not error out.
        context.cacheStorage.appendReturnBlocker.release();
        append2Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify that no cache eviction happened.
        boolean evicted = evictionFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertFalse("Not expected a cache eviction to happen.", evicted);

        // Validate data read back is as expected.
        // readDirect() should return an InputStream for the second append range.
        val readData = context.readIndex.readDirect(segmentId, append1.getLength(), append2.getLength());
        Assert.assertNotNull("Expected append2 to be read back.", readData);
        AssertExtensions.assertStreamEquals("Unexpected data read back from append2.", append2.getReader(), readData.getReader(), append2.getLength());

        // Reading the whole segment should work well too.
        byte[] allData = new byte[append1.getLength() + append2.getLength()];
        context.readIndex.read(segmentId, 0, allData.length, TIMEOUT).readRemaining(allData, TIMEOUT);
        AssertExtensions.assertArrayEquals("Unexpected data read back from segment.", append1.array(), 0, allData, 0, append1.getLength());
        AssertExtensions.assertArrayEquals("Unexpected data read back from segment.", append2.array(), 0, allData, append1.getLength(), append2.getLength());
    }

    /**
     * Tests a deadlock-prone scenario involving multiple Storage read requests from multiple segments, all hitting a
     * CacheFullException while trying to process.
     *
     * Steps:
     * 1. Segment 1: Storage Read Complete -> Ack -> Insert in Index -> Acquire (ReadIndex1.Lock[Thread1]) -> Insert in Cache [Request1]
     * 2. Segment 2: Storage Read Complete -> Ack -> Insert in Index -> Acquire (ReadIndex2.Lock[Thread2]) -> Insert in Cache [Request2]
     * 3. Cache is full. Deadlock occurs if:
     * 3.1. [Request1] invokes Cache Eviction, which wants to acquire ReadIndex2.Lock, but it is owned by Thread2.
     * 3.2. [Request2] invokes Cache Eviction, which wants to acquire ReadIndex1.Lock, but it is owned by Thread1.
     *
     * This test verifies that no deadlock occurs by simulating this exact scenario. It verifies that all requests eventually
     * complete successfully (as the deadlock victim will back off and retry).
     */
    @Test
    public void testCacheFullDeadlock() throws Exception {
        val maxCacheSize = 2 * 1024 * 1024; // This is the actual cache size, even if we set a lower value than this.
        val append1Size = (int) (0.75 * maxCacheSize); // Fill up most of the cache - this is also a candidate for eviction.
        val append2Size = 1; // Dummy append - need to register the read index as a cache client.
        val segmentSize = maxCacheSize + 1;

        val config = ReadIndexConfig
                .builder()
                .with(ReadIndexConfig.MEMORY_READ_MIN_LENGTH, 0) // Default: Off (we have a special test for this).
                .with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, maxCacheSize)
                .build();
        CachePolicy cachePolicy = new CachePolicy(maxCacheSize, Duration.ZERO, Duration.ofMillis(1));
        @Cleanup
        TestContext context = new TestContext(config, cachePolicy, maxCacheSize);

        // Block the first insert (this will be from segment 1
        val append1Address = new AtomicInteger(0);
        context.cacheStorage.insertCallback = a -> append1Address.compareAndSet(0, a);
        val segment1Delete = new ReusableLatch();
        context.cacheStorage.beforeDelete = deleteAddress -> {
            if (deleteAddress == append1Address.get()) {
                // Block eviction of the first segment 1 data (just the first; we want the rest to go through).
                Exceptions.handleInterrupted(segment1Delete::await);
            }
        };

        // Create segments and make each of them slightly bigger than the cache capacity.
        long segment1Id = createSegment(0, context);
        long segment2Id = createSegment(1, context);
        val segment1Metadata = context.metadata.getStreamSegmentMetadata(segment1Id);
        val segment2Metadata = context.metadata.getStreamSegmentMetadata(segment2Id);
        segment1Metadata.setLength(segmentSize);
        segment1Metadata.setStorageLength(segmentSize);
        segment2Metadata.setLength(segmentSize);
        segment2Metadata.setStorageLength(segmentSize);
        createSegmentsInStorage(context);

        context.storage.openWrite(segment1Metadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[segmentSize]), segmentSize, TIMEOUT))
                .join();
        context.storage.openWrite(segment2Metadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[segmentSize]), segmentSize, TIMEOUT))
                .join();

        // Write some data into the cache. This will become a candidate for eviction at the next step.
        context.readIndex.append(segment1Id, 0, new ByteArraySegment(new byte[append1Size]));

        // Write some data into Segment 2's index. This will have no effect on the cache, but we will register it with the Cache Manager.
        context.readIndex.append(segment2Id, 0, new ByteArraySegment(new byte[append2Size]));

        // Initiate the first Storage read. This should exceed the max cache size, so it should trigger the cleanup.
        val segment1Read = context.readIndex.read(segment1Id, append1Size, segmentSize - append1Size, TIMEOUT).next();
        Assert.assertEquals(ReadResultEntryType.Storage, segment1Read.getType());
        segment1Read.requestContent(TIMEOUT);
        segment1Read.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // This one should complete right away.

        // Wait for the delete callback to be latched.
        TestUtils.await(() -> segment1Delete.getQueueLength() > 0, 10, TIMEOUT.toMillis());

        // Initiate the second Storage read. This should also exceed the max cache size and trigger another cleanup, but
        // (most importantly) on a different thread.
        val segment2Read = context.readIndex.read(segment2Id, append2Size, segmentSize - append2Size, TIMEOUT).next();
        Assert.assertEquals(ReadResultEntryType.Storage, segment2Read.getType());
        segment2Read.requestContent(TIMEOUT);
        segment2Read.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS); // As with the first one, this should complete right away.

        // We use yet another thread to validate that no deadlock occurs. This should briefly block on Segment 2's Read index's
        // lock, but it should be unblocked when we release that (next step).
        val append2Future = CompletableFuture.runAsync(() -> {
            try {
                context.readIndex.append(segment2Id, append2Size, new ByteArraySegment(new byte[append1Size]));
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        }, executorService());

        // Release the delete blocker. If all goes well, all the other operations should be unblocked at this point.
        segment1Delete.release();
        append2Future.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Tests the ability of the Read Index to handle "Essential-Only" cache mode, where only cache entries that are not
     * yet persisted to Storage may be added to the cache.
     */
    @Test
    public void testCacheEssentialOnlyMode() throws Exception {
        val rnd = new Random(0);
        val appendSize = 4 * 1024; // Cache block size.
        val segmentLength = 10 * appendSize;
        // Setup a cache policy that will keep at most 4 blocks in the cache, and enter essential mode after 4 blocks too
        // NOTE: blocks includes the metadata block (internal to the cache), so usable blocks is 3.
        CachePolicy cachePolicy = new CachePolicy(segmentLength, 0.3, 0.4, Duration.ofHours(1000), Duration.ofSeconds(1));
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG, cachePolicy);
        context.cacheStorage.appendReturnBlocker = null; // Not blocking anything now.

        // Create segment, generate some content for it, setup its metadata and write 40% of it to Storage.
        long segmentId = createSegment(0, context);
        val segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
        createSegmentsInStorage(context);
        val segmentData = new byte[segmentLength];
        rnd.nextBytes(segmentData);
        val part1 = new ByteArraySegment(segmentData, 0, appendSize);
        val part2 = new ByteArraySegment(segmentData, appendSize, appendSize);
        val part3 = new ByteArraySegment(segmentData, 2 * appendSize, appendSize);
        val part4 = new ByteArraySegment(segmentData, 3 * appendSize, appendSize);
        val part5 = new ByteArraySegment(segmentData, 4 * appendSize, appendSize);
        segmentMetadata.setLength(segmentLength);
        segmentMetadata.setStorageLength(part1.getLength() + part2.getLength());
        context.storage.openWrite(segmentMetadata.getName())
                .thenCompose(h -> context.storage.write(h, 0, new ByteArrayInputStream(segmentData),
                        (int) segmentMetadata.getStorageLength(), TIMEOUT)).join();

        val insertCount = new AtomicInteger(0);
        val storageReadCount = new AtomicInteger(0);
        context.cacheStorage.insertCallback = address -> insertCount.incrementAndGet();
        context.storage.setReadInterceptor((segment, wrappedStorage) -> storageReadCount.incrementAndGet());

        // Helper for reading a segment part.
        BiConsumer<Long, BufferView> readPart = (partOffset, partContents) -> {
            try {
                @Cleanup
                val rr = context.readIndex.read(segmentId, partOffset, partContents.getLength(), TIMEOUT);
                val readData = rr.readRemaining(partContents.getLength(), TIMEOUT);
                Assert.assertEquals(partContents, BufferView.wrap(readData));
            } catch (Exception ex) {
                throw new CompletionException(ex);
            }
        };

        // Read parts 1 and 2 (separately). They should be cached as individual entries.
        readPart.accept(0L, part1);
        Assert.assertEquals(1, storageReadCount.get());

        // Cache insertion is done async. Need to wait until we write
        AssertExtensions.assertEventuallyEquals(1, insertCount::get, TIMEOUT.toMillis());
        AssertExtensions.assertEventuallyEquals(1, context.readIndex.getIndex(segmentId).getSummary()::size, TIMEOUT.toMillis());

        boolean evicted = context.cacheManager.applyCachePolicy(); // No eviction, but increase generation.
        Assert.assertFalse("Not expected an eviction now.", evicted);

        readPart.accept((long) part1.getLength(), part2);

        // We expect 2 storage reads and also 2 cache inserts.
        Assert.assertEquals(2, storageReadCount.get());
        AssertExtensions.assertEventuallyEquals(2, insertCount::get, TIMEOUT.toMillis()); // This one is done asynchronously.
        AssertExtensions.assertEventuallyEquals(2, context.readIndex.getIndex(segmentId).getSummary()::size, TIMEOUT.toMillis());

        evicted = context.cacheManager.applyCachePolicy(); // No eviction, but increase generation.
        Assert.assertFalse("Not expected an eviction now.", evicted);

        // Append parts 3, 4 and 5.
        context.readIndex.append(segmentId, segmentMetadata.getStorageLength(), part3);
        Assert.assertEquals(3, insertCount.get()); // This insertion is done synchronously.
        evicted = context.cacheManager.applyCachePolicy(); // Eviction (part 1) + increase generation.
        Assert.assertTrue("Expected an eviction after writing 3 blocks.", evicted);

        context.readIndex.append(segmentId, segmentMetadata.getStorageLength() + part3.getLength(), part4);
        Assert.assertEquals("Expected an insertion for appends even in essential-only mode.", 4, insertCount.get());
        evicted = context.cacheManager.applyCachePolicy(); // Eviction (part 2) + increase generation.
        Assert.assertTrue("Expected an eviction after writing 4 blocks.", evicted);

        context.readIndex.append(segmentId, segmentMetadata.getStorageLength() + part3.getLength() + part4.getLength(), part5);
        Assert.assertEquals("Expected an insertion for appends even in essential-only mode.", 5, insertCount.get());
        evicted = context.cacheManager.applyCachePolicy(); // Nothing to evict.
        Assert.assertFalse("Not expecting an eviction after writing 5 blocks.", evicted);
        Assert.assertTrue("Expected to be in essential-only mode after pinning 3 blocks.", context.cacheManager.isEssentialEntriesOnly());

        // Verify that re-reading parts 1 and 2 results in no cache inserts.
        insertCount.set(0);
        storageReadCount.set(0);
        int expectedReadCount = 0;
        for (int i = 0; i < 5; i++) {
            readPart.accept(0L, part1);
            readPart.accept((long) part1.getLength(), part2);
            expectedReadCount += 2;
        }

        Assert.assertTrue("Not expected to have exited essential-only mode.", context.cacheManager.isEssentialEntriesOnly());
        Assert.assertEquals("Unexpected number of storage reads in essential-only mode.", expectedReadCount, storageReadCount.get());
        Assert.assertEquals("Unexpected number of cache inserts in essential-only mode.", 0, insertCount.get());
    }

    //endregion

    //region Helpers

    private void checkOffsets(List<SegmentOffset> removedKeys, long segmentId, int startIndex, int count, int startOffset, int stepIncrease) {
        int expectedStartOffset = startOffset;
        for (int i = 0; i < count; i++) {
            int listIndex = startIndex + i;
            SegmentOffset currentEntry = removedKeys.get(startIndex + i);
            Assert.assertEquals(
                    String.format("Unexpected CacheKey.SegmentOffset at index %d for SegmentId %d.", listIndex, segmentId),
                    expectedStartOffset,
                    currentEntry.offset);
            expectedStartOffset += stepIncrease;
        }
    }

    private void createSegmentsInStorage(TestContext context) {
        for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
            SegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
            context.storage.create(sm.getName(), TIMEOUT).join();
        }
    }

    private void appendData(Collection<Long> segmentIds, Map<Long, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        appendData(segmentIds, segmentContents, context, null);
    }

    private void appendData(Collection<Long> segmentIds, Map<Long, ByteArrayOutputStream> segmentContents, TestContext context, Runnable callback) throws Exception {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : segmentIds) {
                UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);
                ByteArraySegment data = getAppendData(segmentMetadata.getName(), segmentId, i, writeId);
                writeId++;

                appendSingleWrite(segmentId, data, context);
                recordAppend(segmentId, data, segmentContents);
                if (callback != null) {
                    callback.run();
                }
            }
        }
    }

    private void appendSingleWrite(long segmentId, ByteArraySegment data, TestContext context) throws Exception {
        UpdateableSegmentMetadata segmentMetadata = context.metadata.getStreamSegmentMetadata(segmentId);

        // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
        long offset = segmentMetadata.getLength();
        segmentMetadata.setLength(offset + data.getLength());
        context.readIndex.append(segmentId, offset, data);
    }

    private void appendDataInStorage(TestContext context, HashMap<Long, ByteArrayOutputStream> segmentContents) throws IOException {
        int writeId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (long segmentId : context.metadata.getAllStreamSegmentIds()) {
                UpdateableSegmentMetadata sm = context.metadata.getStreamSegmentMetadata(segmentId);
                ByteArraySegment data = getAppendData(sm.getName(), segmentId, i, writeId);
                writeId++;

                // Make sure we increase the Length prior to appending; the ReadIndex checks for this.
                long offset = context.storage.getStreamSegmentInfo(sm.getName(), TIMEOUT).join().getLength();
                val handle = context.storage.openWrite(sm.getName()).join();
                context.storage.write(handle, offset, data.getReader(), data.getLength(), TIMEOUT).join();

                // Update metadata appropriately.
                sm.setStorageLength(offset + data.getLength());
                if (sm.getStorageLength() > sm.getLength()) {
                    sm.setLength(sm.getStorageLength());
                }

                recordAppend(segmentId, data, segmentContents);
            }
        }
    }

    private ByteArraySegment getAppendData(String segmentName, long segmentId, int segmentAppendSeq, int writeId) {
        return new ByteArraySegment(String.format("SegmentName=%s,SegmentId=_%d,AppendSeq=%d,WriteId=%d", segmentName, segmentId, segmentAppendSeq, writeId).getBytes());
    }

    private void completeMergeTransactions(HashMap<Long, ArrayList<Long>> transactionsBySegment, TestContext context) throws Exception {
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
        if (segmentContents.containsKey(transactionId)) {
            segmentContents.get(parentMetadata.getId()).write(segmentContents.get(transactionId).toByteArray());
            segmentContents.remove(transactionId);
        }
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
                AssertExtensions.assertSuppliedFutureThrows(
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

                // Since this is a non-sealed segment, we only expect Cache or Storage read result
                // entries.
                Assert.assertTrue(testId + ": Unexpected type of ReadResultEntry for non-sealed segment " + segmentId,
                                  readEntry.getType() == ReadResultEntryType.Cache || readEntry.getType() == ReadResultEntryType.Storage);
                if (readEntry.getType() == ReadResultEntryType.Cache) {
                    Assert.assertTrue(testId + ": getContent() did not return a completed future (ReadResultEntryType.Cache) for segment" + segmentId,
                                      readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                } else if (readEntry.getType() == ReadResultEntryType.Storage) {
                    Assert.assertFalse(testId + ": getContent() did not return a non-completed future (ReadResultEntryType.Storage) for segment" + segmentId,
                                       readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                }

                // Request content, in case it wasn't returned yet.
                readEntry.requestContent(TIMEOUT);
                BufferView readEntryContents = readEntry.getContent().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                AssertExtensions.assertGreaterThan(testId + ": getContent() returned an empty result entry for segment " + segmentId, 0, readEntryContents.getLength());

                byte[] actualData = readEntryContents.getCopy();
                AssertExtensions.assertArrayEquals(
                    testId + ": Unexpected data read from segment " + segmentId + " at offset " + expectedCurrentOffset,
                    expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());

                expectedCurrentOffset += readEntryContents.getLength();
                if (readEntry.getType() == ReadResultEntryType.Storage) {
                    AssertExtensions.assertLessThanOrEqual("Misaligned storage read.",
                            context.maxExpectedStorageReadLength, readEntryContents.getLength());
                }
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
            byte[] actualData = context.readIndex.readDirect(segmentId, startOffset, readLength).getCopy();
            AssertExtensions.assertArrayEquals("Unexpected data read.", expectedData, (int) startOffset, actualData, 0, actualData.length);
        }
    }

    private <T> void recordAppend(T segmentIdentifier, ByteArraySegment data, Map<T, ByteArrayOutputStream> segmentContents) throws IOException {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentIdentifier, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentIdentifier, contents);
        }

        data.copyTo(contents);
    }

    private ArrayList<Long> createSegments(TestContext context) {
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            segmentIds.add(createSegment(i, context));
        }

        return segmentIds;
    }

    private long createSegment(long id, TestContext context) {
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

            for (int i = 0; i < transactionsPerSegment; i++) {
                segmentTransactions.add(createTransaction(transactionId, context));
                transactionId++;
            }
        }

        return transactions;
    }

    private long createTransaction(long transactionId, TestContext context) {
        String transactionName = "Txn" + UUID.randomUUID().toString();
        context.metadata.mapStreamSegmentId(transactionName, transactionId);
        initializeSegment(transactionId, context);
        return transactionId;
    }

    private String getSegmentName(long id) {
        return "Segment_" + id;
    }

    private void initializeSegment(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.metadata.getStreamSegmentMetadata(segmentId);
        metadata.setLength(0);
        metadata.setStorageLength(0);
    }

    private void markInactive(long segmentId, TestContext context) {
        ((StreamSegmentMetadata) context.metadata.getStreamSegmentMetadata(segmentId)).markInactive();
    }

    private void evict(long segmentId, TestContext context) {
        val candidates = Collections.singleton((SegmentMetadata) context.metadata.getStreamSegmentMetadata(segmentId));
        val em = (EvictableMetadata) context.metadata;
        val sn = context.metadata.getOperationSequenceNumber() + 1;
        context.metadata.removeTruncationMarkers(sn);
        em.cleanup(candidates, sn);
    }

    private boolean isExpectedAfterSealed(Throwable ex) {
        return ex instanceof StreamSegmentSealedException
                || ex instanceof ObjectClosedException
                || ex instanceof CancellationException;
    }

    //endregion

    //region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata metadata;
        final ContainerReadIndex readIndex;
        final TestCacheManager cacheManager;
        final TestCacheStorage cacheStorage;
        final TestStorage storage;
        final int maxExpectedStorageReadLength;

        TestContext() {
            this(DEFAULT_CONFIG, CachePolicy.INFINITE);
        }

        TestContext(ReadIndexConfig readIndexConfig, CachePolicy cachePolicy) {
            this(readIndexConfig, cachePolicy, Integer.MAX_VALUE);
        }

        TestContext(ReadIndexConfig readIndexConfig, CachePolicy cachePolicy, int actualCacheSize) {
            this.cacheStorage = new TestCacheStorage(Math.min(Integer.MAX_VALUE, actualCacheSize));
            this.metadata = new MetadataBuilder(CONTAINER_ID).build();
            this.storage = new TestStorage(new InMemoryStorage(), executorService());
            this.storage.initialize(1);
            this.cacheManager = new TestCacheManager(cachePolicy, this.cacheStorage, executorService());
            this.readIndex = new ContainerReadIndex(readIndexConfig, this.metadata, this.storage, this.cacheManager, executorService());
            this.maxExpectedStorageReadLength = calculateMaxStorageReadLength();
        }

        private int calculateMaxStorageReadLength() {
            int a = this.cacheManager.getCacheStorage().getBlockAlignment();
            return DEFAULT_CONFIG.getStorageReadAlignment() +
                    (DEFAULT_CONFIG.getStorageReadAlignment() % a == 0 ? 0 : a - DEFAULT_CONFIG.getStorageReadAlignment() % a);
        }

        @Override
        @SneakyThrows
        public void close() {
            this.readIndex.close();
            AssertExtensions.assertEventuallyEquals("MEMORY LEAK: Read Index did not delete all CacheStorage entries after closing.",
                    0L, () -> this.cacheStorage.getState().getStoredBytes(), 10, TIMEOUT.toMillis());
            this.storage.close();
            this.cacheManager.close();
            this.cacheStorage.close();
        }
    }

    //endregion

    //region TestCacheStorage

    private static class TestCacheStorage extends DirectMemoryCache {
        Consumer<Integer> beforeDelete;
        Runnable beforeInsert;
        Consumer<Integer> insertCallback;
        Consumer<Integer> deleteCallback;
        boolean disableAppends;
        boolean usedBytesSameAsStoredBytes;
        ReusableLatch appendComplete; // If set, will invoke ReusableLatch.release() when append is done (before appendReturnBlocker).
        ReusableLatch appendReturnBlocker; // If set, blocks append calls from returning AFTER the append has been executed.
        @Getter
        int maxEntryLength;

        TestCacheStorage(long maxSizeBytes) {
            super(maxSizeBytes);
            this.maxEntryLength = super.getMaxEntryLength();
        }

        @Override
        public int getAppendableLength(int currentLength) {
            return this.disableAppends ? 0 : super.getAppendableLength(currentLength);
        }

        @Override
        public int insert(BufferView data) {
            Runnable beforeInsert = this.beforeInsert;
            if (beforeInsert != null) {
                beforeInsert.run();
            }
            int r = super.insert(data);
            Consumer<Integer> afterInsert = this.insertCallback;
            if (afterInsert != null) {
                afterInsert.accept(r);
            }

            return r;
        }

        @Override
        public int append(int address, int expectedLength, BufferView data) {
            if (this.disableAppends) {
                return 0;
            }

            int result = super.append(address, expectedLength, data);

            ReusableLatch complete = this.appendComplete;
            if (complete != null) {
                complete.release();
            }

            // Blocker hits after the append is done.
            ReusableLatch blocker = this.appendReturnBlocker;
            if (blocker != null) {
                blocker.awaitUninterruptibly();
            }

            return result;
        }

        @Override
        public void delete(int address) {
            Consumer<Integer> beforeDelete = this.beforeDelete;
            if (beforeDelete != null) {
                beforeDelete.accept(address);
            }

            super.delete(address);
            Consumer<Integer> callback = this.deleteCallback;
            if (callback != null) {
                callback.accept(address);
            }
        }

        @Override
        public CacheState getState() {
            val s = super.getState();
            if (this.usedBytesSameAsStoredBytes) {
                return new CacheState(s.getStoredBytes(), s.getStoredBytes(), s.getReservedBytes(), s.getAllocatedBytes(), s.getMaxBytes());
            } else {
                return s;
            }
        }
    }

    //endregion

    //region SegmentOffset

    @RequiredArgsConstructor
    private static class SegmentOffset {
        final long segmentId;
        final long offset;
    }

    //endregion

    @FunctionalInterface
    private interface BiConsumerWithException<T, U> {
        void accept(T var1, U var2) throws Exception;
    }

    /**
     * Convenience class that helps us record an action for later cleanup (helps simplify the code by allowing the use
     * of {@code @Cleanup} instead of an ugly {@code try-catch} block.
     */
    private static class AutoCloseObject implements AutoCloseable {
        private Runnable onClose;

        @Override
        public void close() {
            val c = this.onClose;
            if (c != null) {
                c.run();
            }
        }
    }
}
