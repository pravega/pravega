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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.io.StreamHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentMergedException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.MetadataRepository;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.StreamSegmentNameUtils;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.logs.DurableLogConfig;
import com.emc.pravega.service.server.logs.DurableLogFactory;
import com.emc.pravega.service.server.mocks.InMemoryCacheFactory;
import com.emc.pravega.service.server.mocks.InMemoryMetadataRepository;
import com.emc.pravega.service.server.reading.AsyncReadResultEntryHandler;
import com.emc.pravega.service.server.reading.AsyncReadResultProcessor;
import com.emc.pravega.service.server.reading.ContainerReadIndexFactory;
import com.emc.pravega.service.server.reading.ReadIndexConfig;
import com.emc.pravega.service.server.writer.StorageWriterFactory;
import com.emc.pravega.service.server.writer.WriterConfig;
import com.emc.pravega.service.storage.CacheFactory;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import com.emc.pravega.service.storage.mocks.InMemoryStorageFactory;
import com.emc.pravega.testcommon.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tests for StreamSegmentContainer class.
 * These are not really unit tests. They are more like integration/end-to-end tests, since they test a real StreamSegmentContainer
 * using a real DurableLog, real ReadIndex and real LogSynchronizer(TBD) - but all against in-memory mocks of Storage and
 * DurableDataLog.
 */
public class StreamSegmentContainerTests {
    private static final int SEGMENT_COUNT = 200;
    private static final int BATCHES_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int CLIENT_COUNT = 10;
    private static final int CONTAINER_ID = 1234567;
    private static final int THREAD_POOL_SIZE = 50;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    // Create checkpoints every 100 operations or after 10MB have been written, but under no circumstance less frequently than 10 ops.
    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = ConfigHelpers.createDurableLogConfig(10, 100, 10 * 1024 * 1024);
    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ConfigHelpers.createReadIndexConfig(100, 1024);
    private static final WriterConfig DEFAULT_WRITER_CONFIG = ConfigHelpers.createWriterConfig(Integer.MAX_VALUE, Long.MAX_VALUE);

    /**
     * Tests the createSegment, append, read, getSegmentInfo, getLastAppendContext.
     */
    @Test
    public void testSegmentRegularOperations() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, AppendContext> lastAppendContexts = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        int appendId = 0;
        ArrayList<UUID> clients = createClients();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                AppendContext appendContext = new AppendContext(clients.get(appendId % clients.size()), i);
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, appendContext, TIMEOUT));

                lastAppendContexts.put(getAppendContextKey(segmentName, appendContext.getClientId()), appendContext);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
                appendId++;
            }
        }

        FutureHelpers.allOf(appendFutures).join();

        // 3. getSegmentInfo & getLastAppendContext
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            long expectedLength = lengths.get(segmentName);

            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
            Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());

            for (UUID clientId : clients) {
                AppendContext actualContext = context.container.getLastAppendContext(segmentName, clientId, TIMEOUT).join();
                AppendContext expectedContext = lastAppendContexts.get(getAppendContextKey(segmentName, clientId));
                Assert.assertEquals("Unexpected return value from getLastAppendContext for segment " + segmentName, expectedContext, actualContext);
            }
        }

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the seal operation on StreamSegments. Also tests the behavior of Reads (non-tailing) when encountering
     * the end of a sealed StreamSegment.
     */
    @Test
    public void testSegmentSeal() throws Exception {
        final int appendsPerSegment = 1;
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();

        for (int i = 0; i < appendsPerSegment; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
            }
        }

        FutureHelpers.allOf(appendFutures).join();

        // 3. Seal first half of segments.
        ArrayList<CompletableFuture<Long>> sealFutures = new ArrayList<>();
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            sealFutures.add(context.container.sealStreamSegment(segmentNames.get(i), TIMEOUT));
        }

        FutureHelpers.allOf(sealFutures).join();

        // Check that the segments were properly sealed.
        for (int i = 0; i < segmentNames.size(); i++) {
            String segmentName = segmentNames.get(i);
            boolean expectedSealed = i < segmentNames.size() / 2;
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            if (expectedSealed) {
                Assert.assertTrue("Segment is not sealed when it should be " + segmentName, sp.isSealed());
                Assert.assertEquals("Unexpected result from seal() future for segment " + segmentName, sp.getLength(), (long) sealFutures.get(i).join());
                AssertExtensions.assertThrows(
                        "Container allowed appending to a sealed segment " + segmentName,
                        context.container.append(segmentName, "foo".getBytes(), new AppendContext(UUID.randomUUID(), Integer.MAX_VALUE), TIMEOUT)::join,
                        ex -> ex instanceof StreamSegmentSealedException);
            } else {
                Assert.assertFalse("Segment is sealed when it shouldn't be " + segmentName, sp.isSealed());

                // Verify we can still append to these segments.
                context.container.append(segmentName, "foo".getBytes(), new AppendContext(UUID.randomUUID(), Integer.MAX_VALUE), TIMEOUT).join();
            }
        }

        // 4. Reads (regular reads, not tail reads, and only for the sealed segments).
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            String segmentName = segmentNames.get(i);
            long segmentLength = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();

            // Read starting 1 byte from the end - make sure it wont hang at the end by turning into a future read.
            final int totalReadLength = 1;
            long expectedCurrentOffset = segmentLength - totalReadLength;
            @Cleanup
            ReadResult readResult = context.container.read(segmentName, expectedCurrentOffset, Integer.MAX_VALUE, TIMEOUT).join();

            int readLength = 0;
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                if (readEntry.getStreamSegmentOffset() >= segmentLength) {
                    Assert.assertEquals("Unexpected value for isEndOfStreamSegment when reaching the end of sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    AssertExtensions.assertThrows(
                            "ReadResultEntry.getContent() returned a result when reached the end of sealed segment " + segmentName,
                            () -> readEntry.getContent().join(),
                            ex -> ex instanceof IllegalStateException);
                } else {
                    Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment before reaching end of sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());
                    Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                    ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                    expectedCurrentOffset += readEntryContents.getLength();
                    readLength += readEntryContents.getLength();
                }
            }

            Assert.assertEquals("Unexpected number of bytes read.", totalReadLength, readLength);
            Assert.assertTrue("ReadResult was not closed when reaching the end of sealed segment" + segmentName, readResult.isClosed());
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the behavior of various operations when the StreamSegment does not exist.
     */
    @Test
    public void testInexistentSegment() {
        final String segmentName = "foo";
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        AssertExtensions.assertThrows(
                "getStreamSegmentInfo did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.getStreamSegmentInfo(segmentName, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "append did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.append(segmentName, "foo".getBytes(), new AppendContext(UUID.randomUUID(), 0), TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "getLastAppendContext did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.getLastAppendContext(segmentName, UUID.randomUUID(), TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "read did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.read(segmentName, 0, 1, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);
    }

    /**
     * Tests the ability to delete StreamSegments.
     */
    @Test
    public void testSegmentDelete() {
        final int appendsPerSegment = 1;
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> batchesBySegment = createBatches(segmentNames, context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();

        for (int i = 0; i < appendsPerSegment; i++) {
            for (String segmentName : segmentNames) {
                appendFutures.add(context.container.append(segmentName, getAppendData(segmentName, i), new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                for (String batchName : batchesBySegment.get(segmentName)) {
                    appendFutures.add(context.container.append(batchName, getAppendData(batchName, i), new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                }
            }
        }

        FutureHelpers.allOf(appendFutures).join();

        // 3. Delete the first half of the segments.
        ArrayList<CompletableFuture<Void>> deleteFutures = new ArrayList<>();
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            String segmentName = segmentNames.get(i);
            deleteFutures.add(context.container.deleteStreamSegment(segmentName, TIMEOUT));
        }

        FutureHelpers.allOf(deleteFutures);

        // 4. Verify that only the first half of the segments (and their batches) were deleted, and not the others.
        for (int i = 0; i < segmentNames.size(); i++) {
            ArrayList<String> toCheck = new ArrayList<>();
            toCheck.add(segmentNames.get(i));
            toCheck.addAll(batchesBySegment.get(segmentNames.get(i)));

            boolean expectedDeleted = i < segmentNames.size() / 2;
            if (expectedDeleted) {
                // Verify the segments and their batches are not there anymore.
                for (String sn : toCheck) {
                    AssertExtensions.assertThrows(
                            "getStreamSegmentInfo did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.getStreamSegmentInfo(sn, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "append did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.append(sn, "foo".getBytes(), new AppendContext(UUID.randomUUID(), 0), TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "getLastAppendContext did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.getLastAppendContext(sn, UUID.randomUUID(), TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "read did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.read(sn, 0, 1, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);
                }
            } else {
                // Verify the segments and their batches are still there.
                for (String sn : toCheck) {
                    SegmentProperties props = context.container.getStreamSegmentInfo(sn, TIMEOUT).join();
                    Assert.assertFalse("Not-deleted segment (or one of its batches) was marked as deleted in metadata.", props.isDeleted());

                    // Verify we can still append and read from this segment.
                    context.container.append(sn, "foo".getBytes(), new AppendContext(UUID.randomUUID(), 0), TIMEOUT).join();

                    @Cleanup
                    ReadResult rr = context.container.read(sn, 0, 1, TIMEOUT).join();
                }
            }
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the createBatch, append-to-batch, mergeBatch methods.
     */
    @Test
    public void testBatchOperations() throws Exception {
        // Create Batch and Append to Batch were partially tested in the Delete test, so we will focus on merge Batch here.
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> batchesBySegment = createBatches(segmentNames, context);

        // 2. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndBatches(segmentNames, batchesBySegment, lengths, segmentContents, context);

        // 3. Merge all the batches.
        mergeBatches(batchesBySegment, lengths, segmentContents, context);

        // 4. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                appendFutures.add(context.container.append(segmentName, appendData, new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                // Verify that we can no longer append to batches.
                for (String batchName : batchesBySegment.get(segmentName)) {
                    AssertExtensions.assertThrows(
                            "An append was allowed to a merged batch " + batchName,
                            context.container.append(batchName, "foo".getBytes(), new AppendContext(UUID.randomUUID(), 0), TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentMergedException);
                }
            }
        }

        // 5. Verify their contents.
        checkReadIndex(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to perform future (tail) reads. Scenarios tested include:
     * * Regular appends
     * * Segment sealing
     * * Batch merging.
     */
    @Test
    public void testFutureReads() throws Exception {
        final int nonSealReadLimit = 100;
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> batchesBySegment = createBatches(segmentNames, context);
        HashMap<String, ReadResult> readsBySegment = new HashMap<>();
        HashMap<String, AsyncReadResultProcessor> processorsBySegment = new HashMap<>();
        HashSet<String> segmentsToSeal = new HashSet<>();
        HashMap<String, ByteArrayOutputStream> readContents = new HashMap<>();
        HashMap<String, TestEntryHandler> entryHandlers = new HashMap<>();

        // 2. Setup tail reads.
        // First 1/2 of segments will try to read Int32.Max bytes, while the other half will try to read 100 bytes.
        // We will then seal the first 1/2 of the segments, which should cause the tail reads to stop (the remaining
        // should stop upon reaching the limit).
        for (int i = 0; i < segmentNames.size(); i++) {
            String segmentName = segmentNames.get(i);
            ByteArrayOutputStream readContentsStream = new ByteArrayOutputStream();
            readContents.put(segmentName, readContentsStream);

            ReadResult readResult;
            if (i < segmentNames.size() / 2) {
                // We're going to seal this one at one point.
                segmentsToSeal.add(segmentName);
                readResult = context.container.read(segmentName, 0, Integer.MAX_VALUE, TIMEOUT).join();
            } else {
                // Just a regular one, nothing special.
                readResult = context.container.read(segmentName, 0, nonSealReadLimit, TIMEOUT).join();
            }

            // The Read callback is only accumulating data in this test; we will then compare it against the real data.
            TestEntryHandler entryHandler = new TestEntryHandler(readContentsStream);
            entryHandlers.put(segmentName, entryHandler);
            AsyncReadResultProcessor readResultProcessor = new AsyncReadResultProcessor(readResult, entryHandler, context.executorService.get());
            readResultProcessor.startAsync().awaitRunning();
            readsBySegment.put(segmentName, readResult);
            processorsBySegment.put(segmentName, readResultProcessor);
        }

        // 3. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndBatches(segmentNames, batchesBySegment, lengths, segmentContents, context);

        // 4. Merge all the batches.
        mergeBatches(batchesBySegment, lengths, segmentContents, context);

        // 5. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Long>> operationFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                operationFutures.add(context.container.append(segmentName, appendData, new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
            }
        }

        segmentsToSeal.forEach(segmentName -> operationFutures.add(context.container.sealStreamSegment(segmentName, TIMEOUT)));
        FutureHelpers.allOf(operationFutures).join();

        // Now wait for all the reads to complete, and verify their results against the expected output.
        ServiceShutdownListener.awaitShutdown(processorsBySegment.values(), TIMEOUT, true);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<String, TestEntryHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().error.get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail("Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue().error.get());
                }
            }
        }

        // Check that all the ReadResults are closed
        for (Map.Entry<String, ReadResult> e : readsBySegment.entrySet()) {
            Assert.assertTrue("Read result is not closed for segment " + e.getKey(), e.getValue().isClosed());
        }

        // Compare, byte-by-byte, the outcome of the tail reads.
        Assert.assertEquals("Unexpected number of segments were read.", segmentContents.size(), readContents.size());
        for (String segmentName : segmentNames) {
            boolean isSealed = segmentsToSeal.contains(segmentName);

            byte[] expectedData = segmentContents.get(segmentName).toByteArray();
            byte[] actualData = readContents.get(segmentName).toByteArray();
            int expectedLength = isSealed ? (int) (long) lengths.get(segmentName) : nonSealReadLimit;
            Assert.assertEquals("Unexpected read length for segment " + segmentName, expectedLength, actualData.length);
            AssertExtensions.assertArrayEquals("Unexpected read contents for segment " + segmentName, expectedData, 0, actualData, 0, actualData.length);
        }
    }

    private static void checkReadIndex(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, TestContext context) throws Exception {
        for (String segmentName : segmentContents.keySet()) {
            long expectedLength = lengths.get(segmentName);
            long segmentLength = context.container.getStreamSegmentInfo(segmentName, TIMEOUT).join().getLength();

            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, segmentLength);
            byte[] expectedData = segmentContents.get(segmentName).toByteArray();

            long expectedCurrentOffset = 0;
            @Cleanup
            ReadResult readResult = context.container.read(segmentName, expectedCurrentOffset, (int) segmentLength, TIMEOUT).join();
            Assert.assertTrue("Empty read result for segment " + segmentName, readResult.hasNext());

            // A more thorough read check is done in testSegmentRegularOperations; here we just check if the data was merged correctly.
            while (readResult.hasNext()) {
                ReadResultEntry readEntry = readResult.next();
                AssertExtensions.assertGreaterThan("getRequestedReadLength should be a positive integer for segment " + segmentName, 0, readEntry.getRequestedReadLength());
                Assert.assertEquals("Unexpected value from getStreamSegmentOffset for segment " + segmentName, expectedCurrentOffset, readEntry.getStreamSegmentOffset());
                Assert.assertTrue("getContent() did not return a completed future for segment" + segmentName, readEntry.getContent().isDone() && !readEntry.getContent().isCompletedExceptionally());
                Assert.assertNotEquals("Unexpected value for isEndOfStreamSegment for non-sealed segment " + segmentName, ReadResultEntryType.EndOfStreamSegment, readEntry.getType());

                ReadResultEntryContents readEntryContents = readEntry.getContent().join();
                byte[] actualData = new byte[readEntryContents.getLength()];
                StreamHelpers.readAll(readEntryContents.getData(), actualData, 0, actualData.length);
                AssertExtensions.assertArrayEquals("Unexpected data read from segment " + segmentName + " at offset " + expectedCurrentOffset, expectedData, (int) expectedCurrentOffset, actualData, 0, readEntryContents.getLength());
                expectedCurrentOffset += readEntryContents.getLength();
            }

            Assert.assertTrue("ReadResult was not closed post-full-consumption for segment" + segmentName, readResult.isClosed());
        }
    }

    private void appendToParentsAndBatches(Collection<String> segmentNames, HashMap<String, ArrayList<String>> batchesBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Long>> appendFutures = new ArrayList<>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                for (String batchName : batchesBySegment.get(segmentName)) {
                    appendData = getAppendData(batchName, i);
                    appendFutures.add(context.container.append(batchName, appendData, new AppendContext(UUID.randomUUID(), 0), TIMEOUT));
                    lengths.put(batchName, lengths.getOrDefault(batchName, 0L) + appendData.length);
                    recordAppend(batchName, appendData, segmentContents);
                }
            }
        }

        FutureHelpers.allOf(appendFutures).join();
    }

    private void mergeBatches(HashMap<String, ArrayList<String>> batchesBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Long>> mergeFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : batchesBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String batchName : e.getValue()) {
                mergeFutures.add(context.container.sealStreamSegment(batchName, TIMEOUT));
                mergeFutures.add(context.container.mergeBatch(batchName, TIMEOUT));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(batchName));
                lengths.remove(batchName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(batchName).toByteArray());
                segmentContents.remove(batchName);
            }
        }

        FutureHelpers.allOf(mergeFutures).join();
    }

    private byte[] getAppendData(String segmentName, int appendId) {
        return String.format("%s_%d", segmentName, appendId).getBytes();
    }

    private ArrayList<String> createSegments(TestContext context) {
        ArrayList<String> segmentNames = new ArrayList<>();
        ArrayList<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            String segmentName = getSegmentName(i);
            segmentNames.add(segmentName);
            futures.add(context.container.createStreamSegment(segmentName, TIMEOUT));
        }

        FutureHelpers.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createBatches(Collection<String> segmentNames, TestContext context) {
        // Create the batches.
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < BATCHES_PER_SEGMENT; i++) {
                futures.add(context.container.createBatch(segmentName, TIMEOUT));
            }
        }

        FutureHelpers.allOf(futures).join();

        // Get the batch names and index them by parent segment names.
        HashMap<String, ArrayList<String>> batches = new HashMap<>();
        for (CompletableFuture<String> batchFuture : futures) {
            String batchName = batchFuture.join();
            String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(batchName);
            assert parentName != null : "batch created with invalid parent";
            ArrayList<String> segmentBatches = batches.get(parentName);
            if (segmentBatches == null) {
                segmentBatches = new ArrayList<>();
                batches.put(parentName, segmentBatches);
            }

            segmentBatches.add(batchName);
        }

        return batches;
    }

    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
    }

    private String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private ArrayList<UUID> createClients() {
        ArrayList<UUID> clients = new ArrayList<>();
        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients.add(UUID.randomUUID());
        }

        return clients;
    }

    private String getAppendContextKey(String segmentName, UUID clientId) {
        return String.format("%s_%s", segmentName, clientId);
    }

    //region TestContext

    private static class TestContext implements AutoCloseable {
        final SegmentContainer container;
        private final MetadataRepository metadataRepository;
        private final CloseableExecutorService executorService;
        private final StorageFactory storageFactory;
        private final DurableDataLogFactory dataLogFactory;
        private final OperationLogFactory operationLogFactory;
        private final ReadIndexFactory readIndexFactory;
        private final WriterFactory writerFactory;
        private final CacheFactory cacheFactory;

        TestContext() {
            this.metadataRepository = new InMemoryMetadataRepository();
            this.executorService = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.storageFactory = new InMemoryStorageFactory(this.executorService.get());
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE);
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService.get());
            this.cacheFactory = new InMemoryCacheFactory();
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.storageFactory, this.executorService.get());
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, this.storageFactory, this.executorService.get());
            StreamSegmentContainerFactory factory = new StreamSegmentContainerFactory(this.metadataRepository, this.operationLogFactory, this.readIndexFactory, this.writerFactory, this.storageFactory, this.cacheFactory, this.executorService.get());
            this.container = factory.createStreamSegmentContainer(CONTAINER_ID);
        }

        @Override
        public void close() {
            this.container.close();
            this.dataLogFactory.close();
            this.storageFactory.close();
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
