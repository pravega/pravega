/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.containers;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.segment.StreamSegmentNameUtils;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.TypedProperties;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.AttributeUpdateType;
import io.pravega.service.contracts.Attributes;
import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.ReadResultEntry;
import io.pravega.service.contracts.ReadResultEntryContents;
import io.pravega.service.contracts.ReadResultEntryType;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentMergedException;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.contracts.TooManyActiveSegmentsException;
import io.pravega.service.server.ConfigHelpers;
import io.pravega.service.server.OperationLogFactory;
import io.pravega.service.server.ReadIndexFactory;
import io.pravega.service.server.SegmentContainer;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.SegmentMetadataComparer;
import io.pravega.service.server.WriterFactory;
import io.pravega.service.server.logs.DurableLogConfig;
import io.pravega.service.server.logs.DurableLogFactory;
import io.pravega.service.server.reading.AsyncReadResultProcessor;
import io.pravega.service.server.reading.ContainerReadIndexFactory;
import io.pravega.service.server.reading.ReadIndexConfig;
import io.pravega.service.server.reading.TestReadResultHandler;
import io.pravega.service.server.writer.StorageWriterFactory;
import io.pravega.service.server.writer.WriterConfig;
import io.pravega.service.storage.CacheFactory;
import io.pravega.service.storage.DurableDataLogFactory;
import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
import io.pravega.service.storage.mocks.InMemoryCacheFactory;
import io.pravega.service.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.service.storage.mocks.InMemoryStorageFactory;
import io.pravega.service.storage.mocks.ListenableStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for StreamSegmentContainer class.
 * These are not really unit tests. They are more like integration/end-to-end tests, since they test a real StreamSegmentContainer
 * using a real DurableLog, real ReadIndex and real StorageWriter - but all against in-memory mocks of Storage and
 * DurableDataLog.
 */
public class StreamSegmentContainerTests extends ThreadPooledTestSuite {
    private static final int SEGMENT_COUNT = 1;
    private static final int TRANSACTIONS_PER_SEGMENT = 5;
    private static final int APPENDS_PER_SEGMENT = 100;
    private static final int ATTRIBUTE_UPDATES_PER_SEGMENT = 1;
    private static final int CONTAINER_ID = 1234567;
    private static final int MAX_DATA_LOG_APPEND_SIZE = 100 * 1024;
    private static final Duration TIMEOUT = Duration.ofSeconds(100);
    private static final ContainerConfig DEFAULT_CONFIG = ContainerConfig
            .builder()
            .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, 10 * 60)
            .build();

    // Create checkpoints every 100 operations or after 10MB have been written, but under no circumstance less frequently than 10 ops.
    private static final DurableLogConfig DEFAULT_DURABLE_LOG_CONFIG = DurableLogConfig
            .builder()
            .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 10)
            .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 100)
            .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
            .build();

    private static final ReadIndexConfig DEFAULT_READ_INDEX_CONFIG = ConfigHelpers
            .withInfiniteCachePolicy(ReadIndexConfig.builder().with(ReadIndexConfig.STORAGE_READ_ALIGNMENT, 1024))
            .build();

    private static final WriterConfig DEFAULT_WRITER_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 1)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 25L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .with(WriterConfig.MAX_READ_TIMEOUT_MILLIS, 250L)
            .build();

    @Override
    protected int getThreadPoolSize() {
        return 5;
    }

    /**
     * Tests the createSegment, append, updateAttributes, read, getSegmentInfo, getActiveSegments.
     */
    @Test
    public void testSegmentRegularOperations() throws Exception {
        final UUID attributeAccumulate = UUID.randomUUID();
        final UUID attributeReplace = UUID.randomUUID();
        final UUID attributeReplaceIfGreater = UUID.randomUUID();
        final UUID attributeNoUpdate = UUID.randomUUID();
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;

        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        checkActiveSegments(context.container, 0);
        activateAllSegments(segmentNames, context);
        checkActiveSegments(context.container, segmentNames.size());

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> opFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, i + 1));
                byte[] appendData = getAppendData(segmentName, i);
                opFutures.add(context.container.append(segmentName, appendData, attributeUpdates, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
            }
        }

        // 2.1 Update some of the attributes.
        for (String segmentName : segmentNames) {
            // Record a one-off update.
            opFutures.add(context.container.updateAttributes(
                    segmentName,
                    Collections.singleton(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, expectedAttributeValue)),
                    TIMEOUT));

            for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, APPENDS_PER_SEGMENT + i + 1));
                attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, APPENDS_PER_SEGMENT + i + 1));
                opFutures.add(context.container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }
        }

        FutureHelpers.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            long expectedLength = lengths.get(segmentName);

            Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength, sp.getLength());
            Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
            Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());

            // Verify all attribute values.
            Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeNoUpdate, SegmentMetadata.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeAccumulate, SegmentMetadata.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeReplace + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeReplace, SegmentMetadata.NULL_ATTRIBUTE_VALUE));
            Assert.assertEquals("Unexpected value for attribute " + attributeReplaceIfGreater + " for segment " + segmentName,
                    expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeReplaceIfGreater, SegmentMetadata.NULL_ATTRIBUTE_VALUE));
        }

        checkActiveSegments(context.container, segmentNames.size());

        // 4. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability for the StreamSegmentContainer to handle concurrent actions on a Segment that it does not know
     * anything about, and handling the resulting concurrency.
     * Note: this is tested with a single segment. It could be tested with multiple segments, but different segments
     * are mostly independent of each other, so we would not be gaining much by doing so.
     */
    @Test
    public void testConcurrentSegmentActivation() throws Exception {
        final UUID attributeAccumulate = UUID.randomUUID();
        final long expectedAttributeValue = APPENDS_PER_SEGMENT + ATTRIBUTE_UPDATES_PER_SEGMENT;
        final int appendLength = 10;

        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        String segmentName = createSegments(context).get(0);

        // 2. Add some appends.
        List<CompletableFuture<Void>> opFutures = Collections.synchronizedList(new ArrayList<>());
        AtomicLong expectedLength = new AtomicLong();

        @Cleanup("shutdown")
        ExecutorService testExecutor = Executors.newFixedThreadPool(Math.min(20, APPENDS_PER_SEGMENT));
        val submitFutures = new ArrayList<Future<?>>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            final byte fillValue = (byte) i;
            submitFutures.add(testExecutor.submit(() -> {
                Collection<AttributeUpdate> attributeUpdates = Collections.singleton(
                        new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                byte[] appendData = new byte[appendLength];
                Arrays.fill(appendData, (byte) (fillValue + 1));
                opFutures.add(context.container.append(segmentName, appendData, attributeUpdates, TIMEOUT));
                expectedLength.addAndGet(appendData.length);
            }));
        }

        // 2.1 Update the attribute.
        for (int i = 0; i < ATTRIBUTE_UPDATES_PER_SEGMENT; i++) {
            submitFutures.add(testExecutor.submit(() -> {
                Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
                attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
                opFutures.add(context.container.updateAttributes(segmentName, attributeUpdates, TIMEOUT));
            }));
        }

        // Wait for the submittal of tasks to complete.
        submitFutures.forEach(this::await);

        // Now wait for all the appends to finish.
        FutureHelpers.allOf(opFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. getSegmentInfo: verify final state of the attribute.
        SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();

        Assert.assertEquals("Unexpected length for segment " + segmentName, expectedLength.get(), sp.getLength());
        Assert.assertFalse("Unexpected value for isDeleted for segment " + segmentName, sp.isDeleted());
        Assert.assertFalse("Unexpected value for isSealed for segment " + segmentName, sp.isDeleted());

        // Verify all attribute values.
        Assert.assertEquals("Unexpected value for attribute " + attributeAccumulate + " for segment " + segmentName,
                expectedAttributeValue, (long) sp.getAttributes().getOrDefault(attributeAccumulate, SegmentMetadata.NULL_ATTRIBUTE_VALUE));

        checkActiveSegments(context.container, 1);

        // 4. Written data.
        byte[] actualData = new byte[(int) expectedLength.get()];
        int offset = 0;
        @Cleanup
        ReadResult readResult = context.container.read(segmentName, 0, actualData.length, TIMEOUT).join();
        while (readResult.hasNext()) {
            ReadResultEntry readEntry = readResult.next();
            ReadResultEntryContents readEntryContents = readEntry.getContent().join();
            AssertExtensions.assertLessThanOrEqual("Too much to read.", actualData.length, offset + actualData.length);
            StreamHelpers.readAll(readEntryContents.getData(), actualData, offset, actualData.length);
            offset += actualData.length;
        }

        Assert.assertEquals("Unexpected number of bytes read.", actualData.length, offset);
        Assert.assertTrue("Unexpected number of bytes read (multiple of appendLength).", actualData.length % appendLength == 0);

        boolean[] observedValues = new boolean[APPENDS_PER_SEGMENT + 1];
        for (int i = 0; i < actualData.length; i += appendLength) {
            byte value = actualData[i];
            Assert.assertFalse("Append with value " + value + " was written multiple times.", observedValues[value]);
            observedValues[value] = true;
            for (int j = 1; j < appendLength; j++) {
                Assert.assertEquals("Append was not written atomically at offset " + (i + j), value, actualData[i + j]);
            }
        }

        // Verify all the appends made it (we purposefully did not write 0, since that's the default fill value in an array).
        Assert.assertFalse("Not expecting 0 as a value.", observedValues[0]);
        for (int i = 1; i < observedValues.length; i++) {
            Assert.assertTrue("Append with value " + i + " was not written.", observedValues[i]);
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to make appends with offset.
     */
    @Test
    public void testAppendWithOffset() throws Exception {
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        activateAllSegments(segmentNames, context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        int appendId = 0;
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                long offset = lengths.getOrDefault(segmentName, 0L);
                appendFutures.add(context.container.append(segmentName, offset, appendData, null, TIMEOUT));

                lengths.put(segmentName, offset + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
                appendId++;
            }
        }

        FutureHelpers.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 2.1 Verify that if we pass wrong offsets, the append is failed.
        for (String segmentName : segmentNames) {
            byte[] appendData = getAppendData(segmentName, appendId);
            long offset = lengths.get(segmentName) + (appendId % 2 == 0 ? 1 : -1);

            AssertExtensions.assertThrows(
                    "append did not fail with the appropriate exception when passed a bad offset.",
                    () -> context.container.append(segmentName, offset, appendData, null, TIMEOUT),
                    ex -> ex instanceof BadOffsetException);
            appendId++;
        }

        // 3. Reads (regular reads, not tail reads).
        checkReadIndex(segmentContents, lengths, context);

        // 4. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

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
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        HashMap<String, Long> lengths = new HashMap<>();

        for (String segmentName : segmentNames) {
            ByteArrayOutputStream segmentStream = new ByteArrayOutputStream();
            segmentContents.put(segmentName, segmentStream);
            for (int i = 0; i < appendsPerSegment; i++) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                segmentStream.write(appendData);
            }
        }

        FutureHelpers.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Seal first half of segments.
        ArrayList<CompletableFuture<Long>> sealFutures = new ArrayList<>();
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            sealFutures.add(context.container.sealStreamSegment(segmentNames.get(i), TIMEOUT));
        }

        FutureHelpers.allOf(sealFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Check that the segments were properly sealed.
        for (int i = 0; i < segmentNames.size(); i++) {
            String segmentName = segmentNames.get(i);
            boolean expectedSealed = i < segmentNames.size() / 2;
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            if (expectedSealed) {
                Assert.assertTrue("Segment is not sealed when it should be " + segmentName, sp.isSealed());
                Assert.assertEquals("Unexpected result from seal() future for segment " + segmentName, sp.getLength(), (long) sealFutures.get(i).join());
                AssertExtensions.assertThrows(
                        "Container allowed appending to a sealed segment " + segmentName,
                        context.container.append(segmentName, "foo".getBytes(), null, TIMEOUT)::join,
                        ex -> ex instanceof StreamSegmentSealedException);
            } else {
                Assert.assertFalse("Segment is sealed when it shouldn't be " + segmentName, sp.isSealed());

                // Verify we can still append to these segments.
                byte[] appendData = "foo".getBytes();
                context.container.append(segmentName, appendData, null, TIMEOUT).join();
                segmentContents.get(segmentName).write(appendData);
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
            }
        }

        // 4. Reads (regular reads, not tail reads, and only for the sealed segments).
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            String segmentName = segmentNames.get(i);
            long segmentLength = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();

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
                            readEntry::getContent,
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

        // 5. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

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
                context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT)::join,
                ex -> ex instanceof StreamSegmentNotExistsException);

        AssertExtensions.assertThrows(
                "append did not throw expected exception when called on a non-existent StreamSegment.",
                context.container.append(segmentName, "foo".getBytes(), null, TIMEOUT)::join,
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
    public void testSegmentDelete() throws Exception {
        final int appendsPerSegment = 1;
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);

        // 2. Add some appends.
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();

        for (int i = 0; i < appendsPerSegment; i++) {
            for (String segmentName : segmentNames) {
                appendFutures.add(context.container.append(segmentName, getAppendData(segmentName, i), null, TIMEOUT));
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    appendFutures.add(context.container.append(transactionName, getAppendData(transactionName, i), null, TIMEOUT));
                }
            }
        }

        FutureHelpers.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 3. Delete the first half of the segments.
        ArrayList<CompletableFuture<Void>> deleteFutures = new ArrayList<>();
        for (int i = 0; i < segmentNames.size() / 2; i++) {
            String segmentName = segmentNames.get(i);
            deleteFutures.add(context.container.deleteStreamSegment(segmentName, TIMEOUT));
        }

        FutureHelpers.allOf(deleteFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 4. Verify that only the first half of the segments (and their Transactions) were deleted, and not the others.
        for (int i = 0; i < segmentNames.size(); i++) {
            ArrayList<String> toCheck = new ArrayList<>();
            toCheck.add(segmentNames.get(i));
            toCheck.addAll(transactionsBySegment.get(segmentNames.get(i)));

            boolean expectedDeleted = i < segmentNames.size() / 2;
            if (expectedDeleted) {
                // Verify the segments and their Transactions are not there anymore.
                for (String sn : toCheck) {
                    AssertExtensions.assertThrows(
                            "getStreamSegmentInfo did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.getStreamSegmentInfo(sn, false, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "append did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.append(sn, "foo".getBytes(), null, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    AssertExtensions.assertThrows(
                            "read did not throw expected exception when called on a deleted StreamSegment.",
                            context.container.read(sn, 0, 1, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentNotExistsException);

                    Assert.assertFalse("Segment not deleted in storage.", context.storage.exists(sn, TIMEOUT).join());
                }
            } else {
                // Verify the segments and their Transactions are still there.
                for (String sn : toCheck) {
                    SegmentProperties props = context.container.getStreamSegmentInfo(sn, false, TIMEOUT).join();
                    Assert.assertFalse("Not-deleted segment (or one of its Transactions) was marked as deleted in metadata.", props.isDeleted());

                    // Verify we can still append and read from this segment.
                    context.container.append(sn, "foo".getBytes(), null, TIMEOUT).join();

                    @Cleanup
                    ReadResult rr = context.container.read(sn, 0, 1, TIMEOUT).join();

                    // Verify the segment still exists in storage.
                    context.storage.getStreamSegmentInfo(sn, TIMEOUT).join();
                }
            }
        }

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Test the createTransaction, append-to-Transaction, mergeTransaction methods.
     */
    @Test
    public void testTransactionOperations() throws Exception {
        // Create Transaction and Append to Transaction were partially tested in the Delete test, so we will focus on merge Transaction here.
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        // 2. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 3. Merge all the Transaction.
        mergeTransactions(transactionsBySegment, lengths, segmentContents, context);

        // 4. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                // Verify that we can no longer append to Transaction.
                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    AssertExtensions.assertThrows(
                            "An append was allowed to a merged Transaction " + transactionName,
                            context.container.append(transactionName, "foo".getBytes(), null, TIMEOUT)::join,
                            ex -> ex instanceof StreamSegmentMergedException || ex instanceof StreamSegmentNotExistsException);
                }
            }
        }

        FutureHelpers.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // 5. Verify their contents.
        checkReadIndex(segmentContents, lengths, context);

        // 6. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Tests the ability to perform future (tail) reads. Scenarios tested include:
     * * Regular appends
     * * Segment sealing
     * * Transaction merging.
     */
    @Test
    public void testFutureReads() throws Exception {
        final int nonSealReadLimit = 100;
        @Cleanup
        TestContext context = new TestContext();
        context.container.startAsync().awaitRunning();

        // 1. Create the StreamSegments.
        ArrayList<String> segmentNames = createSegments(context);
        HashMap<String, ArrayList<String>> transactionsBySegment = createTransactions(segmentNames, context);
        activateAllSegments(segmentNames, context);
        transactionsBySegment.values().forEach(s -> activateAllSegments(s, context));

        HashMap<String, ReadResult> readsBySegment = new HashMap<>();
        ArrayList<AsyncReadResultProcessor> readProcessors = new ArrayList<>();
        HashSet<String> segmentsToSeal = new HashSet<>();
        HashMap<String, ByteArrayOutputStream> readContents = new HashMap<>();
        HashMap<String, TestReadResultHandler> entryHandlers = new HashMap<>();

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
            TestReadResultHandler entryHandler = new TestReadResultHandler(readContentsStream, TIMEOUT);
            entryHandlers.put(segmentName, entryHandler);
            readsBySegment.put(segmentName, readResult);
            readProcessors.add(AsyncReadResultProcessor.process(readResult, entryHandler, executorService()));
        }

        // 3. Add some appends.
        HashMap<String, Long> lengths = new HashMap<>();
        HashMap<String, ByteArrayOutputStream> segmentContents = new HashMap<>();
        appendToParentsAndTransactions(segmentNames, transactionsBySegment, lengths, segmentContents, context);

        // 4. Merge all the Transactions.
        mergeTransactions(transactionsBySegment, lengths, segmentContents, context);

        // 5. Add more appends (to the parent segments)
        ArrayList<CompletableFuture<Void>> operationFutures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, APPENDS_PER_SEGMENT + i);
                operationFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);
            }
        }

        segmentsToSeal.forEach(segmentName -> operationFutures
                .add(FutureHelpers.toVoid(context.container.sealStreamSegment(segmentName, TIMEOUT))));
        FutureHelpers.allOf(operationFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now wait for all the reads to complete, and verify their results against the expected output.
        FutureHelpers.allOf(entryHandlers.values().stream().map(h -> h.getCompleted()).collect(Collectors.toList())).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        readProcessors.forEach(AsyncReadResultProcessor::close);

        // Check to see if any errors got thrown (and caught) during the reading process).
        for (Map.Entry<String, TestReadResultHandler> e : entryHandlers.entrySet()) {
            Throwable err = e.getValue().getError().get();
            if (err != null) {
                // Check to see if the exception we got was a SegmentSealedException. If so, this is only expected if the segment was to be sealed.
                // The next check (see below) will verify if the segments were properly read).
                if (!(err instanceof StreamSegmentSealedException && segmentsToSeal.contains(e.getKey()))) {
                    Assert.fail("Unexpected error happened while processing Segment " + e.getKey() + ": " + e.getValue().getError().get());
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

        // 6. Writer moving data to Storage.
        waitForSegmentsInStorage(segmentNames, context).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        checkStorage(segmentContents, lengths, context);
    }

    /**
     * Tests the ability to clean up SegmentMetadata for those segments which have not been used recently.
     * This test does the following:
     * 1. Sets up a custom SegmentContainer with a hook into the metadataCleanup task
     * 2. Creates a segment and appends something to it, each time updating attributes (and verifies they were updated correctly).
     * 3. Waits for the segment to be forgotten (evicted).
     * 4. Requests info on the segment, validates it, then makes another append, seals it, at each step verifying it was done
     * correctly (checking Metadata, Attributes and Storage).
     * 5. Deletes the segment, waits for metadata to be cleared (via forcing another log truncation), re-creates the
     * same segment and validates that the old attributes did not "bleed in".
     */
    @Test
    public void testMetadataCleanup() throws Exception {
        final String segmentName = "segment";
        final UUID[] attributes = new UUID[]{Attributes.CREATION_TIME, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        final byte[] appendData = "hello".getBytes();
        final Map<UUID, Long> expectedAttributes = new HashMap<>();

        // We need a special DL config so that we can force truncations after every operation - this will speed up metadata
        // eviction eligibility.
        final DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 5)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10 * 1024 * 1024L)
                .build();
        final TestContainerConfig containerConfig = new TestContainerConfig();
        containerConfig.setSegmentMetadataExpiration(Duration.ofMillis(250));

        @Cleanup
        TestContext context = new TestContext(containerConfig);
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(durableLogConfig, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.writerFactory, context.storageFactory, executorService());
        localContainer.startAsync().awaitRunning();

        // Create segment with initial attributes and verify they were set correctly.
        val initialAttributes = createAttributeUpdates(attributes);
        applyAttributes(initialAttributes, expectedAttributes);
        localContainer.createStreamSegment(segmentName, initialAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentProperties sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after segment creation.", expectedAttributes, sp);

        // Add one append with some attribute changes and verify they were set correctly.
        val appendAttributes = createAttributeUpdates(attributes);
        applyAttributes(appendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData, appendAttributes, TIMEOUT);
        sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after append.", expectedAttributes, sp);

        // Wait until the segment is forgotten.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now get attributes again and verify them.
        sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp);

        // Append again, and make sure we can append at the right offset.
        val secondAppendAttributes = createAttributeUpdates(attributes);
        applyAttributes(secondAppendAttributes, expectedAttributes);
        localContainer.append(segmentName, appendData.length, appendData, secondAppendAttributes, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length from segment after eviction & resurrection.", 2 * appendData.length, sp.getLength());
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after eviction & resurrection.", expectedAttributes, sp);

        // Seal (this should clear out non-dynamic attributes).
        expectedAttributes.keySet().removeIf(Attributes::isDynamic);
        localContainer.sealStreamSegment(segmentName, TIMEOUT);
        sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after seal.", expectedAttributes, sp);

        // Verify the segment actually made to Storage in one piece.
        waitForSegmentInStorage(sp, context.storage).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        val storageInfo = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected length in storage for segment.", sp.getLength(), storageInfo.getLength());

        // Delete segment and wait until it is forgotten again (we need to create another dummy segment so that we can
        // force a Metadata Truncation in order to facilitate that; this is the purpose of segment2).
        localContainer.deleteStreamSegment(segmentName, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Wait for the segment to be forgotten again.
        localContainer.triggerMetadataCleanup(Collections.singleton(segmentName)).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Now Create the Segment again and verify the old attributes were not "remembered".
        val newAttributes = createAttributeUpdates(attributes);
        applyAttributes(newAttributes, expectedAttributes);
        localContainer.createStreamSegment(segmentName, newAttributes, TIMEOUT)
                      .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        sp = localContainer.getStreamSegmentInfo(segmentName, true, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after deletion and re-creation.", expectedAttributes, sp);
    }

    /**
     * Tests the case when the ContainerMetadata has filled up to capacity (with segments and we cannot map anymore segments).
     */
    @Test
    public void testForcedMetadataCleanup() throws Exception {
        final int maxSegmentCount = 3;
        final ContainerConfig containerConfig = ContainerConfig
                .builder()
                .with(ContainerConfig.SEGMENT_METADATA_EXPIRATION_SECONDS, (int) DEFAULT_CONFIG.getSegmentMetadataExpiration().getSeconds())
                .with(ContainerConfig.MAX_ACTIVE_SEGMENT_COUNT, maxSegmentCount)
                .build();

        // We need a special DL config so that we can force truncations after every operation - this will speed up metadata
        // eviction eligibility.
        final DurableLogConfig durableLogConfig = DurableLogConfig
                .builder()
                .with(DurableLogConfig.CHECKPOINT_MIN_COMMIT_COUNT, 1)
                .with(DurableLogConfig.CHECKPOINT_COMMIT_COUNT, 5)
                .with(DurableLogConfig.CHECKPOINT_TOTAL_COMMIT_LENGTH, 10L * 1024 * 1024)
                .build();

        @Cleanup
        TestContext context = new TestContext(containerConfig);
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(durableLogConfig, context.dataLogFactory, executorService());
        @Cleanup
        MetadataCleanupContainer localContainer = new MetadataCleanupContainer(CONTAINER_ID, containerConfig, localDurableLogFactory,
                context.readIndexFactory, context.writerFactory, context.storageFactory, executorService());
        localContainer.startAsync().awaitRunning();

        // Create 4 segments and one transaction.
        String segment0 = getSegmentName(0);
        localContainer.createStreamSegment(segment0, null, TIMEOUT).join();
        String segment1 = getSegmentName(1);
        localContainer.createStreamSegment(segment1, null, TIMEOUT).join();
        String segment2 = getSegmentName(2);
        localContainer.createStreamSegment(segment2, null, TIMEOUT).join();
        String segment3 = getSegmentName(3);
        localContainer.createStreamSegment(segment3, null, TIMEOUT).join();
        String txn1 = localContainer.createTransaction(segment3, UUID.randomUUID(), null, TIMEOUT).join();

        // Activate one segment.
        localContainer.getStreamSegmentInfo(segment2, false, TIMEOUT).join();

        // Activate the transaction; this should fill up the metadata (itself + parent).
        localContainer.getStreamSegmentInfo(txn1, false, TIMEOUT).join();

        // Verify the transaction's parent has been activated.
        Assert.assertNotNull("Transaction's parent has not been activated.",
                localContainer.getStreamSegmentInfo(segment3, false, TIMEOUT).join());

        // At this point, the active segments should be: 2, 3 and Txn.
        // Verify we cannot activate any other segment.
        AssertExtensions.assertThrows(
                "getSegmentId() allowed mapping more segments than the metadata can support.",
                () -> localContainer.getStreamSegmentInfo(segment1, false, TIMEOUT),
                ex -> ex instanceof TooManyActiveSegmentsException);

        AssertExtensions.assertThrows(
                "getSegmentId() allowed mapping more segments than the metadata can support.",
                () -> localContainer.getStreamSegmentInfo(segment0, false, TIMEOUT),
                ex -> ex instanceof TooManyActiveSegmentsException);

        // Test the ability to forcefully evict items from the metadata when there is pressure and we need to register something new.
        // Case 1: following a Segment deletion.
        localContainer.deleteStreamSegment(segment2, TIMEOUT).join();
        val segment1Activation = tryActivate(localContainer, segment1, segment3);
        val segment1Info = segment1Activation.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNotNull("Unable to properly activate dormant segment (1).", segment1Info);

        // Case 2: following a Merge.
        localContainer.sealStreamSegment(txn1, TIMEOUT).join();
        localContainer.mergeTransaction(txn1, TIMEOUT).join();
        val segment0Activation = tryActivate(localContainer, segment0, segment3);
        val segment0Info = segment0Activation.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertNotNull("Unable to properly activate dormant segment (0).", segment0Info);

        // At this point the active segments should be: 0, 1 and 3.
        Assert.assertNotNull("Pre-activated segment did not stay in metadata (3).",
                localContainer.getStreamSegmentInfo(segment3, false, TIMEOUT).join());

        Assert.assertNotNull("Pre-activated segment did not stay in metadata (1).",
                localContainer.getStreamSegmentInfo(segment1, false, TIMEOUT).join());

        Assert.assertNotNull("Pre-activated segment did not stay in metadata (0).",
                localContainer.getStreamSegmentInfo(segment0, false, TIMEOUT).join());

        context.container.stopAsync().awaitTerminated();
    }

    /**
     * Attempts to activate the targetSegment in the given Container. Since we do not have access to the internals of the
     * Container, we need to trigger this somehow, hence the need for this complex code. We need to trigger a truncation,
     * so we need an 'appendSegment' to which we continuously append so that the DurableDataLog is truncated. After truncation,
     * the Metadata should have enough leeway in making room for new activation.
     *
     * @return A Future that will complete either with an exception (failure) or SegmentProperties for the targetSegment.
     */
    private CompletableFuture<SegmentProperties> tryActivate(MetadataCleanupContainer localContainer, String targetSegment, String appendSegment) {
        CompletableFuture<SegmentProperties> successfulMap = new CompletableFuture<>();

        // Append continuously to an existing segment in order to trigger truncations (these are necessary for forced evictions).
        val appendFuture = localContainer.appendRandomly(appendSegment, false, () -> !successfulMap.isDone());
        FutureHelpers.exceptionListener(appendFuture, successfulMap::completeExceptionally);

        // Repeatedly try to get info on 'segment1' (activate it), until we succeed or time out.
        TimeoutTimer remaining = new TimeoutTimer(TIMEOUT);
        FutureHelpers.loop(
                () -> !successfulMap.isDone(),
                () -> FutureHelpers
                        .delayedFuture(Duration.ofMillis(250), executorService())
                        .thenCompose(v -> localContainer.getStreamSegmentInfo(targetSegment, false, TIMEOUT))
                        .thenAccept(successfulMap::complete)
                        .exceptionally(ex -> {
                            if (!(ExceptionHelpers.getRealException(ex) instanceof TooManyActiveSegmentsException)) {
                                // Some other error.
                                successfulMap.completeExceptionally(ex);
                            } else if (!remaining.hasRemaining()) {
                                // Waited too long.
                                successfulMap.completeExceptionally(new TimeoutException("No successful activation could be done in the allotted time."));
                            }

                            // Try again.
                            return null;
                        }),
                executorService());
        return successfulMap;
    }

    private static void checkStorage(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, TestContext context) {
        for (String segmentName : segmentContents.keySet()) {
            // 1. Deletion status
            SegmentProperties sp = null;
            try {
                sp = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            } catch (Exception ex) {
                if (!(ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException)) {
                    throw ex;
                }
            }

            if (sp == null) {
                Assert.assertFalse(
                        "Segment is marked as deleted in metadata but was not deleted in Storage " + segmentName,
                        context.storage.exists(segmentName, TIMEOUT).join());

                // No need to do other checks.
                continue;
            }

            // 2. Seal Status
            SegmentProperties storageProps = context.storage.getStreamSegmentInfo(segmentName, TIMEOUT).join();
            Assert.assertEquals("Segment seal status disagree between Metadata and Storage for segment " + segmentName, sp.isSealed(), storageProps.isSealed());

            // 3. Contents.
            long expectedLength = lengths.get(segmentName);
            Assert.assertEquals("Unexpected Storage length for segment " + segmentName, expectedLength, storageProps.getLength());

            byte[] expectedData = segmentContents.get(segmentName).toByteArray();
            byte[] actualData = new byte[expectedData.length];
            val readHandle = context.storage.openRead(segmentName).join();
            int actualLength = context.storage.read(readHandle, 0, actualData, 0, actualData.length, TIMEOUT).join();
            Assert.assertEquals("Unexpected number of bytes read from Storage for segment " + segmentName, expectedLength, actualLength);
            Assert.assertArrayEquals("Unexpected data written to storage for segment " + segmentName, expectedData, actualData);
        }
    }

    private static void checkReadIndex(HashMap<String, ByteArrayOutputStream> segmentContents, HashMap<String, Long> lengths, TestContext context) throws Exception {
        for (String segmentName : segmentContents.keySet()) {
            long expectedLength = lengths.get(segmentName);
            long segmentLength = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join().getLength();

            Assert.assertEquals("Unexpected Read Index length for segment " + segmentName, expectedLength, segmentLength);
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

    private void checkActiveSegments(SegmentContainer container, int expectedCount) {
        val initialActiveSegments = container.getActiveSegments();
        Assert.assertEquals("Unexpected result from getActiveSegments with freshly created segments.", expectedCount, initialActiveSegments.size());
        for (SegmentProperties sp : initialActiveSegments) {
            val expectedSp = container.getStreamSegmentInfo(sp.getName(), false, TIMEOUT).join();
            Assert.assertEquals("Unexpected length (from getActiveSegments) for segment " + sp.getName(), expectedSp.getLength(), sp.getLength());
            Assert.assertEquals("Unexpected sealed (from getActiveSegments) for segment " + sp.getName(), expectedSp.isSealed(), sp.isSealed());
            Assert.assertEquals("Unexpected deleted (from getActiveSegments) for segment " + sp.getName(), expectedSp.isDeleted(), sp.isDeleted());
            SegmentMetadataComparer.assertSameAttributes("Unexpected attributes (from getActiveSegments) for segment " + sp.getName(),
                    expectedSp.getAttributes(), sp);
        }
    }

    private void appendToParentsAndTransactions(Collection<String> segmentNames, HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Void>> appendFutures = new ArrayList<>();
        for (int i = 0; i < APPENDS_PER_SEGMENT; i++) {
            for (String segmentName : segmentNames) {
                byte[] appendData = getAppendData(segmentName, i);
                appendFutures.add(context.container.append(segmentName, appendData, null, TIMEOUT));
                lengths.put(segmentName, lengths.getOrDefault(segmentName, 0L) + appendData.length);
                recordAppend(segmentName, appendData, segmentContents);

                for (String transactionName : transactionsBySegment.get(segmentName)) {
                    appendData = getAppendData(transactionName, i);
                    appendFutures.add(context.container.append(transactionName, appendData, null, TIMEOUT));
                    lengths.put(transactionName, lengths.getOrDefault(transactionName, 0L) + appendData.length);
                    recordAppend(transactionName, appendData, segmentContents);
                }
            }
        }

        FutureHelpers.allOf(appendFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void mergeTransactions(HashMap<String, ArrayList<String>> transactionsBySegment, HashMap<String, Long> lengths, HashMap<String, ByteArrayOutputStream> segmentContents, TestContext context) throws Exception {
        ArrayList<CompletableFuture<Void>> mergeFutures = new ArrayList<>();
        for (Map.Entry<String, ArrayList<String>> e : transactionsBySegment.entrySet()) {
            String parentName = e.getKey();
            for (String transactionName : e.getValue()) {
                mergeFutures.add(FutureHelpers.toVoid(context.container.sealStreamSegment(transactionName, TIMEOUT)));
                mergeFutures.add(context.container.mergeTransaction(transactionName, TIMEOUT));

                // Update parent length.
                lengths.put(parentName, lengths.get(parentName) + lengths.get(transactionName));
                lengths.remove(transactionName);

                // Update parent contents.
                segmentContents.get(parentName).write(segmentContents.get(transactionName).toByteArray());
                segmentContents.remove(transactionName);
            }
        }

        FutureHelpers.allOf(mergeFutures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
            futures.add(context.container.createStreamSegment(segmentName, null, TIMEOUT));
        }

        FutureHelpers.allOf(futures).join();
        return segmentNames;
    }

    private HashMap<String, ArrayList<String>> createTransactions(Collection<String> segmentNames, TestContext context) {
        // Create the Transaction.
        ArrayList<CompletableFuture<String>> futures = new ArrayList<>();
        for (String segmentName : segmentNames) {
            for (int i = 0; i < TRANSACTIONS_PER_SEGMENT; i++) {
                futures.add(context.container.createTransaction(segmentName, UUID.randomUUID(), null, TIMEOUT));
            }
        }

        FutureHelpers.allOf(futures).join();

        // Get the Transaction names and index them by parent segment names.
        HashMap<String, ArrayList<String>> transactions = new HashMap<>();
        for (CompletableFuture<String> transactionFuture : futures) {
            String transactionName = transactionFuture.join();
            String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
            assert parentName != null : "Transaction created with invalid parent";
            ArrayList<String> segmentTransactions = transactions.get(parentName);
            if (segmentTransactions == null) {
                segmentTransactions = new ArrayList<>();
                transactions.put(parentName, segmentTransactions);
            }

            segmentTransactions.add(transactionName);
        }

        return transactions;
    }

    private void recordAppend(String segmentName, byte[] data, HashMap<String, ByteArrayOutputStream> segmentContents) throws Exception {
        ByteArrayOutputStream contents = segmentContents.getOrDefault(segmentName, null);
        if (contents == null) {
            contents = new ByteArrayOutputStream();
            segmentContents.put(segmentName, contents);
        }

        contents.write(data);
    }

    private static String getSegmentName(int i) {
        return "Segment_" + i;
    }

    private CompletableFuture<Void> waitForSegmentsInStorage(Collection<String> segmentNames, TestContext context) {
        ArrayList<CompletableFuture<Void>> segmentsCompletion = new ArrayList<>();
        for (String segmentName : segmentNames) {
            SegmentProperties sp = context.container.getStreamSegmentInfo(segmentName, false, TIMEOUT).join();
            segmentsCompletion.add(waitForSegmentInStorage(sp, context.storage));
        }

        return FutureHelpers.allOf(segmentsCompletion);
    }

    private CompletableFuture<Void> waitForSegmentInStorage(SegmentProperties sp, Storage storage) {
        Assert.assertTrue("Unable to register triggers.", storage instanceof ListenableStorage);
        ListenableStorage ls = (ListenableStorage) storage;
        if (sp.isSealed()) {
            // Sealed - add a seal trigger.
            return ls.registerSealTrigger(sp.getName(), TIMEOUT);
        } else {
            // Not sealed - add a size trigger.
            return ls.registerSizeTrigger(sp.getName(), sp.getLength(), TIMEOUT);
        }
    }

    private Collection<AttributeUpdate> createAttributeUpdates(UUID[] attributes) {
        return Arrays.stream(attributes)
                     .map(a -> new AttributeUpdate(a, AttributeUpdateType.Replace, System.nanoTime()))
                     .collect(Collectors.toList());
    }

    private void applyAttributes(Collection<AttributeUpdate> updates, Map<UUID, Long> target) {
        updates.forEach(au -> target.put(au.getAttributeId(), au.getValue()));
    }

    /**
     * Ensures that all Segments defined in the given collection are loaded up into the Container's metadata.
     * This is used to simplify a few tests that do not expect interference from StreamSegmentMapper's assignment logic
     * (that is, they execute operations in a certain order and assume that those ops are added to OperationProcessor queue
     * in that order; if StreamSegmentMapper interferes, there is no guarantee as to what this order will be).
     */
    @SneakyThrows
    private void activateAllSegments(Collection<String> segmentNames, TestContext context) {
        val futures = segmentNames.stream()
                                  .map(s -> context.container.getStreamSegmentInfo(s, false, TIMEOUT))
                                  .collect(Collectors.toList());
        FutureHelpers.allOf(futures).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @SneakyThrows
    private void await(Future<?> f) {
        f.get();
    }
    //region TestContext

    private class TestContext implements AutoCloseable {
        final SegmentContainer container;
        private final InMemoryStorageFactory storageFactory;
        private final DurableDataLogFactory dataLogFactory;
        private final OperationLogFactory operationLogFactory;
        private final ReadIndexFactory readIndexFactory;
        private final WriterFactory writerFactory;
        private final CacheFactory cacheFactory;
        private final Storage storage;

        TestContext() {
            this(DEFAULT_CONFIG);
        }

        TestContext(ContainerConfig config) {
            this.storageFactory = new InMemoryStorageFactory(executorService());
            this.dataLogFactory = new InMemoryDurableDataLogFactory(MAX_DATA_LOG_APPEND_SIZE, executorService());
            this.operationLogFactory = new DurableLogFactory(DEFAULT_DURABLE_LOG_CONFIG, dataLogFactory, executorService());
            this.cacheFactory = new InMemoryCacheFactory();
            this.readIndexFactory = new ContainerReadIndexFactory(DEFAULT_READ_INDEX_CONFIG, this.cacheFactory, executorService());
            this.writerFactory = new StorageWriterFactory(DEFAULT_WRITER_CONFIG, executorService());
            StreamSegmentContainerFactory factory = new StreamSegmentContainerFactory(config, this.operationLogFactory,
                    this.readIndexFactory, this.writerFactory, this.storageFactory, executorService());
            this.container = factory.createStreamSegmentContainer(CONTAINER_ID);
            this.storage = this.storageFactory.createStorageAdapter();
        }

        @Override
        public void close() {
            this.container.close();
            this.dataLogFactory.close();
            this.storage.close();
            this.storageFactory.close();
        }
    }

    //endregion

    //region MetadataCleanupContainer

    private static class MetadataCleanupContainer extends StreamSegmentContainer {
        private Consumer<Collection<String>> metadataCleanupFinishedCallback = null;
        private final ScheduledExecutorService executor;

        MetadataCleanupContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory,
                                 ReadIndexFactory readIndexFactory, WriterFactory writerFactory, StorageFactory storageFactory,
                                 ScheduledExecutorService executor) {
            super(streamSegmentContainerId, config, durableLogFactory, readIndexFactory, writerFactory, storageFactory, executor);
            this.executor = executor;
        }

        @Override
        protected void notifyMetadataRemoved(Collection<SegmentMetadata> metadatas) {
            super.notifyMetadataRemoved(metadatas);

            Consumer<Collection<String>> c = this.metadataCleanupFinishedCallback;
            if (c != null) {
                c.accept(metadatas.stream().map(SegmentMetadata::getName).collect(Collectors.toList()));
            }
        }

        /**
         * Triggers a number of metadata cleanups by repeatedly appending to a random new segment until a cleanup task is detected.
         *
         * @param expectedSegmentNames The segments that we are expecting to evict.
         */
        CompletableFuture<Void> triggerMetadataCleanup(Collection<String> expectedSegmentNames) {
            String tempSegmentName = getSegmentName(Long.hashCode(System.nanoTime()));
            HashSet<String> remainingSegments = new HashSet<>(expectedSegmentNames);
            CompletableFuture<Void> cleanupTask = FutureHelpers.futureWithTimeout(TIMEOUT, this.executor);

            // Inject this callback into the MetadataCleaner callback, which was setup for us in createMetadataCleaner().
            this.metadataCleanupFinishedCallback = evictedSegmentNames -> {
                remainingSegments.removeAll(evictedSegmentNames);
                if (remainingSegments.size() == 0) {
                    cleanupTask.complete(null);
                }
            };

            CompletableFuture<Void> af = appendRandomly(tempSegmentName, true, () -> !cleanupTask.isDone());
            FutureHelpers.exceptionListener(af, cleanupTask::completeExceptionally);
            return cleanupTask;
        }

        /**
         * Appends continuously to a random new segment in the given container, as long as the given condition holds.
         */
        CompletableFuture<Void> appendRandomly(String segmentName, boolean createSegment, Supplier<Boolean> canContinue) {
            byte[] appendData = new byte[1];
            return (createSegment ? createStreamSegment(segmentName, null, TIMEOUT) : CompletableFuture.completedFuture(null))
                    .thenCompose(v -> FutureHelpers.loop(
                            canContinue,
                            () -> append(segmentName, appendData, null, TIMEOUT),
                            this.executor))
                    .thenCompose(v -> createSegment ? deleteStreamSegment(segmentName, TIMEOUT) : CompletableFuture.completedFuture(null));
        }
    }

    //endregion

    //region TestContainerConfig

    private static class TestContainerConfig extends ContainerConfig {
        @Getter
        @Setter
        private Duration segmentMetadataExpiration;

        TestContainerConfig() throws ConfigurationException {
            super(new TypedProperties(new Properties(), "ns"));
        }
    }

    //endregion
}
