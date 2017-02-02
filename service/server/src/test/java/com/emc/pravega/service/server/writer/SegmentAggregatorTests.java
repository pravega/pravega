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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.AutoStopwatch;
import com.emc.pravega.common.io.FixedByteArrayOutputStream;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.SegmentInfo;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.TestStorage;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeTransactionOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ErrorInjector;
import com.emc.pravega.testcommon.IntentionalException;
import com.emc.pravega.testcommon.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SegmentAggregator class.
 */
public class SegmentAggregatorTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 0;
    private static final long SEGMENT_ID = 123;
    private static final String SEGMENT_NAME = "Segment";
    private static final long TRANSACTION_ID_START = 1000000;
    private static final String TRANSACTION_NAME_PREFIX = "Transaction";
    private static final int TRANSACTION_COUNT = 10;
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final AppendContext APPEND_CONTEXT = new AppendContext(UUID.randomUUID(), 0);
    private static final WriterConfig DEFAULT_CONFIG = ConfigHelpers.createWriterConfig(
            PropertyBag.create()
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, 100)
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                       .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 150)
                       .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    //region initialize()
    /**
     * Tests the initialize() method.
     */
    @Test
    public void testInitialize() {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Check behavior for non-existent segments (in Storage).
        context.transactionAggregators[0].initialize(TIMEOUT).join();
        Assert.assertTrue("isDeleted() flag not set on metadata for deleted segment.", context.transactionAggregators[0].getMetadata().isDeleted());

        // Check behavior for already-sealed segments (in storage, but not in metadata)
        context.storage.create(getSegmentInfo(context.transactionAggregators[1].getMetadata()), TIMEOUT).join();
        context.storage.seal(context.transactionAggregators[1].getMetadata().getName(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "initialize() succeeded on a Segment is sealed in Storage but not in the metadata.",
                () -> context.transactionAggregators[1].initialize(TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Check behavior for already-sealed segments (in storage, in metadata, but metadata does not reflect Sealed in storage.)
        context.storage.create(getSegmentInfo(context.transactionAggregators[2].getMetadata()), TIMEOUT).join();
        context.storage.seal(context.transactionAggregators[2].getMetadata().getName(), TIMEOUT).join();
        ((UpdateableSegmentMetadata) context.transactionAggregators[2].getMetadata()).markSealed();
        context.transactionAggregators[2].initialize(TIMEOUT).join();
        Assert.assertTrue("isSealedInStorage() flag not set on metadata for storage-sealed segment.", context.transactionAggregators[2].getMetadata().isSealedInStorage());

        // Check the ability to update Metadata.StorageOffset if it is different.
        final int writeLength = 10;
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.storage.write(context.segmentAggregator.getMetadata().getName(), 0, new ByteArrayInputStream(new byte[writeLength]), writeLength, TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        Assert.assertEquals("SegmentMetadata.StorageLength was not updated after call to initialize().", writeLength, context.segmentAggregator.getMetadata().getStorageLength());
    }

    /**
     * Verifies that no publicly visible operation is allowed if the SegmentAggregator is not initialized.
     */
    @Test
    public void testNotInitialized() {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        AssertExtensions.assertThrows(
                "add() was allowed before initialization.",
                () -> context.segmentAggregator.add(new StreamSegmentAppendOperation(0, new byte[0], APPEND_CONTEXT)),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "add() was allowed before initialization.",
                () -> context.segmentAggregator.flush(TIMEOUT, executorService()),
                ex -> ex instanceof IllegalStateException);
    }

    //endregion

    //region add()

    /**
     * Tests the add() method with valid operations only.
     */
    @Test
    public void testAddValidOperations() throws Exception {
        final int appendCount = 10;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We only needs one Transaction for this test.
        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
        SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();

        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.storage.create(getSegmentInfo(transactionMetadata), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();

        // Verify Appends with correct parameters work as expected.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
            transactionAggregator.add(generateAppendAndUpdateMetadata(i, transactionMetadata.getId(), context));
        }

        // Seal the Transaction and add a MergeTransactionOperation to the parent.
        transactionAggregator.add(generateSealAndUpdateMetadata(transactionMetadata.getId(), context));
        context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(transactionMetadata.getId(), context));

        // Add more appends to the parent.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
        }

        // Seal the parent.
        context.segmentAggregator.add(generateSealAndUpdateMetadata(SEGMENT_ID, context));
    }

    /**
     * Tests the add() method with invalid arguments.
     */
    @Test
    public void testAddWithBadInput() throws Exception {
        final long badTransactionId = 12345;
        final long badParentId = 56789;
        final String badParentName = "Foo_Parent";
        final String badTransactionName = "Foo_Transaction";

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We only needs one Transaction for this test.
        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
        SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();

        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.storage.create(getSegmentInfo(transactionMetadata), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();

        // Create 2 more segments that can be used to verify MergeTransactionOperation.
        context.containerMetadata.mapStreamSegmentId(badParentName, badParentId);
        UpdateableSegmentMetadata badTransactionMetadata = context.containerMetadata.mapStreamSegmentId(badTransactionName, badTransactionId, badParentId);
        badTransactionMetadata.setDurableLogLength(0);
        badTransactionMetadata.setStorageLength(0);
        context.storage.create(getSegmentInfo(badTransactionMetadata), TIMEOUT).join();

        // 1. MergeTransactionOperation
        // 1a.Verify that MergeTransactionOperation cannot be added to the Transaction segment.
        AssertExtensions.assertThrows(
                "add() allowed a MergeTransactionOperation on the Transaction segment.",
                () -> transactionAggregator.add(generateSimpleMergeTransaction(transactionMetadata.getId(), context)),
                ex -> ex instanceof IllegalArgumentException);

        // 1b. Verify that MergeTransactionOperation has the right parent.
        AssertExtensions.assertThrows(
                "add() allowed a MergeTransactionOperation on the parent for a Transaction that did not have it as a parent.",
                () -> transactionAggregator.add(generateSimpleMergeTransaction(badTransactionId, context)),
                ex -> ex instanceof IllegalArgumentException);

        // 2. StreamSegmentSealOperation.
        // 2a. Verify we cannot add a StreamSegmentSealOperation if the segment is not sealed yet.
        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentSealOperation for a non-sealed segment.",
                () -> {
                    @Cleanup
                    SegmentAggregator badTransactionAggregator = new SegmentAggregator(badTransactionMetadata, context.dataSource, context.storage, DEFAULT_CONFIG, context.stopwatch);
                    badTransactionAggregator.initialize(TIMEOUT).join();
                    badTransactionAggregator.add(generateSimpleSeal(badTransactionId, context));
                },
                ex -> ex instanceof DataCorruptionException);

        // 2b. Verify that nothing is allowed after Seal (after adding one append to and sealing the Transaction Segment).
        StorageOperation transactionAppend1 = generateAppendAndUpdateMetadata(0, transactionMetadata.getId(), context);
        transactionAggregator.add(transactionAppend1);
        transactionAggregator.add(generateSealAndUpdateMetadata(transactionMetadata.getId(), context));
        AssertExtensions.assertThrows(
                "add() allowed operation after seal.",
                () -> transactionAggregator.add(generateSimpleAppend(transactionMetadata.getId(), context)),
                ex -> ex instanceof DataCorruptionException);

        // 3. CachedStreamSegmentAppendOperation.
        final StorageOperation parentAppend1 = generateAppendAndUpdateMetadata(0, SEGMENT_ID, context);

        // 3a. Verify we cannot add StreamSegmentAppendOperations.
        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentAppendOperation.",
                () -> {
                    // We have the correct offset, but we did not increase the DurableLogLength.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(
                            parentAppend1.getStreamSegmentId(),
                            parentAppend1.getStreamSegmentOffset(),
                            new byte[(int) parentAppend1.getLength()],
                            new AppendContext(UUID.randomUUID(), 1));
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof IllegalArgumentException);

        // Add this one append to the parent (nothing unusual here); we'll use this for the next tests.
        context.segmentAggregator.add(parentAppend1);

        // 3b. Verify we cannot add anything beyond the DurableLogOffset (offset or offset+length).
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset).",
                () -> {
                    // We have the correct offset, but we did not increase the DurableLogLength.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        ((UpdateableSegmentMetadata) context.segmentAggregator.getMetadata()).setDurableLogLength(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset+length).",
                () -> {
                    // We have the correct offset, but we the append exceeds the DurableLogLength by 1 byte.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        // 3c. Verify contiguity (offsets - we cannot have gaps in the data).
        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too small).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(0);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large, but no pending operations).",
                () -> {
                    @Cleanup
                    SegmentAggregator badTransactionAggregator = new SegmentAggregator(badTransactionMetadata, context.dataSource, context.storage, DEFAULT_CONFIG, context.stopwatch);
                    badTransactionMetadata.setDurableLogLength(100);
                    badTransactionAggregator.initialize(TIMEOUT).join();

                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(1);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        // 4. Verify Segment Id match.
        AssertExtensions.assertThrows(
                "add() allowed an Append operation with wrong Segment Id.",
                () -> {
                    StreamSegmentAppendOperation badIdAppend = new StreamSegmentAppendOperation(Integer.MAX_VALUE, "foo".getBytes(), APPEND_CONTEXT);
                    badIdAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badIdAppend));
                },
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentSealOperation with wrong SegmentId.",
                () -> {
                    StreamSegmentSealOperation badIdSeal = new StreamSegmentSealOperation(Integer.MAX_VALUE);
                    badIdSeal.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badIdSeal);
                },
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "add() allowed a MergeTransactionOperation with wrong SegmentId.",
                () -> {
                    MergeTransactionOperation badIdMerge = new MergeTransactionOperation(Integer.MAX_VALUE, transactionMetadata.getId());
                    badIdMerge.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    badIdMerge.setLength(1);
                    context.segmentAggregator.add(badIdMerge);
                },
                ex -> ex instanceof IllegalArgumentException);
    }

    //endregion

    //region flush()

    /**
     * Tests the flush() method only with Append operations.
     * Verifies both length-based and time-based flush triggers, as well as flushing rather large operations.
     */
    @Test
    public void testFlushAppend() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = config.getFlushThresholdBytes() * 10;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
        AtomicLong outstandingSize = new AtomicLong(); // Number of bytes remaining to be flushed.
        SequenceNumberCalculator sequenceNumbers = new SequenceNumberCalculator(context, outstandingSize);

        // Part 1: flush triggered by accumulated size.
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize.addAndGet(appendOp.getLength());
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
            sequenceNumbers.record(appendOp);

            boolean expectFlush = outstandingSize.get() >= config.getFlushThresholdBytes();
            Assert.assertEquals("Unexpected value returned by mustFlush() (size threshold).", expectFlush, context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (size threshold).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());

            // Call flush() and inspect the result.
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();
            if (expectFlush) {
                AssertExtensions.assertGreaterThanOrEqual("Not enough bytes were flushed (size threshold).", config.getFlushThresholdBytes(), flushResult.getFlushedBytes());
                outstandingSize.addAndGet(-flushResult.getFlushedBytes());
                Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (size threshold).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            } else {
                Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%s, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                        0, flushResult.getFlushedBytes());
            }

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (size threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 2: flush triggered by time.
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize.addAndGet(appendOp.getLength());
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
            sequenceNumbers.record(appendOp);

            // Call flush() and inspect the result.
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 3: Transaction appends. This will force an internal loop inside flush() to do so repeatedly.
        final int transactionSize = 100;
        for (int i = 0; i < appendCount / 10; i++) {
            for (int j = 0; j < transactionSize; j++) {
                // Add another operation and record its length.
                StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
                outstandingSize.addAndGet(appendOp.getLength());
                context.segmentAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
                sequenceNumbers.record(appendOp);
                Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (Transaction appends).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            }

            // Call flush() and inspect the result.
            Assert.assertTrue("Unexpected value returned by mustFlush() (Transaction appends).", context.segmentAggregator.mustFlush());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (Transaction appends).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (Transaction appends).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (Transaction appends).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 4: large appends (larger than MaxFlushSize).
        Random random = new Random();
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            byte[] largeAppendData = new byte[config.getMaxFlushSizeBytes() * 10 + 1];
            random.nextBytes(largeAppendData);
            StorageOperation appendOp = generateAppendAndUpdateMetadata(SEGMENT_ID, largeAppendData, context);
            outstandingSize.addAndGet(appendOp.getLength());
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
            sequenceNumbers.record(appendOp);

            // Call flush() and inspect the result.
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (large appends).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (large appends).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (large appends).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (large appends).", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test (large appends).", 0, flushResult.getMergedBytes());
        }

        // Verify data.
        Assert.assertEquals("Not expecting leftover data not flushed.", 0, outstandingSize.get());
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the behavior of flush() with appends and storage errors (on the write() method).
     */
    @Test
    public void testFlushAppendWithStorageErrors() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = config.getFlushThresholdBytes() * 10;
        final int failSyncEvery = 2;
        final int failAsyncEvery = 3;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Have the writes fail every few attempts with a well known exception.
        AtomicReference<IntentionalException> setException = new AtomicReference<>();
        Supplier<Exception> exceptionSupplier = () -> {
            IntentionalException ex = new IntentionalException(Long.toString(currentTime.get()));
            setException.set(ex);
            return ex;
        };
        context.storage.setWriteSyncErrorInjector(new ErrorInjector<>(count -> count % failSyncEvery == 0, exceptionSupplier));
        context.storage.setWriteAsyncErrorInjector(new ErrorInjector<>(count -> count % failAsyncEvery == 0, exceptionSupplier));

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        // Part 1: flush triggered by accumulated size.
        int exceptionCount = 0;
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);

            // Call flush() and inspect the result.
            setException.set(null);
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            FlushResult flushResult = null;

            try {
                flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), ExceptionHelpers.getRealException(ex));
                    exceptionCount++;
                } else {
                    // Not expecting any exception this time.
                    throw ex;
                }
            }

            // Check flush result.
            if (flushResult != null) {
                AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
                Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            }
        }

        // Do one last flush at the end to make sure we clear out all the buffers, if there's anything else left.
        currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        context.storage.setWriteSyncErrorInjector(null);
        context.storage.setWriteAsyncErrorInjector(null);
        context.segmentAggregator.flush(TIMEOUT, executorService()).join();

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
        AssertExtensions.assertGreaterThan("Not enough errors injected.", 0, exceptionCount);
    }

    /**
     * Tests the flush() method with Append and StreamSegmentSealOperations.
     */
    @Test
    public void testSeal() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        final int appendCount = 1000;
        final WriterConfig config = ConfigHelpers.createWriterConfig(
                PropertyBag.create()
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                           .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 10000)
                           .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        // Accumulate some Appends
        AtomicLong outstandingSize = new AtomicLong();
        SequenceNumberCalculator sequenceNumbers = new SequenceNumberCalculator(context, outstandingSize);
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize.addAndGet(appendOp.getLength());
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
            sequenceNumbers.record(appendOp);

            // Call flush() and verify that we haven't flushed anything (by design).
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();
            Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%s, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                    0, flushResult.getFlushedBytes());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        Assert.assertFalse("Unexpected value returned by mustFlush() before adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after adding StreamSegmentSealOperation.", sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Unexpected value returned by mustFlush() after adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Call flush and verify that the entire Aggregator got flushed and the Seal got persisted to Storage.
        FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize.get(), flushResult.getFlushedBytes());
        Assert.assertFalse("Unexpected value returned by mustFlush() after flushing.", context.segmentAggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flushing.", Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Segment is not sealed in storage post flush.", storageInfo.isSealed());
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentAggregator.getMetadata().isSealedInStorage());
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method when it has a StreamSegmentSealOperation but the Segment is already sealed in Storage.
     */
    @Test
    public void testSealAlreadySealed() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        final WriterConfig config = DEFAULT_CONFIG;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Seal the segment in Storage, behind the scenes.
        context.storage.seal(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();

        // Call flush and verify no exception is thrown.
        context.segmentAggregator.flush(TIMEOUT, executorService()).join();

        // Verify data - even though already sealed, make sure the metadata is updated accordingly.
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentAggregator.getMetadata().isSealedInStorage());
    }

    /**
     * Tests the flush() method with Append and StreamSegmentSealOperations when there are Storage errors.
     */
    @Test
    public void testSealWithStorageErrors() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        final int appendCount = 1000;
        final WriterConfig config = ConfigHelpers.createWriterConfig(
                PropertyBag.create()
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                           .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 10000)
                           .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        // Part 1: flush triggered by accumulated size.
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length (not bothering with flushing here; testFlushSeal() covers that).
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
        }

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Have the writes fail every few attempts with a well known exception.
        AtomicBoolean generateSyncException = new AtomicBoolean(true);
        AtomicBoolean generateAsyncException = new AtomicBoolean(true);
        AtomicReference<IntentionalException> setException = new AtomicReference<>();
        Supplier<Exception> exceptionSupplier = () -> {
            IntentionalException ex = new IntentionalException(Long.toString(currentTime.get()));
            setException.set(ex);
            return ex;
        };
        context.storage.setSealSyncErrorInjector(new ErrorInjector<>(count -> generateSyncException.getAndSet(false), exceptionSupplier));
        context.storage.setSealAsyncErrorInjector(new ErrorInjector<>(count -> generateAsyncException.getAndSet(false), exceptionSupplier));

        // Call flush and verify that the entire Aggregator got flushed and the Seal got persisted to Storage.
        int attemptCount = 4;
        for (int i = 0; i < attemptCount; i++) {
            // Repeat a number of times, at least once should work.
            setException.set(null);
            try {
                FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).join();
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), ExceptionHelpers.getRealException(ex));
                } else {
                    // Not expecting any exception this time.
                    throw ex;
                }
            }

            if (!generateAsyncException.get() && !generateSyncException.get() && setException.get() == null) {
                // We are done. We got at least one through.
                break;
            }
        }

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Segment is not sealed in storage post flush.", storageInfo.isSealed());
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentAggregator.getMetadata().isSealedInStorage());
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method with Append and MergeTransactionOperations.
     * Overall strategy:
     * 1. Create one Parent Segment and N Transaction Segments.
     * 2. Populate all Transaction Segments with data.
     * 3. Seal the first N/2 Transaction Segments.
     * 4. Add some Appends, interspersed with Merge Transaction Ops to the Parent (for all Transactions)
     * 5. Call flush() repeatedly on all Segments, until nothing is flushed anymore. Verify only the first N/2 Transactions were merged.
     * 6. Seal the remaining N/2 Transaction Segments
     * 7. Call flush() repeatedly on all Segments, until nothing is flushed anymore. Verify all Transactions were merged.
     * 8. Verify the Parent Segment has all the data (from itself and its Transactions), in the correct order.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testMerge() throws Exception {
        final int appendCount = 100; // This is number of appends per Segment/Transaction - there will be a lot of appends here.
        final WriterConfig config = ConfigHelpers.createWriterConfig(
                PropertyBag.create()
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                           .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 10000)
                           .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);

        // Create and initialize all segments.
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            context.storage.create(getSegmentInfo(a.getMetadata()), TIMEOUT).join();
            a.initialize(TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();

        // Add a few appends to each Transaction aggregator and to the parent aggregator.
        // Seal the first half of the Transaction aggregators (thus, those Transactions will be fully flushed).
        HashSet<Long> sealedTransactionIds = new HashSet<>();
        for (int i = 0; i < context.transactionAggregators.length; i++) {
            SegmentAggregator transactionAggregator = context.transactionAggregators[i];
            long transactionId = transactionAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(transactionId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, transactionId, context);
                transactionAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            if (i < context.transactionAggregators.length / 2) {
                // We only seal the first half.
                transactionAggregator.add(generateSealAndUpdateMetadata(transactionId, context));
                sealedTransactionIds.add(transactionId);
            }
        }

        // Add MergeTransactionOperations to the parent aggregator, making sure we have both the following cases:
        // * Two or more consecutive MergeTransactionOperations both for Transactions that are sealed and for those that are not.
        // * MergeTransactionOperations with appends interspersed between them (in the parent), both for sealed Transactions and non-sealed Transactions.
        long parentSegmentId = context.segmentAggregator.getMetadata().getId();
        @Cleanup
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        for (int transIndex = 0; transIndex < context.transactionAggregators.length; transIndex++) {
            // Every even step, we add an append (but not for odd-numbered steps).
            // This helps ensure that we have both interspersed appends, and consecutive MergeTransactionOperations in the parent.
            if (transIndex % 2 == 1) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(transIndex, parentSegmentId, context);
                context.segmentAggregator.add(appendOp);
                getAppendData(appendOp, parentData, context);
            }

            // Merge this Transaction into the parent & record its data in the final parent data array.
            long transactionId = context.transactionAggregators[transIndex].getMetadata().getId();
            context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(transactionId, context));

            ByteArrayOutputStream transactionData = dataBySegment.get(transactionId);
            parentData.write(transactionData.toByteArray());
            transactionData.close();
        }

        // Flush all the Aggregators as long as at least one of them reports being able to flush and that it did flush something.
        flushAllSegments(context);

        // Now check to see that only those Transactions that were sealed were merged.
        for (SegmentAggregator transactionAggregator : context.transactionAggregators) {
            SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();
            boolean expectedMerged = sealedTransactionIds.contains(transactionMetadata.getId());

            if (expectedMerged) {
                Assert.assertTrue("Transaction to be merged was not marked as deleted in metadata.", transactionMetadata.isDeleted());
                Assert.assertFalse("Transaction to be merged still exists in storage.", context.storage.exists(transactionMetadata.getName(), TIMEOUT).join());
            } else {
                Assert.assertFalse("Transaction not to be merged was marked as deleted in metadata.", transactionMetadata.isDeleted());
                SegmentProperties sp = context.storage.getStreamSegmentInfo(transactionMetadata.getName(), TIMEOUT).join();
                Assert.assertFalse("Transaction not to be merged is sealed in storage.", sp.isSealed());
            }
        }

        // Then seal the rest of the Transactions and re-run the flush on the parent a few times.
        for (SegmentAggregator a : context.transactionAggregators) {
            long transactionId = a.getMetadata().getId();
            if (!sealedTransactionIds.contains(transactionId)) {
                // This Transaction was not sealed (and merged) previously. Do it now.
                a.add(generateSealAndUpdateMetadata(transactionId, context));
                sealedTransactionIds.add(transactionId);
            }
        }

        // Flush all the Aggregators as long as at least one of them reports being able to flush and that it did flush something.
        flushAllSegments(context);

        // Verify that all Transactions are now fully merged.
        for (SegmentAggregator transactionAggregator : context.transactionAggregators) {
            SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();
            Assert.assertTrue("Merged Transaction was not marked as deleted in metadata.", transactionMetadata.isDeleted());
            Assert.assertFalse("Merged Transaction still exists in storage.", context.storage.exists(transactionMetadata.getName(), TIMEOUT).join());
        }

        // Verify that in the end, the contents of the parents is as expected.
        verifyParentSegmentData(parentData, context);
    }

    /**
     * Tests the flush() method with Append and MergeTransactionOperations.
     */
    @Test
    public void testMergeWithStorageErrors() throws Exception {
        // Storage Errors
        final int appendCount = 100; // This is number of appends per Segment/Transaction - there will be a lot of appends here.
        final int failSyncEvery = 2;
        final int failAsyncEvery = 3;
        final WriterConfig config = ConfigHelpers.createWriterConfig(
                PropertyBag.create()
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                           .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                           .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 10000)
                           .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);

        // Create and initialize all segments.
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            context.storage.create(getSegmentInfo(a.getMetadata()), TIMEOUT).join();
            a.initialize(TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();

        // Add a few appends to each Transaction aggregator and to the parent aggregator and seal all Transactions.
        for (int i = 0; i < context.transactionAggregators.length; i++) {
            SegmentAggregator transactionAggregator = context.transactionAggregators[i];
            long transactionId = transactionAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(transactionId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, transactionId, context);
                transactionAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            transactionAggregator.add(generateSealAndUpdateMetadata(transactionId, context));
        }

        // Merge all the Transactions in the parent Segment.
        @Cleanup
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        for (int transIndex = 0; transIndex < context.transactionAggregators.length; transIndex++) {
            // Merge this Transaction into the parent & record its data in the final parent data array.
            long transactionId = context.transactionAggregators[transIndex].getMetadata().getId();
            context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(transactionId, context));

            ByteArrayOutputStream transactionData = dataBySegment.get(transactionId);
            parentData.write(transactionData.toByteArray());
            transactionData.close();
        }

        // Have the writes fail every few attempts with a well known exception.
        AtomicReference<IntentionalException> setException = new AtomicReference<>();
        Supplier<Exception> exceptionSupplier = () -> {
            IntentionalException ex = new IntentionalException(Long.toString(currentTime.get()));
            setException.set(ex);
            return ex;
        };
        context.storage.setConcatSyncErrorInjector(new ErrorInjector<>(count -> count % failSyncEvery == 0, exceptionSupplier));
        context.storage.setConcatAsyncErrorInjector(new ErrorInjector<>(count -> count % failAsyncEvery == 0, exceptionSupplier));

        // Flush all the Aggregators, while checking that the right errors get handled and can be recovered from.
        tryFlushAllSegments(context, () -> setException.set(null), setException::get);

        // Verify that all Transactions are now fully merged.
        for (SegmentAggregator transactionAggregator : context.transactionAggregators) {
            SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();
            Assert.assertTrue("Merged Transaction was not marked as deleted in metadata.", transactionMetadata.isDeleted());
            Assert.assertFalse("Merged Transaction still exists in storage.", context.storage.exists(transactionMetadata.getName(), TIMEOUT).join());
        }

        // Verify that in the end, the contents of the parents is as expected.
        byte[] expectedData = parentData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed/merged to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    //endregion

    //region Unknown outcome operation reconciliation

    /**
     * Tests the ability of the SegmentAggregator to reconcile AppendOperations (Cached/NonCached).
     */
    @Test
    public void testReconcileAppends() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 1000;
        final int failEvery = 3;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // The writes always succeed, but every few times we return some random error, indicating that they didn't.
        AtomicInteger writeCount = new AtomicInteger();
        AtomicReference<Exception> setException = new AtomicReference<>();
        context.storage.setWriteInterceptor((segmentName, offset, data, length, storage) -> {
            if (writeCount.incrementAndGet() % failEvery == 0) {
                // Time to wreak some havoc.
                return storage.write(segmentName, offset, data, length, TIMEOUT)
                              .thenAccept(v -> {
                                  IntentionalException ex = new IntentionalException(String.format("S=%s,O=%d,L=%d", segmentName, offset, length));
                                  setException.set(ex);
                                  throw ex;
                              });
            } else {
                setException.set(null);
                return null;
            }
        });

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);
        }

        currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        while (context.segmentAggregator.mustFlush()) {
            // Call flush() and inspect the result.
            FlushResult flushResult = null;

            try {
                flushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), ExceptionHelpers.getRealException(ex));
                } else {
                    // Only expecting a BadOffsetException after our own injected exception.
                    Throwable realEx = ExceptionHelpers.getRealException(ex);
                    Assert.assertTrue("Unexpected exception thrown: " + realEx, realEx instanceof BadOffsetException);
                }
            }

            // Check flush result.
            if (flushResult != null) {
                AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
                Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            }

            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        }

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile StreamSegmentSealOperations.
     */
    @Test
    public void testReconcileSeal() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // The seal succeeds, but we throw some random error, indicating that it didn't.
        context.storage.setSealInterceptor((segmentName, storage) -> {
            storage.seal(segmentName, TIMEOUT).join();
            throw new IntentionalException(String.format("S=%s", segmentName));
        });

        // Attempt to seal.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // First time: attempt to flush/seal, which must end in failure.
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ExceptionHelpers.getRealException(ex) instanceof IntentionalException);

        context.storage.setSealInterceptor(null);
        // Second time: we are in reconcilation mode, so flush must succeed (and update internal state based on storage).
        context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify outcome.
        Assert.assertTrue("Segment not marked as sealed in storage (in metadata).", context.segmentAggregator.getMetadata().isSealedInStorage());
        Assert.assertTrue("SegmentAggregator not closed.", context.segmentAggregator.isClosed());
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile MergeTransactionOperations.
     */
    @Test
    public void testReconcileMerge() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 100;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);

        // Create a parent segment and one transaction segment.
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
        context.storage.create(getSegmentInfo(transactionAggregator.getMetadata()), TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();

        // Store written data by segment - so we can check it later.
        ByteArrayOutputStream transactionData = new ByteArrayOutputStream();

        // Add a bunch of data to the transaction.
        for (int appendId = 0; appendId < appendCount; appendId++) {
            StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, transactionAggregator.getMetadata().getId(), context);
            transactionAggregator.add(appendOp);
            getAppendData(appendOp, transactionData, context);
        }

        // Seal & flush everything in the transaction
        transactionAggregator.add(generateSealAndUpdateMetadata(transactionAggregator.getMetadata().getId(), context));
        while (transactionAggregator.mustFlush()) {
            transactionAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // The concat succeeds, but we throw some random error, indicating that it didn't.
        context.storage.setConcatInterceptor((targetSegment, offset, sourceSegment, storage) -> {
            storage.concat(targetSegment, offset, sourceSegment, TIMEOUT).join();
            throw new IntentionalException(String.format("T=%s,O=%d,S=%s", targetSegment, offset, sourceSegment));
        });

        // Attempt to concat.
        StorageOperation sealOp = generateMergeTransactionAndUpdateMetadata(transactionAggregator.getMetadata().getId(), context);
        context.segmentAggregator.add(sealOp);

        // First time: attempt to flush/seal, which must end in failure.
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ExceptionHelpers.getRealException(ex) instanceof IntentionalException);

        // Second time: we are not yet in reconcilation mode, but we are about to detect that the Transaction segment
        // no longer exists
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> ExceptionHelpers.getRealException(ex) instanceof StreamSegmentNotExistsException);

        // Third time: we should be in reconciliation mode, and we should be able to recover from it.
        context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify outcome.
        Assert.assertFalse("Unexpected value from mustFlush() after merger reconciliation.", context.segmentAggregator.mustFlush());
        Assert.assertTrue("Transaction Aggregator not closed.", transactionAggregator.isClosed());

        byte[] expectedData = transactionData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile operations as they are added to it (it detected a possible
     * data corruption, but it does not yet have all the operations it needs to reconcile - it needs to stay in reconciliation
     * mode until all disagreements have been resolved).
     */
    @Test
    public void testProgressiveReconcile() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 1000;
        final int failEvery = 3;
        final int maxFlushLoopCount = 5;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
        ArrayList<StorageOperation> appendOperations = new ArrayList<>();
        ArrayList<InputStream> appendData = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            appendOperations.add(appendOp);
            byte[] ad = new byte[(int) appendOp.getLength()];
            getAppendData(appendOp, new FixedByteArrayOutputStream(ad, 0, ad.length), context);
            appendData.add(new ByteArrayInputStream(ad));
            writtenData.write(ad);
        }

        // Add each operation at at time, and every X appends, write ahead to storage (X-1 appends). This will force a
        // good mix of reconciles and normal appends.
        int errorCount = 0;
        int flushCount = 0;
        for (int i = 0; i < appendOperations.size(); i++) {
            StorageOperation op = appendOperations.get(i);
            context.segmentAggregator.add(op);
            if (i % failEvery == 0) {
                // Corrupt the storage by adding the next failEvery-1 ops to Storage.
                for (int j = i; j < i + failEvery - 1 && j < appendOperations.size(); j++) {
                    long offset = context.storage.getStreamSegmentInfo(SEGMENT_NAME, TIMEOUT).join().getLength();
                    context.storage.write(SEGMENT_NAME, offset, appendData.get(j), appendData.get(j).available(), TIMEOUT).join();
                }
            }
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            int flushLoopCount = 0;
            while (context.segmentAggregator.mustFlush()) {
                try {
                    flushCount++;
                    context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    errorCount++;
                    Assert.assertTrue("", ExceptionHelpers.getRealException(ex) instanceof BadOffsetException);
                }

                flushLoopCount++;
                AssertExtensions.assertLessThan("Too many flush-loops for a single attempt.", maxFlushLoopCount, flushLoopCount);
            }
        }

        AssertExtensions.assertGreaterThan("At least one flush was expected.", 0, flushCount);
        AssertExtensions.assertGreaterThan("At least one BadOffsetException was expected.", 0, errorCount);

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    //endregion

    //region Recovery

    /**
     * Tests a scenario where data that is about to be added already exists in Storage. This would most likely happen
     * in a recovery situation, where we committed the data but did not properly ack/truncate it from the DataSource.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testRecovery() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 100;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(getSegmentInfo(context.segmentAggregator.getMetadata()), TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            context.storage.create(getSegmentInfo(a.getMetadata()), TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();
        ArrayList<StorageOperation> operations = new ArrayList<>();

        // Create a segment and all its Transactions (do not initialize yet).
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        dataBySegment.put(context.segmentAggregator.getMetadata().getId(), parentData);

        // All Transactions have appends (First 1/3 of Transactions just have appends, that exist in Storage as well)
        for (int i = 0; i < context.transactionAggregators.length; i++) {
            SegmentAggregator transactionAggregator = context.transactionAggregators[i];
            long transactionId = transactionAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(transactionId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, transactionId, context);
                operations.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            // Second and third 1/3s of Transactions are sealed, with the seals in storage, but we'll still add them.
            boolean isSealed = i >= context.transactionAggregators.length / 3;
            if (isSealed) {
                operations.add(generateSealAndUpdateMetadata(transactionId, context));
            }

            // Last 1/3 of Transactions are also merged.
            boolean isMerged = isSealed && (i >= context.transactionAggregators.length * 2 / 3);
            if (isMerged) {
                operations.add(generateMergeTransactionAndUpdateMetadata(transactionId, context));
                ByteArrayOutputStream transactionData = dataBySegment.get(transactionId);
                parentData.write(transactionData.toByteArray());
                transactionData.close();
                dataBySegment.remove(transactionId);
            }
        }

        // Populate the storage.
        for (Map.Entry<Long, ByteArrayOutputStream> e : dataBySegment.entrySet()) {
            context.storage.write(
                    context.containerMetadata.getStreamSegmentMetadata(e.getKey()).getName(),
                    0,
                    new ByteArrayInputStream(e.getValue().toByteArray()),
                    e.getValue().size(),
                    TIMEOUT).join();
        }

        for (SegmentAggregator a : context.transactionAggregators) {
            if (a.getMetadata().isSealed()) {
                context.storage.seal(a.getMetadata().getName(), TIMEOUT).join();
            }

            if (a.getMetadata().isMerged() || a.getMetadata().isDeleted()) {
                context.storage.delete(a.getMetadata().getName(), TIMEOUT).join();
            }
        }

        // Now initialize the SegmentAggregators
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            a.initialize(TIMEOUT).join();
        }

        // Add all operations we had so far.
        for (StorageOperation o : operations) {
            int transactionIndex = (int) (o.getStreamSegmentId() - TRANSACTION_ID_START);
            SegmentAggregator a = transactionIndex < 0 ? context.segmentAggregator : context.transactionAggregators[transactionIndex];
            a.add(o);
        }

        // And now finish up the operations (merge all Transactions).
        for (SegmentAggregator a : context.transactionAggregators) {
            if (!a.getMetadata().isSealed()) {
                a.add(generateSealAndUpdateMetadata(a.getMetadata().getId(), context));
            }

            if (!a.getMetadata().isMerged()) {
                context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(a.getMetadata().getId(), context));
                ByteArrayOutputStream transactionData = dataBySegment.get(a.getMetadata().getId());
                parentData.write(transactionData.toByteArray());
                transactionData.close();
                dataBySegment.remove(a.getMetadata().getId());
            }
        }

        flushAllSegments(context);

        // Verify that in the end, the contents of the parents is as expected.
        verifyParentSegmentData(parentData, context);
    }

    //endregion

    //region Helpers

    private void getAppendData(StorageOperation operation, OutputStream stream, TestContext context) {
        Assert.assertTrue("Not an append operation: " + operation, operation instanceof CachedStreamSegmentAppendOperation);
        InputStream result = context.dataSource.getAppendData(operation.getStreamSegmentId(), operation.getStreamSegmentOffset(), (int) operation.getLength());
        try {
            IOUtils.copy(result, stream);
        } catch (IOException ex) {
            Assert.fail("Not expecting this exception: " + ex);
        }
    }

    private StorageOperation generateMergeTransactionAndUpdateMetadata(long transactionId, TestContext context) {
        UpdateableSegmentMetadata transactionMetadata = context.containerMetadata.getStreamSegmentMetadata(transactionId);
        UpdateableSegmentMetadata parentMetadata = context.containerMetadata.getStreamSegmentMetadata(transactionMetadata.getParentId());

        MergeTransactionOperation op = new MergeTransactionOperation(parentMetadata.getId(), transactionMetadata.getId());
        op.setLength(transactionMetadata.getLength());
        op.setStreamSegmentOffset(parentMetadata.getDurableLogLength());

        parentMetadata.setDurableLogLength(parentMetadata.getDurableLogLength() + transactionMetadata.getDurableLogLength());
        transactionMetadata.markMerged();
        return op;
    }

    private StorageOperation generateSimpleMergeTransaction(long transactionId, TestContext context) {
        UpdateableSegmentMetadata transactionMetadata = context.containerMetadata.getStreamSegmentMetadata(transactionId);
        UpdateableSegmentMetadata parentMetadata = context.containerMetadata.getStreamSegmentMetadata(transactionMetadata.getParentId());

        MergeTransactionOperation op = new MergeTransactionOperation(parentMetadata.getId(), transactionMetadata.getId());
        op.setLength(transactionMetadata.getLength());
        op.setStreamSegmentOffset(parentMetadata.getDurableLogLength());

        return op;
    }

    private StorageOperation generateSealAndUpdateMetadata(long segmentId, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        segmentMetadata.markSealed();
        return generateSimpleSeal(segmentId, context);
    }

    private StorageOperation generateSimpleSeal(long segmentId, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        StreamSegmentSealOperation sealOp = new StreamSegmentSealOperation(segmentId);
        sealOp.setStreamSegmentOffset(segmentMetadata.getDurableLogLength());
        sealOp.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return sealOp;
    }

    private StorageOperation generateAppendAndUpdateMetadata(int appendId, long segmentId, TestContext context) {
        byte[] data = String.format("Append_%d", appendId).getBytes();
        return generateAppendAndUpdateMetadata(segmentId, data, context);
    }

    private StorageOperation generateAppendAndUpdateMetadata(long segmentId, byte[] data, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long offset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(offset + data.length);
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, data, APPEND_CONTEXT);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());

        context.dataSource.recordAppend(op);
        return new CachedStreamSegmentAppendOperation(op);
    }

    private StorageOperation generateSimpleAppend(long segmentId, TestContext context) {
        byte[] data = "Append_Dummy".getBytes();
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long offset = segmentMetadata.getDurableLogLength();
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, data, APPEND_CONTEXT);
        op.setStreamSegmentOffset(offset);
        return op;
    }

    private void flushAllSegments(TestContext context) throws Exception {
        // Flush all segments in the TestContext, as long as any of them still has something to flush and is able
        // to flush anything.
        boolean anythingFlushed = true;
        while (anythingFlushed) {
            anythingFlushed = false;
            for (SegmentAggregator transactionAggregator : context.transactionAggregators) {
                if (transactionAggregator.mustFlush()) {
                    FlushResult transactionFlushResult = transactionAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    anythingFlushed = anythingFlushed | transactionFlushResult.getFlushedBytes() > 0;
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                FlushResult parentFlushResult = context.segmentAggregator.flush(TIMEOUT, executorService()).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                anythingFlushed = anythingFlushed | (parentFlushResult.getFlushedBytes() + parentFlushResult.getMergedBytes()) > 0;
            }
        }
    }

    private <T extends Throwable> void tryFlushAllSegments(TestContext context, Runnable exceptionReset, Supplier<T> exceptionProvider) {
        // Flush all segments in the TestContext, as long as any of them still has something to flush and is able to
        // flush anything, or an exception was thrown (and expected).
        boolean anythingFlushed = true;
        while (anythingFlushed) {
            anythingFlushed = false;
            for (SegmentAggregator transactionAggregator : context.transactionAggregators) {
                if (transactionAggregator.mustFlush()) {
                    exceptionReset.run();
                    FlushResult transactionFlushResult = tryFlushSegment(transactionAggregator, exceptionProvider, executorService());
                    anythingFlushed = anythingFlushed | (transactionFlushResult == null || transactionFlushResult.getFlushedBytes() > 0);
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                exceptionReset.run();
                FlushResult parentFlushResult = tryFlushSegment(context.segmentAggregator, exceptionProvider, executorService());
                anythingFlushed = anythingFlushed | (parentFlushResult == null || (parentFlushResult.getFlushedBytes() + parentFlushResult.getMergedBytes()) > 0);
            }
        }
    }

    private <T extends Throwable> FlushResult tryFlushSegment(SegmentAggregator aggregator, Supplier<T> exceptionProvider, Executor executor) {
        try {
            FlushResult flushResult = aggregator.flush(TIMEOUT, executor).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            T expectedException = exceptionProvider.get();
            Assert.assertNull("Expected an exception but none got thrown.", expectedException);
            Assert.assertNotNull("Expected a FlushResult.", flushResult);
            return flushResult;
        } catch (Throwable ex) {
            ex = ExceptionHelpers.getRealException(ex);
            T expectedException = exceptionProvider.get();
            Assert.assertEquals("Unexpected exception or no exception got thrown.", expectedException, ex);
            return null;
        }
    }

    private void verifyParentSegmentData(ByteArrayOutputStream parentData, TestContext context) {
        byte[] expectedData = parentData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed/merged to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentAggregator.getMetadata().getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    //endregion

    // region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata containerMetadata;
        final TestWriterDataSource dataSource;
        final TestStorage storage;
        final AutoStopwatch stopwatch;
        final SegmentAggregator segmentAggregator;
        final SegmentAggregator[] transactionAggregators;

        TestContext(WriterConfig config) {
            this(config, System::currentTimeMillis);
        }

        TestContext(WriterConfig config, Supplier<Long> stopwatchGetMillis) {
            this.containerMetadata = new StreamSegmentContainerMetadata(CONTAINER_ID);

            this.storage = new TestStorage(new InMemoryStorage(executorService()));
            this.stopwatch = new AutoStopwatch(stopwatchGetMillis);

            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = TestWriterDataSource.DataSourceConfig.NO_METADATA_CHECKPOINT;
            this.dataSource = new TestWriterDataSource(this.containerMetadata, executorService(), dataSourceConfig);

            this.transactionAggregators = new SegmentAggregator[TRANSACTION_COUNT];
            UpdateableSegmentMetadata segmentMetadata = initialize(this.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID));
            this.segmentAggregator = new SegmentAggregator(segmentMetadata, this.dataSource, this.storage, config, this.stopwatch);
            for (int i = 0; i < TRANSACTION_COUNT; i++) {
                String name = TRANSACTION_NAME_PREFIX + i;
                UpdateableSegmentMetadata transactionMetadata = initialize(this.containerMetadata.mapStreamSegmentId(name, TRANSACTION_ID_START + i, SEGMENT_ID));
                this.transactionAggregators[i] = new SegmentAggregator(transactionMetadata, this.dataSource, this.storage, config, this.stopwatch);
            }
        }

        @Override
        public void close() {
            for (SegmentAggregator aggregator : this.transactionAggregators) {
                aggregator.close();
            }

            this.segmentAggregator.close();
            this.dataSource.close();
            this.storage.close();
        }

        private UpdateableSegmentMetadata initialize(UpdateableSegmentMetadata segmentMetadata) {
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setDurableLogLength(0);
            return segmentMetadata;
        }
    }

    // endregion

    // region SequenceNumberCalculator

    @RequiredArgsConstructor
    private static class SequenceNumberCalculator {
        private final TreeMap<Long, Long> sequenceNumbers = new TreeMap<>();
        private final TestContext context;
        private final AtomicLong outstandingSize;

        void record(StorageOperation op) {
            this.sequenceNumbers.put(op.getStreamSegmentOffset(), op.getSequenceNumber());
        }

        long getLowestUncommitted() {
            return outstandingSize.get() <= 0 ?
                    Operation.NO_SEQUENCE_NUMBER :
                    this.sequenceNumbers.floorEntry(this.context.segmentAggregator.getMetadata().getStorageLength()).getValue();
        }
    }

    private SegmentInfo getSegmentInfo(SegmentMetadata sm) {
        return SegmentInfo.noAutoScale(sm.getName());
    }
    // endregion
}
