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
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.CloseableExecutorService;
import com.emc.pravega.service.server.ConfigHelpers;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.PropertyBag;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.TestStorage;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.mocks.InMemoryCache;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.ErrorInjector;
import com.emc.pravega.testcommon.IntentionalException;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Unit tests for the SegmentAggregator class.
 */
public class SegmentAggregatorTests {
    private static final int CONTAINER_ID = 0;
    private static final long SEGMENT_ID = 123;
    private static final String SEGMENT_NAME = "Segment";
    private static final long BATCH_ID_START = 1000000;
    private static final String BATCH_NAME_PREFIX = "Batch";
    private static final int BATCH_COUNT = 10;
    private static final int THREAD_POOL_SIZE = 10; // We only need this for storage, so no need for a big pool.
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final AppendContext APPEND_CONTEXT = new AppendContext(UUID.randomUUID(), 0);
    private static final WriterConfig DEFAULT_CONFIG = ConfigHelpers.createWriterConfig(
            PropertyBag.create()
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_BYTES, 100)
                       .with(WriterConfig.PROPERTY_FLUSH_THRESHOLD_MILLIS, 1000)
                       .with(WriterConfig.PROPERTY_MAX_FLUSH_SIZE_BYTES, 150)
                       .with(WriterConfig.PROPERTY_MIN_READ_TIMEOUT_MILLIS, 10));

    //region initialize()

    /**
     * Tests the initialize() method.
     */
    @Test
    public void testInitialize() {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Check behavior for non-existent segments (in Storage).
        context.batchesAggregators[0].initialize(TIMEOUT).join();
        Assert.assertTrue("isDeleted() flag not set on metadata for deleted segment.", context.batchesAggregators[0].getMetadata().isDeleted());

        // Check behavior for already-sealed segments (in storage, but not in metadata)
        context.storage.create(context.batchesAggregators[1].getMetadata().getName(), TIMEOUT).join();
        context.storage.seal(context.batchesAggregators[1].getMetadata().getName(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "initialize() succeeded on a Segment is sealed in Storage but not in the metadata.",
                () -> context.batchesAggregators[1].initialize(TIMEOUT),
                ex -> ex instanceof RuntimeStreamingException && ex.getCause() instanceof DataCorruptionException);

        // Check behavior for already-sealed segments (in storage, in metadata, but metadata does not reflect Sealed in storage.)
        context.storage.create(context.batchesAggregators[2].getMetadata().getName(), TIMEOUT).join();
        context.storage.seal(context.batchesAggregators[2].getMetadata().getName(), TIMEOUT).join();
        ((UpdateableSegmentMetadata) context.batchesAggregators[2].getMetadata()).markSealed();
        context.batchesAggregators[2].initialize(TIMEOUT).join();
        Assert.assertTrue("isSealedInStorage() flag not set on metadata for storage-sealed segment.", context.batchesAggregators[2].getMetadata().isSealedInStorage());

        // Check the ability to update Metadata.StorageOffset if it is different.
        final int writeLength = 10;
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
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
                () -> context.segmentAggregator.flush(TIMEOUT),
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

        // We only needs one batch for this test.
        SegmentAggregator batchAggregator = context.batchesAggregators[0];
        SegmentMetadata batchMetadata = batchAggregator.getMetadata();

        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.storage.create(batchMetadata.getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        batchAggregator.initialize(TIMEOUT).join();

        // Verify Appends with correct parameters work as expected.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
            batchAggregator.add(generateAppendAndUpdateMetadata(i, batchMetadata.getId(), context));
        }

        // Seal the batch and add a MergeBatchOperation to the parent.
        batchAggregator.add(generateSealAndUpdateMetadata(batchMetadata.getId(), context));
        context.segmentAggregator.add(generateMergeBatchAndUpdateMetadata(batchMetadata.getId(), context));

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
        final long badBatchId = 12345;
        final long badParentId = 56789;
        final String badParentName = "Foo_Parent";
        final String badBatchName = "Foo_Batch";

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We only needs one batch for this test.
        SegmentAggregator batchAggregator = context.batchesAggregators[0];
        SegmentMetadata batchMetadata = batchAggregator.getMetadata();

        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.storage.create(batchMetadata.getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        batchAggregator.initialize(TIMEOUT).join();

        // Create 2 more segments that can be used to verify MergeBatchOperation.
        context.containerMetadata.mapStreamSegmentId(badParentName, badParentId);
        UpdateableSegmentMetadata badBatchMeta = context.containerMetadata.mapStreamSegmentId(badBatchName, badBatchId, badParentId);
        badBatchMeta.setDurableLogLength(0);
        badBatchMeta.setStorageLength(0);
        context.storage.create(badBatchMeta.getName(), TIMEOUT).join();

        // 1. MergeBatchOperation
        // 1a.Verify that MergeBatchOperation cannot be added to the batch segment.
        AssertExtensions.assertThrows(
                "add() allowed a MergeBatchOperation on the batch segment.",
                () -> batchAggregator.add(generateSimpleMergeBatch(batchMetadata.getId(), context)),
                ex -> ex instanceof IllegalArgumentException);

        // 1b. Verify that MergeBatchOperation has the right parent.
        AssertExtensions.assertThrows(
                "add() allowed a MergeBatchOperation on the parent for a Batch that did not have it as a parent.",
                () -> batchAggregator.add(generateSimpleMergeBatch(badBatchId, context)),
                ex -> ex instanceof IllegalArgumentException);

        // 2. StreamSegmentSealOperation.
        // 2a. Verify we cannot add a StreamSegmentSealOperation if the segment is not sealed yet.
        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentSealOperation for a non-sealed segment.",
                () -> {
                    @Cleanup
                    SegmentAggregator badBatchAggregator = new SegmentAggregator(badBatchMeta, context.dataSource, context.storage, DEFAULT_CONFIG, context.stopwatch);
                    badBatchAggregator.initialize(TIMEOUT).join();
                    badBatchAggregator.add(generateSimpleSeal(badBatchId, context));
                },
                ex -> ex instanceof DataCorruptionException);

        // 2b. Verify that nothing is allowed after Seal (after adding one append to and sealing the Batch Segment).
        StorageOperation batchAppend1 = generateAppendAndUpdateMetadata(0, batchMetadata.getId(), context);
        batchAggregator.add(batchAppend1);
        batchAggregator.add(generateSealAndUpdateMetadata(batchMetadata.getId(), context));
        AssertExtensions.assertThrows(
                "add() allowed operation after seal.",
                () -> batchAggregator.add(generateSimpleAppend(batchMetadata.getId(), context)),
                ex -> ex instanceof DataCorruptionException);

        // 3. (Cached)StreamSegmentAppendOperation.
        // 3a. Add one append to the parent (nothing unusual here).
        StorageOperation parentAppend1 = generateAppendAndUpdateMetadata(0, SEGMENT_ID, context);
        context.segmentAggregator.add(parentAppend1);

        // 3b. Verify we cannot add anything beyond the DurableLogOffset (offset or offset+length).
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset).",
                () -> {
                    // We have the correct offset, but we did not increase the DurableLogLength.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        ((UpdateableSegmentMetadata) context.segmentAggregator.getMetadata()).setDurableLogLength(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset+length).",
                () -> {
                    // We have the correct offset, but we the append exceeds the DurableLogLength by 1 byte.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        // 3c. Verify contiguity (offsets - we cannot have gaps in the data).
        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too small).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(0);
                    context.segmentAggregator.add(badOffsetAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
                    context.segmentAggregator.add(badOffsetAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large, but no pending operations).",
                () -> {
                    @Cleanup
                    SegmentAggregator badBatchAggregator = new SegmentAggregator(badBatchMeta, context.dataSource, context.storage, DEFAULT_CONFIG, context.stopwatch);
                    badBatchMeta.setDurableLogLength(100);
                    badBatchAggregator.initialize(TIMEOUT).join();

                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(1);
                    context.segmentAggregator.add(badOffsetAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        // 4. Verify Segment Id match.
        AssertExtensions.assertThrows(
                "add() allowed an Append operation with wrong Segment Id.",
                () -> {
                    StreamSegmentAppendOperation badIdAppend = new StreamSegmentAppendOperation(Integer.MAX_VALUE, "foo".getBytes(), APPEND_CONTEXT);
                    badIdAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badIdAppend);
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
                "add() allowed a MergeBatchOperation with wrong SegmentId.",
                () -> {
                    MergeBatchOperation badIdMerge = new MergeBatchOperation(Integer.MAX_VALUE, batchMetadata.getId());
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        // Part 1: flush triggered by accumulated size.
        long outstandingSize = 0;
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize += appendOp.getLength();
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);

            boolean expectFlush = outstandingSize >= config.getFlushThresholdBytes();
            Assert.assertEquals("Unexpected value returned by mustFlush() (size threshold).", expectFlush, context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (size threshold).", appendOp.getSequenceNumber(), context.segmentAggregator.getLowestUncommittedSequenceNumber());

            // Call flush() and inspect the result.
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
            if (expectFlush) {
                AssertExtensions.assertGreaterThanOrEqual("Not enough bytes were flushed (size threshold).", config.getFlushThresholdBytes(), flushResult.getFlushedBytes());
                outstandingSize -= flushResult.getFlushedBytes();
            } else {
                Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%d, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                        0, flushResult.getFlushedBytes());
            }

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (size threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (size threshold).", Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 2: flush triggered by time.
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize += appendOp.getLength();
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);

            // Call flush() and inspect the result.
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).", appendOp.getSequenceNumber(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
            outstandingSize -= flushResult.getFlushedBytes();

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).", Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 3: batch appends. This will force an internal loop inside flush() to do so repeatedly.
        final int batchSize = 100;
        for (int i = 0; i < appendCount / 10; i++) {
            for (int j = 0; j < batchSize; j++) {
                // Add another operation and record its length.
                StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
                outstandingSize += appendOp.getLength();
                context.segmentAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
                Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (batch appends).", appendOp.getSequenceNumber(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            }

            // Call flush() and inspect the result.
            Assert.assertTrue("Unexpected value returned by mustFlush() (batch appends).", context.segmentAggregator.mustFlush());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (batch appends).", 0, flushResult.getFlushedBytes());
            outstandingSize -= flushResult.getFlushedBytes();

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (batch appends).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (batch appends).", Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 4: large appends (larger than MaxFlushSize).
        Random random = new Random();
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            byte[] largeAppendData = new byte[config.getMaxFlushSizeBytes() * 10 + 1];
            random.nextBytes(largeAppendData);
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, largeAppendData, context);
            outstandingSize += appendOp.getLength();
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);

            // Call flush() and inspect the result.
            currentTime.set(currentTime.get() + config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).", appendOp.getSequenceNumber(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
            outstandingSize -= flushResult.getFlushedBytes();

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).", Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Verify data.
        Assert.assertEquals("Not expecting leftover data not flushed.", 0, outstandingSize);
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
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
                flushResult = context.segmentAggregator.flush(TIMEOUT).join();
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

            // Check flush result.
            if (flushResult != null) {
                AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
                Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            }
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
     * Tests the flush() method with Append and StreamSegmentSealOperations.
     */
    @Test
    public void testFlushSeal() throws Exception {
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();

        // Accumulate some Appends
        long outstandingSize = 0;
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            outstandingSize += appendOp.getLength();
            context.segmentAggregator.add(appendOp);
            getAppendData(appendOp, writtenData, context);

            // Call flush() and verify that we haven't flushed anything (by design).
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
            Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%d, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                    0, flushResult.getFlushedBytes());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        Assert.assertFalse("Unexpected value returned by mustFlush() before adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after adding StreamSegmentSealOperation.", sealOp.getSequenceNumber(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Unexpected value returned by mustFlush() after adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Call flush and verify that the entire Aggregator got flushed and the Seal got persisted to Storage.
        FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize, flushResult.getFlushedBytes());
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
    public void testFlushSealAlreadySealed() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        final WriterConfig config = DEFAULT_CONFIG;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Seal the segment in Storage, behind the scenes.
        context.storage.seal(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();

        // Call flush and verify no exception is thrown.
        context.segmentAggregator.flush(TIMEOUT).join();

        // Verify data - even though already sealed, make sure the metadata is updated accordingly.
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentAggregator.getMetadata().isSealedInStorage());
    }

    /**
     * Tests the flush() method with Append and StreamSegmentSealOperations when there are Storage errors.
     */
    @Test
    public void testFlushSealWithStorageErrors() throws Exception {
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
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
                FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
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
     * Tests the flush() method with Append and MergeBatchOperations.
     * Overall strategy:
     * 1. Create one Parent Segment and N Batch Segments.
     * 2. Populate all Batch Segments with data.
     * 3. Seal the first N/2 Batch Segments.
     * 4. Add some Appends, interspersed with Merge Batch Ops to the Parent (for all Batches)
     * 5. Call flush() repeatedly on all Segments, until nothing is flushed anymore. Verify only the first N/2 batches were merged.
     * 6. Seal the remaining N/2 Batch Segments
     * 7. Call flush() repeatedly on all Segments, until nothing is flushed anymore. Verify all batches were merged.
     * 8. Verify the Parent Segment has all the data (from itself and its batches), in the correct order.
     */
    @Test
    public void testFlushMerge() throws Exception {
        final int appendCount = 100; // This is number of appends per Segment/Batch - there will be a lot of appends here.
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.batchesAggregators) {
            context.storage.create(a.getMetadata().getName(), TIMEOUT).join();
            a.initialize(TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();

        // Add a few appends to each batch aggregator and to the parent aggregator.
        // Seal the first half of the batch aggregators (thus, those batches will be fully flushed).
        HashSet<Long> sealedBatchIds = new HashSet<>();
        for (int i = 0; i < context.batchesAggregators.length; i++) {
            SegmentAggregator batchAggregator = context.batchesAggregators[i];
            long batchId = batchAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(batchId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, batchId, context);
                batchAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            if (i < context.batchesAggregators.length / 2) {
                // We only seal the first half.
                batchAggregator.add(generateSealAndUpdateMetadata(batchId, context));
                sealedBatchIds.add(batchId);
            }
        }

        // Add MergeBatchOperations to the parent aggregator, making sure we have both the following cases:
        // * Two or more consecutive MergeBatchOperations both for Batches that are sealed and for those that are not.
        // * MergeBatchOperations with appends interspersed between them (in the parent), both for sealed Batches and non-sealed batches.
        long parentSegmentId = context.segmentAggregator.getMetadata().getId();
        @Cleanup
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        for (int batchIndex = 0; batchIndex < context.batchesAggregators.length; batchIndex++) {
            // Every even step, we add an append (but not for odd-numbered steps).
            // This helps ensure that we have both interspersed appends, and consecutive MergeBatchOperations in the parent.
            if (batchIndex % 2 == 1) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(batchIndex, parentSegmentId, context);
                context.segmentAggregator.add(appendOp);
                getAppendData(appendOp, parentData, context);
            }

            // Merge this Batch into the parent & record its data in the final parent data array.
            long batchId = context.batchesAggregators[batchIndex].getMetadata().getId();
            context.segmentAggregator.add(generateMergeBatchAndUpdateMetadata(batchId, context));

            ByteArrayOutputStream batchData = dataBySegment.get(batchId);
            parentData.write(batchData.toByteArray());
            batchData.close();
        }

        // Flush all the Aggregators as long as at least one of them reports being able to flush and that it did flush something.
        flushAllSegments(context);

        // Now check to see that only those batches that were sealed were merged.
        for (SegmentAggregator batchAggregator : context.batchesAggregators) {
            SegmentMetadata batchMetadata = batchAggregator.getMetadata();
            boolean expectedMerged = sealedBatchIds.contains(batchMetadata.getId());

            if (expectedMerged) {
                Assert.assertTrue("BatchSegment to be merged was not marked as deleted in metadata.", batchMetadata.isDeleted());
                AssertExtensions.assertThrows(
                        "BatchSegment to be merged still exists in storage.",
                        () -> context.storage.getStreamSegmentInfo(batchMetadata.getName(), TIMEOUT),
                        ex -> ex instanceof StreamSegmentNotExistsException);
            } else {
                Assert.assertFalse("BatchSegment not to be merged was marked as deleted in metadata.", batchMetadata.isDeleted());
                SegmentProperties sp = context.storage.getStreamSegmentInfo(batchMetadata.getName(), TIMEOUT).join();
                Assert.assertFalse("BatchSegment not to be merged is sealed in storage.", sp.isSealed());
            }
        }

        // Then seal the rest of the batches and re-run the flush on the parent a few times.
        for (SegmentAggregator a : context.batchesAggregators) {
            long batchId = a.getMetadata().getId();
            if (!sealedBatchIds.contains(batchId)) {
                // This batch was not sealed (and merged) previously. Do it now.
                a.add(generateSealAndUpdateMetadata(batchId, context));
                sealedBatchIds.add(batchId);
            }
        }

        // Flush all the Aggregators as long as at least one of them reports being able to flush and that it did flush something.
        flushAllSegments(context);

        // Verify that all batches are now fully merged.
        for (SegmentAggregator batchAggregator : context.batchesAggregators) {
            SegmentMetadata batchMetadata = batchAggregator.getMetadata();
            Assert.assertTrue("Merged BatchSegment was not marked as deleted in metadata.", batchMetadata.isDeleted());
            AssertExtensions.assertThrows(
                    "Merged BatchSegment still exists in storage.",
                    () -> context.storage.getStreamSegmentInfo(batchMetadata.getName(), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }

        // Verify that in the end, the contents of the parents is as expected.
        verifyParentSegmentData(parentData, context);
    }

    /**
     * Tests the flush() method with Append and MergeBatchOperations.
     */
    @Test
    public void testFlushMergeWithStorageErrors() throws Exception {
        // Storage Errors
        final int appendCount = 100; // This is number of appends per Segment/Batch - there will be a lot of appends here.
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
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.batchesAggregators) {
            context.storage.create(a.getMetadata().getName(), TIMEOUT).join();
            a.initialize(TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();

        // Add a few appends to each batch aggregator and to the parent aggregator and seal all batches.
        for (int i = 0; i < context.batchesAggregators.length; i++) {
            SegmentAggregator batchAggregator = context.batchesAggregators[i];
            long batchId = batchAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(batchId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, batchId, context);
                batchAggregator.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            batchAggregator.add(generateSealAndUpdateMetadata(batchId, context));
        }

        // Merge all the batches in the parent Segment.
        @Cleanup
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        for (int batchIndex = 0; batchIndex < context.batchesAggregators.length; batchIndex++) {
            // Merge this Batch into the parent & record its data in the final parent data array.
            long batchId = context.batchesAggregators[batchIndex].getMetadata().getId();
            context.segmentAggregator.add(generateMergeBatchAndUpdateMetadata(batchId, context));

            ByteArrayOutputStream batchData = dataBySegment.get(batchId);
            parentData.write(batchData.toByteArray());
            batchData.close();
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

        // Verify that all batches are now fully merged.
        for (SegmentAggregator batchAggregator : context.batchesAggregators) {
            SegmentMetadata batchMetadata = batchAggregator.getMetadata();
            Assert.assertTrue("Merged BatchSegment was not marked as deleted in metadata.", batchMetadata.isDeleted());
            AssertExtensions.assertThrows(
                    "Merged BatchSegment still exists in storage.",
                    () -> context.storage.getStreamSegmentInfo(batchMetadata.getName(), TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);
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

    //region Recovery

    /**
     * Tests a scenario where data that is about to be added already exists in Storage. This would most likely happen
     * in a recovery situation, where we committed the data but did not properly ack/truncate it from the DataSource.
     */
    @Test
    public void testRecovery() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 100;

        // We use this currentTime to simulate time passage - trigger based on time thresholds.
        final AtomicLong currentTime = new AtomicLong();
        @Cleanup
        TestContext context = new TestContext(config, currentTime::get);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        for (SegmentAggregator a : context.batchesAggregators) {
            context.storage.create(a.getMetadata().getName(), TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();
        ArrayList<StorageOperation> operations = new ArrayList<>();

        // Create a segment and all its batches (do not initialize yet).
        ArrayList<StorageOperation> parentOperations = new ArrayList<>();
        ByteArrayOutputStream parentData = new ByteArrayOutputStream();
        dataBySegment.put(context.segmentAggregator.getMetadata().getId(), parentData);

        // All batches have appends (First 1/3 of batches just have appends, that exist in Storage as well)
        for (int i = 0; i < context.batchesAggregators.length; i++) {
            SegmentAggregator batchAggregator = context.batchesAggregators[i];
            long batchId = batchAggregator.getMetadata().getId();
            ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
            dataBySegment.put(batchId, writtenData);

            for (int appendId = 0; appendId < appendCount; appendId++) {
                StorageOperation appendOp = generateAppendAndUpdateMetadata(appendId, batchId, context);
                operations.add(appendOp);
                getAppendData(appendOp, writtenData, context);
            }

            // Second and third 1/3s of batches are sealed, with the seals in storage, but we'll still add them.
            boolean isSealed = i >= context.batchesAggregators.length / 3;
            if (isSealed) {
                operations.add(generateSealAndUpdateMetadata(batchId, context));
            }

            // Last 1/3 of batches are also merged.
            boolean isMerged = isSealed && (i >= context.batchesAggregators.length * 2 / 3);
            if (isMerged) {
                parentOperations.add(generateMergeBatchAndUpdateMetadata(batchId, context));
                ByteArrayOutputStream batchData = dataBySegment.get(batchId);
                parentData.write(batchData.toByteArray());
                batchData.close();
                dataBySegment.remove(batchId);
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

        for (SegmentAggregator a : context.batchesAggregators) {
            if (a.getMetadata().isSealed()) {
                context.storage.seal(a.getMetadata().getName(), TIMEOUT).join();
            }

            if (a.getMetadata().isMerged() || a.getMetadata().isDeleted()) {
                context.storage.delete(a.getMetadata().getName(), TIMEOUT).join();
            }
        }

        // Now initialize the SegmentAggregators
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.batchesAggregators) {
            a.initialize(TIMEOUT).join();
        }

        // Add all operations we had so far.
        for (StorageOperation o : operations) {
            int batchIndex = (int) (o.getStreamSegmentId() - BATCH_ID_START);
            SegmentAggregator a = batchIndex < 0 ? context.segmentAggregator : context.batchesAggregators[batchIndex];
            a.add(o);
        }

        // And now finish up the operations (merge all batches).
        for (SegmentAggregator a : context.batchesAggregators) {
            if (!a.getMetadata().isSealed()) {
                a.add(generateSealAndUpdateMetadata(a.getMetadata().getId(), context));
            }

            if (!a.getMetadata().isMerged()) {
                context.segmentAggregator.add(generateMergeBatchAndUpdateMetadata(a.getMetadata().getId(), context));
                ByteArrayOutputStream batchData = dataBySegment.get(a.getMetadata().getId());
                parentData.write(batchData.toByteArray());
                batchData.close();
                dataBySegment.remove(a.getMetadata().getId());
            }
        }

        flushAllSegments(context);

        // Verify that in the end, the contents of the parents is as expected.
        verifyParentSegmentData(parentData, context);
    }

    //endregion

    //region Helpers

    private byte[] getAppendData(StorageOperation operation, ByteArrayOutputStream stream, TestContext context) {
        byte[] result = null;
        if (operation instanceof StreamSegmentAppendOperation) {
            result = ((StreamSegmentAppendOperation) operation).getData();
        } else if (operation instanceof CachedStreamSegmentAppendOperation) {
            result = context.cache.get(((CachedStreamSegmentAppendOperation) operation).getCacheKey());
        } else {
            Assert.fail("Not an append operation: " + operation);
        }

        try {
            stream.write(result);
        } catch (IOException ex) {
            Assert.fail("Not expecting this exception: " + ex);
        }

        return result;
    }

    private StorageOperation generateMergeBatchAndUpdateMetadata(long batchId, TestContext context) {
        UpdateableSegmentMetadata batchMetadata = context.containerMetadata.getStreamSegmentMetadata(batchId);
        UpdateableSegmentMetadata parentMetadata = context.containerMetadata.getStreamSegmentMetadata(batchMetadata.getParentId());

        MergeBatchOperation op = new MergeBatchOperation(parentMetadata.getId(), batchMetadata.getId());
        op.setLength(batchMetadata.getLength());
        op.setStreamSegmentOffset(parentMetadata.getDurableLogLength());

        parentMetadata.setDurableLogLength(parentMetadata.getDurableLogLength() + batchMetadata.getDurableLogLength());
        batchMetadata.markMerged();
        return op;
    }

    private StorageOperation generateSimpleMergeBatch(long batchId, TestContext context) {
        UpdateableSegmentMetadata batchMetadata = context.containerMetadata.getStreamSegmentMetadata(batchId);
        UpdateableSegmentMetadata parentMetadata = context.containerMetadata.getStreamSegmentMetadata(batchMetadata.getParentId());

        MergeBatchOperation op = new MergeBatchOperation(parentMetadata.getId(), batchMetadata.getId());
        op.setLength(batchMetadata.getLength());
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
        return sealOp;
    }

    private StorageOperation generateAppendAndUpdateMetadata(int appendId, long segmentId, TestContext context) {
        byte[] data = String.format("Append_%d", appendId).getBytes();
        return generateAppendAndUpdateMetadata(appendId, segmentId, data, context);
    }

    private StorageOperation generateAppendAndUpdateMetadata(int appendId, long segmentId, byte[] data, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long offset = segmentMetadata.getDurableLogLength();
        segmentMetadata.setDurableLogLength(offset + data.length);
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, data, APPEND_CONTEXT);
        op.setStreamSegmentOffset(offset);
        if (appendId % 2 == 0) {
            CacheKey key = new CacheKey(segmentId, offset);
            context.cache.insert(key, data);
            return new CachedStreamSegmentAppendOperation(op, key);
        } else {
            return op;
        }
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
            for (SegmentAggregator batchAggregator : context.batchesAggregators) {
                if (batchAggregator.mustFlush()) {
                    FlushResult batchFlushResult = batchAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    anythingFlushed = anythingFlushed | batchFlushResult.getFlushedBytes() > 0;
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                FlushResult parentFlushResult = context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                anythingFlushed = anythingFlushed | (parentFlushResult.getFlushedBytes() + parentFlushResult.getMergedBytes()) > 0;
            }
        }
    }

    private <T extends Throwable> void tryFlushAllSegments(TestContext context, Runnable exceptionReset, Supplier<T> exceptionProvider) throws Exception {
        // Flush all segments in the TestContext, as long as any of them still has something to flush and is able to
        // flush anything, or an exception was thrown (and expected).
        boolean anythingFlushed = true;
        while (anythingFlushed) {
            anythingFlushed = false;
            for (SegmentAggregator batchAggregator : context.batchesAggregators) {
                if (batchAggregator.mustFlush()) {
                    exceptionReset.run();
                    FlushResult batchFlushResult = tryFlushSegment(batchAggregator, exceptionProvider);
                    anythingFlushed = anythingFlushed | (batchFlushResult == null || batchFlushResult.getFlushedBytes() > 0);
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                exceptionReset.run();
                FlushResult parentFlushResult = tryFlushSegment(context.segmentAggregator, exceptionProvider);
                anythingFlushed = anythingFlushed | (parentFlushResult == null || (parentFlushResult.getFlushedBytes() + parentFlushResult.getMergedBytes()) > 0);
            }
        }
    }

    private <T extends Throwable> FlushResult tryFlushSegment(SegmentAggregator aggregator, Supplier<T> exceptionProvider) {
        try {
            FlushResult flushResult = aggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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

    private static class TestContext implements AutoCloseable {
        final CloseableExecutorService executor;
        final UpdateableContainerMetadata containerMetadata;
        final TestWriterDataSource dataSource;
        final TestStorage storage;
        final Cache cache;
        final AutoStopwatch stopwatch;
        final SegmentAggregator segmentAggregator;
        final SegmentAggregator[] batchesAggregators;

        TestContext(WriterConfig config) {
            this(config, System::currentTimeMillis);
        }

        TestContext(WriterConfig config, Supplier<Long> stopwatchGetMillis) {
            this.executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.containerMetadata = new StreamSegmentContainerMetadata(CONTAINER_ID);

            this.storage = new TestStorage(new InMemoryStorage(this.executor.get()));
            this.cache = new InMemoryCache(Integer.toString(CONTAINER_ID));
            this.stopwatch = new AutoStopwatch(stopwatchGetMillis);

            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = TestWriterDataSource.DataSourceConfig.NO_METADATA_CHECKPOINT;
            this.dataSource = new TestWriterDataSource(this.containerMetadata, this.cache, this.executor.get(), dataSourceConfig);

            this.batchesAggregators = new SegmentAggregator[BATCH_COUNT];
            UpdateableSegmentMetadata segmentMetadata = initialize(this.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID));
            this.segmentAggregator = new SegmentAggregator(segmentMetadata, this.dataSource, this.storage, config, this.stopwatch);
            for (int i = 0; i < BATCH_COUNT; i++) {
                String name = BATCH_NAME_PREFIX + i;
                UpdateableSegmentMetadata batchesMetadata = initialize(this.containerMetadata.mapStreamSegmentId(name, BATCH_ID_START + i, SEGMENT_ID));
                this.batchesAggregators[i] = new SegmentAggregator(batchesMetadata, this.dataSource, this.storage, config, this.stopwatch);
            }
        }

        @Override
        public void close() {
            for (SegmentAggregator aggregator : this.batchesAggregators) {
                aggregator.close();
            }

            this.segmentAggregator.close();
            this.dataSource.close();
            this.cache.close();
            this.storage.close();
            this.executor.close();
        }

        private UpdateableSegmentMetadata initialize(UpdateableSegmentMetadata segmentMetadata) {
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setDurableLogLength(0);
            return segmentMetadata;
        }
    }

    // endregion
}
