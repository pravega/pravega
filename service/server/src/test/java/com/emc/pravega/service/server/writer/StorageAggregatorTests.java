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
import com.emc.pravega.service.server.TestStorage;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
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
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Unit tests for the StorageAggregator class.
 */
public class StorageAggregatorTests {
    private static final int CONTAINER_ID = 0;
    private static final long SEGMENT_ID = 123;
    private static final String SEGMENT_NAME = "Segment";
    private static final long BATCH_ID = 1234;
    private static final String BATCH_NAME = "Batch";
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
        AssertExtensions.assertThrows(
                "initialize() succeeded on a Segment that does not exist in Storage.",
                () -> context.segmentAggregator.initialize(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // Check behavior for already-sealed segments (in storage, but not in metadata)
        context.storage.create(context.batchMetadata.getName(), TIMEOUT).join();
        context.storage.seal(context.batchMetadata.getName(), TIMEOUT).join();
        AssertExtensions.assertThrows(
                "initialize() succeeded on a Segment is sealed in Storage but not in the metadata.",
                () -> context.batchAggregator.initialize(TIMEOUT),
                ex -> ex instanceof RuntimeStreamingException && ex.getCause() instanceof DataCorruptionException);

        // Check the ability to update Metadata.StorageOffset if it is different.
        final int writeLength = 10;
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
        context.storage.write(context.segmentMetadata.getName(), 0, new ByteArrayInputStream(new byte[writeLength]), writeLength, TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        Assert.assertEquals("SegmentMetadata.StorageLength was not updated after call to initialize().", writeLength, context.segmentMetadata.getStorageLength());
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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
        context.storage.create(context.batchMetadata.getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        context.batchAggregator.initialize(TIMEOUT).join();

        // Verify Appends with correct parameters work as expected.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
            context.batchAggregator.add(generateAppendAndUpdateMetadata(i, BATCH_ID, context));
        }

        // Seal the batch and add a MergeBatchOperation to the parent.
        context.batchAggregator.add(generateSealAndUpdateMetadata(BATCH_ID, context));
        context.segmentAggregator.add(generateMergeBatchAndUpdateMetadata(BATCH_ID, context));

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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
        context.storage.create(context.batchMetadata.getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        context.batchAggregator.initialize(TIMEOUT).join();

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
                () -> context.batchAggregator.add(generateSimpleMergeBatch(BATCH_ID, context)),
                ex -> ex instanceof IllegalArgumentException);

        // 1b. Verify that MergeBatchOperation has the right parent.
        AssertExtensions.assertThrows(
                "add() allowed a MergeBatchOperation on the parent for a Batch that did not have it as a parent.",
                () -> context.batchAggregator.add(generateSimpleMergeBatch(badBatchId, context)),
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
        StorageOperation batchAppend1 = generateAppendAndUpdateMetadata(0, BATCH_ID, context);
        context.batchAggregator.add(batchAppend1);
        context.batchAggregator.add(generateSealAndUpdateMetadata(BATCH_ID, context));
        AssertExtensions.assertThrows(
                "add() allowed operation after seal.",
                () -> context.batchAggregator.add(generateSimpleAppend(BATCH_ID, context)),
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
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentMetadata.getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        context.segmentMetadata.setDurableLogLength(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset+length).",
                () -> {
                    // We have the correct offset, but we the append exceeds the DurableLogLength by 1 byte.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentMetadata.getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        // 3c. Verify contiguity (offsets - we cannot have gaps in the data).
        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too small).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentMetadata.getId(), "foo".getBytes(), APPEND_CONTEXT);
                    badOffsetAppend.setStreamSegmentOffset(0);
                    context.segmentAggregator.add(badOffsetAppend);
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentMetadata.getId(), "foo".getBytes(), APPEND_CONTEXT);
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

                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentMetadata.getId(), "foo".getBytes(), APPEND_CONTEXT);
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
                    MergeBatchOperation badIdMerge = new MergeBatchOperation(Integer.MAX_VALUE, context.batchMetadata.getId());
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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
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

            // Call flush() and inspect the result.
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
            if (outstandingSize >= config.getFlushThresholdBytes()) {
                // We are expecting a flush.
                AssertExtensions.assertGreaterThanOrEqual("Not enough bytes were flushed (size threshold).", config.getFlushThresholdBytes(), flushResult.getFlushedBytes());
                outstandingSize -= flushResult.getFlushedBytes();
            } else {
                // Not expecting a flush.
                Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%d, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                        0, flushResult.getFlushedBytes());
            }

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
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
            outstandingSize -= flushResult.getFlushedBytes();

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
            }

            // Call flush() and inspect the result.
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (batch appends).", 0, flushResult.getFlushedBytes());
            outstandingSize -= flushResult.getFlushedBytes();

            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Verify data.
        Assert.assertEquals("Not expecting leftover data not flushed.", 0, outstandingSize);
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentMetadata.getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentMetadata.getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
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
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentMetadata.getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(context.segmentMetadata.getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
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

            // Call flush() and inspect the result.
            FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // Not expecting a flush. If we do get one, we need to modify the test parameters.
            Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%d, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                    0, flushResult.getFlushedBytes());

            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Call flush and verify that the entire Aggregator got flushed and the Seal got persisted to Storage.
        FlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize, flushResult.getFlushedBytes());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentMetadata.getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Segment is not sealed in storage post flush.", storageInfo.isSealed());
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentMetadata.isSealedInStorage());
        context.storage.read(context.segmentMetadata.getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();

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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Seal the segment in Storage, behind the scenes.
        context.storage.seal(context.segmentMetadata.getName(), TIMEOUT).join();

        // Call flush and verify no exception is thrown.
        context.segmentAggregator.flush(TIMEOUT).join();

        // Verify data - even though already sealed, make sure the metadata is updated accordingly.
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentMetadata.isSealedInStorage());
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
        context.storage.create(context.segmentMetadata.getName(), TIMEOUT).join();
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
            System.out.println("Y");
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
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentMetadata.getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Segment is not sealed in storage post flush.", storageInfo.isSealed());
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentMetadata.isSealedInStorage());
        context.storage.read(context.segmentMetadata.getName(), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method with Append and MergeBatchOperations.
     */
    @Test
    public void testFlushMerge() {
        // Merge individually/
        // Merge + Seal
        // Merge + Append
        // Merge + Seal
        // Appends + Merge
        // Conditions when merge should not happen
        // Storage Errors
    }

    //endregion

    //region Helpers

    private void getAppendData(StorageOperation operation, ByteArrayOutputStream stream, TestContext context) {
        try {
            if (operation instanceof StreamSegmentAppendOperation) {
                stream.write(((StreamSegmentAppendOperation) operation).getData());
            } else if (operation instanceof CachedStreamSegmentAppendOperation) {
                stream.write(context.cache.get(((CachedStreamSegmentAppendOperation) operation).getCacheKey()));
            } else {
                Assert.fail("Not an append operation: " + operation);
            }
        } catch (IOException ex) {
            Assert.fail("Not expecting this exception: " + ex);
        }
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

    //endregion

    // region TestContext

    private static class TestContext implements AutoCloseable {
        final CloseableExecutorService executor;
        final UpdateableContainerMetadata containerMetadata;
        final UpdateableSegmentMetadata segmentMetadata;
        final UpdateableSegmentMetadata batchMetadata;
        final TestWriterDataSource dataSource;
        final TestStorage storage;
        final Cache cache;
        final AutoStopwatch stopwatch;
        final SegmentAggregator segmentAggregator;
        final SegmentAggregator batchAggregator;

        TestContext(WriterConfig config) {
            this(config, System::currentTimeMillis);
        }

        TestContext(WriterConfig config, Supplier<Long> stopwatchGetMillis) {
            this.executor = new CloseableExecutorService(Executors.newScheduledThreadPool(THREAD_POOL_SIZE));
            this.containerMetadata = new StreamSegmentContainerMetadata(CONTAINER_ID);
            this.segmentMetadata = this.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setDurableLogLength(0);

            this.batchMetadata = this.containerMetadata.mapStreamSegmentId(BATCH_NAME, BATCH_ID, SEGMENT_ID);
            batchMetadata.setStorageLength(0);
            batchMetadata.setDurableLogLength(0);

            this.storage = new TestStorage(new InMemoryStorage(this.executor.get()));
            this.cache = new InMemoryCache(Integer.toString(CONTAINER_ID));
            this.stopwatch = new AutoStopwatch(stopwatchGetMillis);

            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = TestWriterDataSource.DataSourceConfig.NO_METADATA_CHECKPOINT;
            this.dataSource = new TestWriterDataSource(this.containerMetadata, this.cache, this.executor.get(), dataSourceConfig);
            this.segmentAggregator = new SegmentAggregator(this.segmentMetadata, this.dataSource, this.storage, config, this.stopwatch);
            this.batchAggregator = new SegmentAggregator(this.batchMetadata, this.dataSource, this.storage, config, this.stopwatch);
        }

        @Override
        public void close() {
            this.batchAggregator.close();
            this.segmentAggregator.close();
            this.dataSource.close();
            this.cache.close();
            this.storage.close();
            this.executor.close();
        }
    }

    // endregion
}
