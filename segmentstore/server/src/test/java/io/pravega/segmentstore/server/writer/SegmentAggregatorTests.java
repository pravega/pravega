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
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.TestStorage;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.mocks.InMemoryStorage;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

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
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private static final WriterConfig DEFAULT_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_THRESHOLD_BYTES, 100)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
            .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 150)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .build();
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    //region initialize()

    /**
     * Tests the initialize() method.
     */
    @Test
    public void testInitialize() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Check behavior for non-existent segments (in Storage) that are actually supposed to be empty.
        context.transactionAggregators[0].initialize(TIMEOUT).join();
        Assert.assertFalse("isDeleted() flag not set on metadata for deleted segment.", context.transactionAggregators[0].getMetadata().isDeleted());

        // Check behavior for non-existent segments (in Storage) that are not supposed to be empty.
        val sm3 = (UpdateableSegmentMetadata) context.transactionAggregators[3].getMetadata();
        sm3.setLength(1L);
        sm3.setStorageLength(1L);
        context.transactionAggregators[3].initialize(TIMEOUT).join();
        Assert.assertTrue("isDeleted() flag not set on metadata for deleted segment.", sm3.isDeleted());
        Assert.assertTrue("isDeletedInStorage() flag not set on metadata for deleted segment.", sm3.isDeletedInStorage());

        // Check behavior for already-deleted segments (in Metadata) that are also deleted from Storage.
        val sm4  = (UpdateableSegmentMetadata) context.transactionAggregators[4].getMetadata();
        sm4.markDeleted(); // We do not mark it as deleted in Storage.
        val ag4 = context.transactionAggregators[4];
        ag4.initialize(TIMEOUT).join();
        Assert.assertTrue("Expected SegmentMetadata.isDeletedInStorage to be set to true post init.", sm4.isDeletedInStorage());
        sm4.markSealed();
        ag4.add(generateSealAndUpdateMetadata(sm4.getId(), context));
        Assert.assertFalse("SegmentAggregator should have ignored new operation for deleted segment.", ag4.mustFlush());

        // Check behavior for already-deleted segments (in Metadata) that are still present in Storage.
        val sm5  = (UpdateableSegmentMetadata) context.transactionAggregators[5].getMetadata();
        context.storage.create(sm5.getName(), TIMEOUT).join();
        sm5.markDeleted(); // We do not mark it as deleted in Storage.
        val ag5 = context.transactionAggregators[5];
        ag5.initialize(TIMEOUT).join();
        Assert.assertTrue("Expected SegmentMetadata.isDeletedInStorage to be set to true post init.", sm5.isDeletedInStorage());
        Assert.assertFalse("Expected Segment to have been deleted from Storage.", context.storage.exists(sm5.getName(), TIMEOUT).join());
        sm5.markSealed();
        ag5.add(generateSealAndUpdateMetadata(sm5.getId(), context));
        Assert.assertFalse("SegmentAggregator should have ignored new operation for deleted segment.", ag5.mustFlush());

        // Check behavior for already-sealed segments (in storage, but not in metadata)
        context.storage.create(context.transactionAggregators[1].getMetadata().getName(), TIMEOUT).join();
        context.storage.seal(writeHandle(context.transactionAggregators[1].getMetadata().getName()), TIMEOUT).join();
        AssertExtensions.assertSuppliedFutureThrows(
                "initialize() succeeded on a Segment is sealed in Storage but not in the metadata.",
                () -> context.transactionAggregators[1].initialize(TIMEOUT),
                ex -> ex instanceof DataCorruptionException);

        // Check behavior for already-sealed segments (in storage, in metadata, but metadata does not reflect Sealed in storage.)
        context.storage.create(context.transactionAggregators[2].getMetadata().getName(), TIMEOUT).join();
        context.storage.seal(writeHandle(context.transactionAggregators[2].getMetadata().getName()), TIMEOUT).join();
        ((UpdateableSegmentMetadata) context.transactionAggregators[2].getMetadata()).markSealed();
        context.transactionAggregators[2].initialize(TIMEOUT).join();
        Assert.assertTrue("isSealedInStorage() flag not set on metadata for storage-sealed segment.", context.transactionAggregators[2].getMetadata().isSealedInStorage());

        // Check the ability to update Metadata.StorageOffset if it is different.
        final int writeLength = 10;
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.storage.write(writeHandle(context.segmentAggregator.getMetadata().getName()), 0, new ByteArrayInputStream(new byte[writeLength]), writeLength, TIMEOUT).join();
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
                () -> context.segmentAggregator.add(new StreamSegmentAppendOperation(0, BufferView.empty(), null)),
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
        final int appendCount = 20;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We want to make sure we do not prematurely acknowledge anything.
        context.dataSource.setCompleteMergeCallback((target, source) -> Assert.fail("Not expecting any merger callbacks yet."));

        // We need one Transaction for this test (to which we populate data).
        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
        SegmentMetadata transactionMetadata = transactionAggregator.getMetadata();

        // We also need an empty transaction.
        SegmentAggregator emptyTransactionAggregator = context.transactionAggregators[1];
        SegmentMetadata emptyTransactionMetadata = emptyTransactionAggregator.getMetadata();

        context.segmentAggregator.initialize(TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();
        emptyTransactionAggregator.initialize(TIMEOUT).join();

        // Seal the Empty Transaction and add a MergeTransactionOperation to the parent (do this before everything else.
        emptyTransactionAggregator.add(generateSealAndUpdateMetadata(emptyTransactionMetadata.getId(), context));
        context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(emptyTransactionMetadata.getId(), context));

        // Verify Appends with correct parameters work as expected.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
            transactionAggregator.add(generateAppendAndUpdateMetadata(i, transactionMetadata.getId(), context));
        }

        // Seal the Transaction and add a MergeSegmentOperation to the parent.
        transactionAggregator.add(generateSealAndUpdateMetadata(transactionMetadata.getId(), context));
        context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(transactionMetadata.getId(), context));

        // Add more appends to the parent, and truncate the Segment bit by bit.
        for (int i = 0; i < appendCount; i++) {
            context.segmentAggregator.add(generateAppendAndUpdateMetadata(i, SEGMENT_ID, context));
            if (i % 2 == 1) {
                // Every other Append, do a Truncate. This helps us check both Append Aggregation and Segment Truncation.
                context.segmentAggregator.add(generateTruncateAndUpdateMetadata(SEGMENT_ID, context));
            }
        }

        // Seal the parent, then truncate again.
        context.segmentAggregator.add(generateSealAndUpdateMetadata(SEGMENT_ID, context));
        context.segmentAggregator.add(generateTruncateAndUpdateMetadata(SEGMENT_ID, context));

        // This should have no effect and not throw any errors.
        context.segmentAggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(new AttributeUpdate(AttributeId.randomUUID(), AttributeUpdateType.Replace, 1))));
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

        context.segmentAggregator.initialize(TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();

        // Create 2 more segments that can be used to verify MergeSegmentOperation.
        context.containerMetadata.mapStreamSegmentId(badParentName, badParentId);
        UpdateableSegmentMetadata badTransactionMetadata = context.containerMetadata.mapStreamSegmentId(badTransactionName, badTransactionId);
        badTransactionMetadata.setLength(0);
        badTransactionMetadata.setStorageLength(0);

        // 1. MergeSegmentOperation
        // Verify that MergeSegmentOperation cannot be added to the Segment to be merged.
        AssertExtensions.assertThrows(
                "add() allowed a MergeSegmentOperation on the Transaction segment.",
                () -> transactionAggregator.add(generateSimpleMergeTransaction(transactionMetadata.getId(), context)),
                ex -> ex instanceof IllegalArgumentException);

        // 2. StreamSegmentSealOperation.
        // 2a. Verify we cannot add a StreamSegmentSealOperation if the segment is not sealed yet.
        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentSealOperation for a non-sealed segment.",
                () -> {
                    @Cleanup
                    SegmentAggregator badTransactionAggregator = new SegmentAggregator(badTransactionMetadata, context.dataSource,
                            context.storage, DEFAULT_CONFIG, context.timer, executorService());
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
                    // We have the correct offset, but we did not increase the Length.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(
                            parentAppend1.getStreamSegmentId(),
                            parentAppend1.getStreamSegmentOffset(),
                            new ByteArraySegment(new byte[(int) parentAppend1.getLength()]),
                            null);
                    context.segmentAggregator.add(badAppend);
                },
                ex -> ex instanceof IllegalArgumentException);

        // Add this one append to the parent (nothing unusual here); we'll use this for the next tests.
        context.segmentAggregator.add(parentAppend1);

        // 3b. Verify we cannot add anything beyond the DurableLogOffset (offset or offset+length).
        val appendData = new ByteArraySegment("foo".getBytes());
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset).",
                () -> {
                    // We have the correct offset, but we did not increase the Length.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), appendData, null);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        ((UpdateableSegmentMetadata) context.segmentAggregator.getMetadata()).setLength(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
        AssertExtensions.assertThrows(
                "add() allowed an operation beyond the DurableLogOffset (offset+length).",
                () -> {
                    // We have the correct offset, but we the append exceeds the Length by 1 byte.
                    StreamSegmentAppendOperation badAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), appendData, null);
                    badAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        // 3c. Verify contiguity (offsets - we cannot have gaps in the data).
        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too small).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), appendData, null);
                    badOffsetAppend.setStreamSegmentOffset(0);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large).",
                () -> {
                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), appendData, null);
                    badOffsetAppend.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength() + 1);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        AssertExtensions.assertThrows(
                "add() allowed an operation with wrong offset (too large, but no pending operations).",
                () -> {
                    @Cleanup
                    SegmentAggregator badTransactionAggregator = new SegmentAggregator(badTransactionMetadata, context.dataSource,
                            context.storage, DEFAULT_CONFIG, context.timer, executorService());
                    badTransactionMetadata.setLength(100);
                    badTransactionAggregator.initialize(TIMEOUT).join();

                    StreamSegmentAppendOperation badOffsetAppend = new StreamSegmentAppendOperation(context.segmentAggregator.getMetadata().getId(), appendData, null);
                    badOffsetAppend.setStreamSegmentOffset(1);
                    context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(badOffsetAppend));
                },
                ex -> ex instanceof DataCorruptionException);

        // 4. Verify Segment Id match.
        AssertExtensions.assertThrows(
                "add() allowed an Append operation with wrong Segment Id.",
                () -> {
                    StreamSegmentAppendOperation badIdAppend = new StreamSegmentAppendOperation(Integer.MAX_VALUE, appendData, null);
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
                "add() allowed a MergeSegmentOperation with wrong SegmentId.",
                () -> {
                    MergeSegmentOperation badIdMerge = new MergeSegmentOperation(Integer.MAX_VALUE, transactionMetadata.getId());
                    badIdMerge.setStreamSegmentOffset(parentAppend1.getStreamSegmentOffset() + parentAppend1.getLength());
                    badIdMerge.setLength(1);
                    context.segmentAggregator.add(badIdMerge);
                },
                ex -> ex instanceof IllegalArgumentException);

        // 5. Truncations.
        AssertExtensions.assertThrows(
                "add() allowed a StreamSegmentTruncateOperation with a truncation offset beyond the one in the metadata.",
                () -> {
                    StreamSegmentTruncateOperation op = new StreamSegmentTruncateOperation(SEGMENT_ID, 10);
                    op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
                    context.segmentAggregator.add(op);
                },
                ex -> ex instanceof DataCorruptionException);
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

        @Cleanup
        TestContext context = new TestContext(config);
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
            Assert.assertEquals(
                "Unexpected value returned by mustFlush() (size threshold).", expectFlush,
                context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (size threshold).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());

            // Call flush() and inspect the result.
            WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
            if (expectFlush) {
                AssertExtensions.assertGreaterThanOrEqual("Not enough bytes were flushed (size threshold).", config.getFlushThresholdBytes(), flushResult.getFlushedBytes());
                outstandingSize.addAndGet(-flushResult.getFlushedBytes());
                Assert.assertEquals(
                    "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (size threshold).",
                    sequenceNumbers.getLowestUncommitted(),
                    context.segmentAggregator.getLowestUncommittedSequenceNumber());
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
            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
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
                Assert.assertEquals(
                    "Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (Transaction appends).",
                    sequenceNumbers.getLowestUncommitted(),
                    context.segmentAggregator.getLowestUncommittedSequenceNumber());
            }

            // Call flush() and inspect the result.
            Assert.assertTrue("Unexpected value returned by mustFlush() (Transaction appends).", context.segmentAggregator.mustFlush());
            WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (Transaction appends).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (Transaction appends).", context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (Transaction appends).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        // Part 4: large appends (larger than MaxFlushSize).
        Random random = RandomFactory.create();
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
            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (large appends).", context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (large appends).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            AssertExtensions.assertGreaterThan("Not enough bytes were flushed (large appends).", 0, flushResult.getFlushedBytes());
            outstandingSize.addAndGet(-flushResult.getFlushedBytes());

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.segmentAggregator.mustFlush());
            Assert.assertEquals(
                "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (large appends).",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test (large appends).", 0, flushResult.getMergedBytes());
        }

        // Verify data.
        Assert.assertEquals("Not expecting leftover data not flushed.", 0, outstandingSize.get());
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();

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

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Have the writes fail every few attempts with a well known exception.
        AtomicReference<IntentionalException> setException = new AtomicReference<>();
        Supplier<Exception> exceptionSupplier = () -> {
            IntentionalException ex = new IntentionalException(Long.toString(context.timer.getElapsedMillis()));
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
            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            WriterFlushResult flushResult = null;

            try {
                flushResult = context.segmentAggregator.flush(TIMEOUT).join();
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), Exceptions.unwrap(ex));
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
        context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        context.storage.setWriteSyncErrorInjector(null);
        context.storage.setWriteAsyncErrorInjector(null);
        context.segmentAggregator.flush(TIMEOUT).join();

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
        AssertExtensions.assertGreaterThan("Not enough errors injected.", 0, exceptionCount);
    }

    /**
     * Tests the behavior of flush() empty appends.
     */
    @Test
    public void testFlushEmptyAppend() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        val rnd = new Random(0);
        final byte[] initialBytes = new byte[config.getMaxFlushSizeBytes()];
        final byte[] mergedBytes = new byte[100];
        final int segmentLength = initialBytes.length + mergedBytes.length;
        rnd.nextBytes(initialBytes);
        rnd.nextBytes(mergedBytes);

        @Cleanup
        TestContext context = new TestContext(config);

        // Create a segment in Storage.
        context.storage.create(SEGMENT_NAME, TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        val metadata = (UpdateableSegmentMetadata) context.segmentAggregator.getMetadata();
        metadata.setLength(segmentLength);

        // First append fills up the max limit for the AggregatedAppend buffer.
        val append1 = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(initialBytes), null);
        append1.setStreamSegmentOffset(0);
        append1.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        context.dataSource.recordAppend(append1);
        context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(append1));

        // Second append is empty.
        val emptyAppend = new StreamSegmentAppendOperation(SEGMENT_ID, BufferView.empty(), null);
        emptyAppend.setStreamSegmentOffset(initialBytes.length);
        emptyAppend.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        context.dataSource.recordAppend(emptyAppend);
        context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(emptyAppend));

        // Create a source segment.
        val sourceAggregator = context.transactionAggregators[0];
        val sourceMetadata = (UpdateableSegmentMetadata) sourceAggregator.getMetadata();
        sourceMetadata.setLength(mergedBytes.length);
        sourceMetadata.setStorageLength(mergedBytes.length);
        context.storage.create(sourceMetadata.getName(), TIMEOUT).join();
        context.storage.openWrite(sourceMetadata.getName())
                .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(mergedBytes), mergedBytes.length, TIMEOUT)
                        .thenCompose(v -> context.storage.seal(handle, TIMEOUT)))
                .join();

        // And include it via a Merge Op.
        sourceMetadata.markSealed();
        sourceMetadata.markSealedInStorage();
        sourceMetadata.markMerged();
        val mergeOp = new MergeSegmentOperation(SEGMENT_ID, sourceMetadata.getId());
        mergeOp.setStreamSegmentOffset(initialBytes.length);
        mergeOp.setLength(sourceMetadata.getLength());
        mergeOp.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        context.segmentAggregator.add(mergeOp);

        // Flush, and verify the result.
        val flushResult1 = context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of bytes flushed", initialBytes.length, flushResult1.getFlushedBytes());
        Assert.assertEquals("Unexpected number of bytes merged", mergedBytes.length, flushResult1.getMergedBytes());
        byte[] expectedData = new byte[initialBytes.length + mergedBytes.length];
        System.arraycopy(initialBytes, 0, expectedData, 0, initialBytes.length);
        System.arraycopy(mergedBytes, 0, expectedData, initialBytes.length, mergedBytes.length);
        verifySegmentData(expectedData, context);
    }

    /**
     * Tests the flush() method with the force flag set.
     * Verifies both length-based and time-based flush triggers, as well as flushing rather large operations.
     */
    @Test
    public void testFlushForced() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
        AtomicLong outstandingSize = new AtomicLong(); // Number of bytes remaining to be flushed.
        SequenceNumberCalculator sequenceNumbers = new SequenceNumberCalculator(context, outstandingSize);

        // A single operation should be allow us to test this feature.
        StorageOperation appendOp = generateAppendAndUpdateMetadata(0, SEGMENT_ID, context);
        outstandingSize.addAndGet(appendOp.getLength());
        context.segmentAggregator.add(appendOp);
        getAppendData(appendOp, writtenData, context);
        sequenceNumbers.record(appendOp);

        Assert.assertFalse("Unexpected value returned by mustFlush() (size threshold).", context.segmentAggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush.",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Call flush() and inspect the result.
        val flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertFalse("Not expecting a flush.", flushResult.isAnythingFlushed());
        Assert.assertFalse("Unexpected value returned by mustFlush() after no-op flush.", context.segmentAggregator.mustFlush());

        val forceFlushResult = context.segmentAggregator.flush(true, TIMEOUT).join();
        Assert.assertEquals("Unexpected flush result.", appendOp.getLength(), forceFlushResult.getFlushedBytes());
        Assert.assertFalse("Unexpected value returned by mustFlush() after force flush.", context.segmentAggregator.mustFlush());
        outstandingSize.set(0);
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after force flush",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method with Append and StreamSegmentSealOperations.
     */
    @Test
    public void testSeal() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        final int appendCount = 1000;
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);
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
            WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
            Assert.assertEquals(String.format("Not expecting a flush. OutstandingSize=%s, Threshold=%d", outstandingSize, config.getFlushThresholdBytes()),
                    0, flushResult.getFlushedBytes());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
        }

        Assert.assertFalse("Unexpected value returned by mustFlush() before adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);
        Assert.assertEquals(
            "Unexpected value returned by getLowestUncommittedSequenceNumber() after adding StreamSegmentSealOperation.",
            sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Unexpected value returned by mustFlush() after adding StreamSegmentSealOperation.", context.segmentAggregator.mustFlush());

        // Call flush and verify that the entire Aggregator got flushed and the Seal got persisted to Storage.
        WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize.get(), flushResult.getFlushedBytes());
        Assert.assertFalse("Unexpected value returned by mustFlush() after flushing.", context.segmentAggregator.mustFlush());
        Assert.assertEquals(
            "Unexpected value returned by getLowestUncommittedSequenceNumber() after flushing.",
            Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Segment is not sealed in storage post flush.", storageInfo.isSealed());
        Assert.assertTrue("Segment is not marked in metadata as sealed in storage post flush.", context.segmentAggregator.getMetadata().isSealedInStorage());
        context.storage.read(InMemoryStorage.newHandle(context.segmentAggregator.getMetadata().getName(), false), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method when it has a StreamSegmentSealOperation but the Segment is already sealed in Storage.
     */
    @Test
    public void testSealAlreadySealed() throws Exception {
        // Add some appends and seal, and then flush together. Verify that everything got flushed in one go.
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Seal the segment in Storage, behind the scenes.
        context.storage.seal(InMemoryStorage.newHandle(context.segmentAggregator.getMetadata().getName(), false), TIMEOUT).join();

        // Call flush and verify no exception is thrown.
        context.segmentAggregator.flush(TIMEOUT).join();

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
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);
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
            IntentionalException ex = new IntentionalException(Long.toString(context.timer.getElapsedMillis()));
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
                WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), Exceptions.unwrap(ex));
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
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
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
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);

        // Initialize all segments.
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            a.initialize(TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();
        val actualMergeOpAck = new ArrayList<Map.Entry<Long, Long>>();
        context.dataSource.setCompleteMergeCallback((target, source) -> actualMergeOpAck.add(new AbstractMap.SimpleImmutableEntry<Long, Long>(target, source)));

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
                boolean exists = context.storage.exists(transactionMetadata.getName(), TIMEOUT).join();
                if (exists) {
                    // We're not expecting this to exist, but if it does, do check it.
                    SegmentProperties sp = context.storage.getStreamSegmentInfo(transactionMetadata.getName(), TIMEOUT).join();
                    Assert.assertFalse("Transaction not to be merged is sealed in storage.", sp.isSealed());
                }
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
        verifySegmentData(parentData.toByteArray(), context);

        // Verify calls to completeMerge.
        val expectedMergeOpSources = Arrays.stream(context.transactionAggregators).map(a -> a.getMetadata().getId()).collect(Collectors.toSet());
        Assert.assertEquals("Unexpected number of calls to completeMerge.", expectedMergeOpSources.size(), actualMergeOpAck.size());
        val actualMergeOpSources = actualMergeOpAck.stream().map(Map.Entry::getValue).collect(Collectors.toSet());
        AssertExtensions.assertContainsSameElements("Unexpected sources for invocation to completeMerge.", expectedMergeOpSources, actualMergeOpSources);
        for (Map.Entry<Long, Long> e : actualMergeOpAck) {
            Assert.assertEquals("Unexpected target for invocation to completeMerge.", context.segmentAggregator.getMetadata().getId(), (long) e.getKey());
        }
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
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);

        // Initialize all segments.
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
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
            IntentionalException ex = new IntentionalException(Long.toString(context.timer.getElapsedMillis()));
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
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method with StreamSegmentTruncateOperations.
     */
    @Test
    public void testTruncate() throws Exception {
        // Add some appends and a truncate, and then flush together. Verify that everything got flushed in one go.
        final int appendCount = 1000;
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);
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
        }

        Assert.assertFalse("Unexpected value returned by mustFlush() before adding StreamSegmentTruncateOperation.",
                context.segmentAggregator.mustFlush());

        // Generate and add a Truncate Operation.
        StorageOperation truncateOp = generateTruncateAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(truncateOp);
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after adding StreamSegmentTruncateOperation.",
                sequenceNumbers.getLowestUncommitted(), context.segmentAggregator.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Unexpected value returned by mustFlush() after adding StreamSegmentTruncateOperation.",
                context.segmentAggregator.mustFlush());

        // Call flush and verify that the entire Aggregator got flushed and the Truncate got persisted to Storage.
        WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize.get(), flushResult.getFlushedBytes());
        Assert.assertFalse("Unexpected value returned by mustFlush() after flushing.", context.segmentAggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flushing.",
                Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertEquals("Unexpected truncation offset in Storage.", truncateOp.getStreamSegmentOffset(),
                context.storage.getTruncationOffset(context.segmentAggregator.getMetadata().getName()));
        context.storage.read(InMemoryStorage.newHandle(context.segmentAggregator.getMetadata().getName(), false), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the flush() method with StreamSegmentTruncateOperations after the segment has been Sealed.
     */
    @Test
    public void testTruncateAndSeal() throws Exception {
        // Add some data and intersperse with truncates.
        final int appendCount = 1000;
        final int truncateEvery = 20;
        final WriterConfig config = WriterConfig
                .builder()
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, appendCount * 50) // Extra high length threshold.
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, 10000)
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
                .build();

        @Cleanup
        TestContext context = new TestContext(config);
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

            if (i % truncateEvery == 1) {
                StorageOperation truncateOp = generateTruncateAndUpdateMetadata(SEGMENT_ID, context);
                context.segmentAggregator.add(truncateOp);
                sequenceNumbers.record(truncateOp);
            }
        }

        // Generate and add a Seal Operation.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // Add another truncate op, after the Seal.
        StorageOperation lastTruncateOp = generateTruncateAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(lastTruncateOp);

        WriterFlushResult flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Expected the entire Aggregator to be flushed.", outstandingSize.get(), flushResult.getFlushedBytes());
        Assert.assertFalse("Unexpected value returned by mustFlush() after flushing.", context.segmentAggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flushing.",
                Operation.NO_SEQUENCE_NUMBER, context.segmentAggregator.getLowestUncommittedSequenceNumber());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageInfo.getLength());
        Assert.assertTrue("Unexpected sealed status in Storage.", storageInfo.isSealed());
        Assert.assertEquals("Unexpected truncation offset in Storage.", lastTruncateOp.getStreamSegmentOffset(),
                context.storage.getTruncationOffset(context.segmentAggregator.getMetadata().getName()));
        context.storage.read(InMemoryStorage.newHandle(context.segmentAggregator.getMetadata().getName(), false), 0, actualData, 0, actualData.length, TIMEOUT).join();

        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the SegmentAggregator's behavior when an already Sealed Segment is opened and truncated.
     */
    @Test
    public void testTruncateAlreadySealedSegment() throws Exception {
        // Pre-create the segment, write some data, and then seal it.
        val rnd = new Random(0);
        byte[] storageData = new byte[100];
        rnd.nextBytes(storageData);

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a segment, add some data, and seal it in storage.
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.storage.openWrite(context.segmentAggregator.getMetadata().getName())
                       .thenCompose(h -> context.storage.write(h, 0, new ByteArrayInputStream(storageData), storageData.length, TIMEOUT)
                                                        .thenCompose(v -> context.storage.seal(h, TIMEOUT))).join();
        val sm = context.containerMetadata.getStreamSegmentMetadata(context.segmentAggregator.getMetadata().getId());
        sm.setLength(storageData.length);
        sm.setStorageLength(storageData.length);
        sm.markSealed();
        sm.markSealedInStorage();

        // Initialize the SegmentAggregator.
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Generate and add a Seal Operation.
        StorageOperation truncateOp = generateTruncateAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(truncateOp);
        Assert.assertTrue("Unexpected value returned by mustFlush() after adding StreamSegmentTruncateOperation.",
                context.segmentAggregator.mustFlush());

        // Call flush and verify that the entire Aggregator got flushed and the Truncate got persisted to Storage.
        context.segmentAggregator.flush(TIMEOUT).join();

        // Verify data.
        SegmentProperties storageInfo = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes in Storage.", storageData.length, storageInfo.getLength());
        Assert.assertEquals("Unexpected truncation offset in Storage.", truncateOp.getStreamSegmentOffset(),
                context.storage.getTruncationOffset(context.segmentAggregator.getMetadata().getName()));
    }

    /**
     * Tests the ability to process a {@link DeleteSegmentOperation} on Segments in various states:
     * - Empty (not yet created).
     * - Empty (created, but no data).
     * - Not empty, not sealed.
     * - Sealed (empty or not).
     */
    @Test
    public void testDelete() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        val notCreated = context.transactionAggregators[0];
        val empty = context.transactionAggregators[1];
        val notSealed = context.transactionAggregators[2];
        val sealed = context.transactionAggregators[3];
        val withMergers = context.transactionAggregators[4];
        val withMergerSource = context.transactionAggregators[5];
        val emptyWithAttributes = context.transactionAggregators[6];
        val allAggregators = new SegmentAggregator[]{notCreated, empty, notSealed, sealed, withMergers, emptyWithAttributes};

        // Create the segments that are supposed to exist in Storage.
        Stream.of(empty, notSealed, sealed)
              .forEach(a -> context.storage.create(a.getMetadata().getName(), TIMEOUT).join());

        // Write 1 byte to the non-empty segment and add 1 attribute.
        context.storage.openWrite(notSealed.getMetadata().getName())
                       .thenCompose(handle -> context.storage.write(handle, 0, new ByteArrayInputStream(new byte[]{1}), 1, TIMEOUT))
                       .join();
        ((UpdateableSegmentMetadata) notSealed.getMetadata()).setLength(1L);
        context.dataSource.persistAttributes(notSealed.getMetadata().getId(), Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT).join();

        // Seal the sealed segment.
        ((UpdateableSegmentMetadata) sealed.getMetadata()).markSealed();
        context.storage.openWrite(sealed.getMetadata().getName())
                       .thenCompose(handle -> context.storage.seal(handle, TIMEOUT))
                       .join();
        context.dataSource.persistAttributes(sealed.getMetadata().getId(), Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT).join();

        // Create a source segment; we'll verify this was also deleted when its target was.
        context.storage.create(withMergerSource.getMetadata().getName(), TIMEOUT).join();
        context.dataSource.persistAttributes(withMergerSource.getMetadata().getId(), Collections.singletonMap(AttributeId.randomUUID(), 2L), TIMEOUT).join();

        // This segment has an attribute index, but no segment has been created yet (since no data has been written to it).
        context.dataSource.persistAttributes(emptyWithAttributes.getMetadata().getId(), Collections.singletonMap(AttributeId.randomUUID(), 3L), TIMEOUT).join();

        for (val a : allAggregators) {
            // Initialize the Aggregator and add the DeleteSegmentOperation.
            a.initialize(TIMEOUT).join();
            if (a == withMergers) {
                // Add a merged segment to this one, but not before adding an arbitrary operation.
                withMergers.add(generateAppendAndUpdateMetadata(1, withMergers.getMetadata().getId(), context));
                a.add(generateMergeTransactionAndUpdateMetadata(withMergers.getMetadata().getId(), withMergerSource.getMetadata().getId(), context));
            }

            a.add(generateDeleteAndUpdateMetadata(a.getMetadata().getId(), context));
            AssertExtensions.assertGreaterThan("Unexpected LUSN before flush.", 0, a.getLowestUncommittedSequenceNumber());
            Assert.assertTrue("Unexpected value from mustFlush() when DeletedSegmentOperation queued up.", a.mustFlush());

            // Flush everything.
            a.flush(TIMEOUT).join();
            Assert.assertFalse("Unexpected value from mustFlush() after Deletion.", a.mustFlush());
            AssertExtensions.assertLessThan("Unexpected LUSN after flush.", 0, a.getLowestUncommittedSequenceNumber());
            Assert.assertTrue("Unexpected value from isDeleted() after Deletion.", a.getMetadata().isDeleted());
            Assert.assertTrue("Unexpected value from isDeletedInStorage() after Deletion.", a.getMetadata().isDeletedInStorage());

            // Verify that no segment exists in Storage after the flush.
            boolean existsInStorage = context.storage.exists(a.getMetadata().getName(), TIMEOUT).join();
            Assert.assertFalse("Segment still exists in Storage after Deletion.", existsInStorage);
        }

        Assert.assertFalse("Pending merger source segment not deleted.",
                context.storage.exists(withMergerSource.getMetadata().getName(), TIMEOUT).join());
        Assert.assertTrue("Attributes not deleted for non-merged segment.",
                context.dataSource.getPersistedAttributes(notSealed.getMetadata().getId()).isEmpty());
        Assert.assertTrue("Attributes not deleted for merger source segment.",
                context.dataSource.getPersistedAttributes(withMergerSource.getMetadata().getId()).isEmpty());
        Assert.assertTrue("Attributes not deleted for empty segment with attributes.",
                context.dataSource.getPersistedAttributes(emptyWithAttributes.getMetadata().getId()).isEmpty());
    }

    /**
     * Tests the ability to reconcile a {@link DeleteSegmentOperation} on Segments in various states:
     * - Empty (not yet created).
     * - Empty (created, but no data).
     * - Not empty, not sealed.
     * - Sealed (empty or not).
     *
     * Reconciling a {@link DeleteSegmentOperation} is different from any other operation. Even if there are other
     * operations to reconcile, the simple presence of a Delete will bypass any other one and simply delete the segment.
     */
    @Test
    public void testReconcileDelete() throws Exception {
        final int appendLength = DEFAULT_CONFIG.getFlushThresholdBytes();
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        val notExistsWithAppend = context.transactionAggregators[0];
        val existsWithAppend = context.transactionAggregators[1];
        val existsWithSeal = context.transactionAggregators[2];
        val allAggregators = new SegmentAggregator[]{notExistsWithAppend, existsWithAppend, existsWithSeal};

        for (val a : allAggregators) {
            // Create the segment, and add 1 byte to it. This will cause initialize() to not treat it as empty.
            context.storage.create(a.getMetadata().getName(), TIMEOUT)
                    .thenCompose(v -> context.storage.openWrite(a.getMetadata().getName()))
                    .thenCompose(handle -> {
                        ((UpdateableSegmentMetadata) a.getMetadata()).setLength(1L);
                        ((UpdateableSegmentMetadata) a.getMetadata()).setStorageLength(1L);
                        return context.storage.write(handle, 0, new ByteArrayInputStream(new byte[]{1}), 1, TIMEOUT);
                    })
                    .thenCompose(v -> a.initialize(TIMEOUT))
                    .join();

            // Add enough data to trigger a flush.
            a.add(generateAppendAndUpdateMetadata(a.getMetadata().getId(), new byte[appendLength], context));
            if (a == existsWithSeal) {
                // Add a Seal for that segment that should be sealed.
                a.add(generateSealAndUpdateMetadata(existsWithSeal.getMetadata().getId(), context));
            }

            // Delete the Segment from Storage.
            Futures.exceptionallyExpecting(
                    context.storage.openWrite(a.getMetadata().getName())
                            .thenCompose(handle -> context.storage.delete(handle, TIMEOUT)),
                    ex -> ex instanceof StreamSegmentNotExistsException,
                    null).join();

            Assert.assertTrue("Unexpected value from mustFlush() before first flush().", a.mustFlush());

            // First attempt should fail.
            AssertExtensions.assertSuppliedFutureThrows(
                    "First invocation of flush() should fail.",
                    () -> a.flush(TIMEOUT),
                    ex -> ex instanceof StreamSegmentNotExistsException);

            Assert.assertTrue("Unexpected value from mustFlush() after failed flush().", a.mustFlush());

            // Add the DeleteSegmentOperation - this should cause reconciliation to succeed.
            a.add(generateDeleteAndUpdateMetadata(a.getMetadata().getId(), context));
            a.flush(TIMEOUT).join();
            Assert.assertFalse("Unexpected value from mustFlush() after Deletion.", a.mustFlush());
            AssertExtensions.assertLessThan("Unexpected LUSN after flush.", 0, a.getLowestUncommittedSequenceNumber());
            Assert.assertTrue("Unexpected value from isDeleted() after Deletion.", a.getMetadata().isDeleted());
            Assert.assertTrue("Unexpected value from isDeletedInStorage() after Deletion.", a.getMetadata().isDeletedInStorage());

            // Verify that no segment exists in Storage after the flush.
            boolean existsInStorage = context.storage.exists(a.getMetadata().getName(), TIMEOUT).join();
            Assert.assertFalse("Segment still exists in Storage after Deletion.", existsInStorage);
        }
    }

    /**
     * Tests the ability to process and flush various Operations on empty (not yet created) Segments:
     * - Append
     * - Seal
     * - Truncate
     * - Merge (empty source or targets)
     */
    @Test
    public void testEmptySegment() throws Exception {
        final int appendLength = DEFAULT_CONFIG.getFlushThresholdBytes();
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        val append = context.transactionAggregators[0];
        val seal = context.transactionAggregators[1];
        val truncate = context.transactionAggregators[2];
        val mergeEmptySource = context.transactionAggregators[3]; // Empty SOURCE to non-empty target.
        val mergeEmptySourceTarget = context.transactionAggregators[4]; // Empty source to non-empty TARGET.
        val mergeEmptyTarget = context.transactionAggregators[5]; // Empty/non-empty source to empty TARGET.
        val mergeEmptyTargetEmptySource = context.transactionAggregators[6]; // Empty SOURCE to empty target.
        val mergeEmptyTargetNonEmptySource = context.transactionAggregators[7]; // Non-empty SOURCE to empty target.

        val allAggregators = new SegmentAggregator[]{append, seal, truncate, mergeEmptySource, mergeEmptySourceTarget,
                mergeEmptyTarget, mergeEmptyTargetEmptySource, mergeEmptyTargetNonEmptySource};
        val finalAggregators = new SegmentAggregator[]{append, seal, truncate, mergeEmptySourceTarget, mergeEmptyTarget};

        // Create zero-length segment which will be used as a target of a merge with a zero-length, not-yet-created segment.
        context.storage.create(mergeEmptySourceTarget.getMetadata().getName(), TIMEOUT).join();

        // Create a non-zero-length segment which will be used to merge into empty target.
        val nonEmptySourceMetadata = (UpdateableSegmentMetadata) mergeEmptyTargetNonEmptySource.getMetadata();
        nonEmptySourceMetadata.setLength(1L);
        nonEmptySourceMetadata.setStorageLength(1L);
        nonEmptySourceMetadata.markSealed();
        context.storage.create(nonEmptySourceMetadata.getName(), TIMEOUT)
                .thenCompose(v -> context.storage.openWrite(mergeEmptyTargetNonEmptySource.getMetadata().getName()))
                .thenCompose(handle -> context.storage.write(handle, 0L, new ByteArrayInputStream(new byte[1]), 1, TIMEOUT)
                        .thenCompose(v -> context.storage.seal(handle, TIMEOUT)))
                .join();

        // Initialize all the aggregators now, before adding operations for processing.
        for (val a : allAggregators) {
            a.initialize(TIMEOUT).join();
        }

        // Append on empty segment.
        append.add(generateAppendAndUpdateMetadata(append.getMetadata().getId(), new byte[appendLength], context));
        append.flush(TIMEOUT).join();
        Assert.assertEquals("Unexpected segment length after first write.",
                appendLength, context.storage.getStreamSegmentInfo(append.getMetadata().getName(), TIMEOUT).join().getLength());

        // Seal on empty segment.
        seal.add(generateSealAndUpdateMetadata(seal.getMetadata().getId(), context));
        seal.flush(TIMEOUT).join();
        Assert.assertTrue("Unexpected Metadata.isSealedInStorage after seal.", seal.getMetadata().isSealedInStorage());
        Assert.assertFalse("Not expecting segment to have been created in storage after seal.",
                context.storage.exists(seal.getMetadata().getName(), TIMEOUT).join());

        // Truncate on empty segment (a no-op).
        truncate.add(generateTruncateAndUpdateMetadata(truncate.getMetadata().getId(), context));
        truncate.flush(TIMEOUT).join();
        Assert.assertFalse("Not expecting segment to have been created in storage after truncate.",
                context.storage.exists(truncate.getMetadata().getName(), TIMEOUT).join());

        // Merge a zero-length, not-yet-created segment into a zero-length, created segment.
        mergeEmptySourceTarget.add(generateMergeTransactionAndUpdateMetadata(
                mergeEmptySourceTarget.getMetadata().getId(), mergeEmptySource.getMetadata().getId(), context));
        mergeEmptySourceTarget.flush(TIMEOUT).join();
        Assert.assertFalse("Merge source was created for initially empty segment.",
                context.storage.exists(mergeEmptySource.getMetadata().getName(), TIMEOUT).join());
        Assert.assertEquals("Unexpected length of pre-existing target segment after merge with empty segment.",
                0, context.storage.getStreamSegmentInfo(mergeEmptySourceTarget.getMetadata().getName(), TIMEOUT).join().getLength());
        Assert.assertTrue("Unexpected Metadata.IsDeletedInStorage for empty source", mergeEmptySource.getMetadata().isDeletedInStorage());

        // Merge an empty source into an empty target.
        mergeEmptyTarget.add(generateMergeTransactionAndUpdateMetadata(
                mergeEmptyTarget.getMetadata().getId(), mergeEmptyTargetEmptySource.getMetadata().getId(), context));
        mergeEmptyTarget.flush(TIMEOUT).join();
        Assert.assertFalse("Merge source was created for initially empty segment.",
                context.storage.exists(mergeEmptyTargetEmptySource.getMetadata().getName(), TIMEOUT).join());
        Assert.assertFalse("Merge target was created for initially empty segment.",
                context.storage.exists(mergeEmptyTarget.getMetadata().getName(), TIMEOUT).join());
        Assert.assertTrue("Unexpected Metadata.IsDeletedInStorage for empty source", mergeEmptyTargetEmptySource.getMetadata().isDeletedInStorage());

        // Merge a non-empty source segment into an empty target.
        mergeEmptyTarget.add(generateMergeTransactionAndUpdateMetadata(
                mergeEmptyTarget.getMetadata().getId(), mergeEmptyTargetNonEmptySource.getMetadata().getId(), context));
        mergeEmptyTarget.flush(TIMEOUT).join();
        Assert.assertFalse("Merge source still exists for initially non-empty segment.",
                context.storage.exists(mergeEmptyTargetNonEmptySource.getMetadata().getName(), TIMEOUT).join());
        Assert.assertEquals("Unexpected length of target segment after merge with non-empty segment.",
                1, context.storage.getStreamSegmentInfo(mergeEmptyTarget.getMetadata().getName(), TIMEOUT).join().getLength());
        Assert.assertTrue("Unexpected Metadata.IsDeletedInStorage for empty source", mergeEmptyTargetNonEmptySource.getMetadata().isDeletedInStorage());

        // Finally, check that everything was marked as flushed out of the aggregators.
        for (val a : finalAggregators) {
            Assert.assertFalse("Unexpected mustFlush() after flush", a.mustFlush());
            AssertExtensions.assertLessThan("Unexpected LUSN after flush.", 0, a.getLowestUncommittedSequenceNumber());
        }
    }

    /**
     * Tests the ability of the Segment Aggregator to create Segments in Storage using the appropriate Segment Rolling Policy.
     */
    @Test
    public void testSegmentRollingSize() throws Exception {
        val config = WriterConfig
                .builder()
                .with(WriterConfig.MAX_ROLLOVER_SIZE, 1024L)
                .with(WriterConfig.FLUSH_THRESHOLD_BYTES, DEFAULT_CONFIG.getFlushThresholdBytes())
                .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, DEFAULT_CONFIG.getFlushThresholdTime().toMillis())
                .with(WriterConfig.MAX_FLUSH_SIZE_BYTES, DEFAULT_CONFIG.getMaxFlushSizeBytes())
                .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, DEFAULT_CONFIG.getMaxReadTimeout().toMillis())
                .build();

        @Cleanup
        TestContext context = new TestContext(config);

        // Configure rollover size on segment 1 to be much higher than the limit.
        val a1 = context.segmentAggregator;
        ((UpdateableSegmentMetadata) a1.getMetadata()).updateAttributes(
                Collections.singletonMap(Attributes.ROLLOVER_SIZE, config.getMaxRolloverSize() * 10));
        val expectedRollover1 = config.getMaxRolloverSize();

        // Configure rollover size on segment 2 to be lower than the limit
        val a2 = context.transactionAggregators[0];
        ((UpdateableSegmentMetadata) a2.getMetadata()).updateAttributes(
                Collections.singletonMap(Attributes.ROLLOVER_SIZE, config.getMaxRolloverSize() / 2));
        val expectedRollover2 = config.getMaxRolloverSize() / 2;

        // Intercept the Storage.create() call so we can figure out if the appropriate rolling size is passed.
        val rolloverSizes = new HashMap<String, Long>();
        context.storage.setCreateInterceptor((segmentName, policy, s) -> {
            rolloverSizes.put(segmentName, policy.getMaxLength());
            return CompletableFuture.completedFuture(null);
        });

        a1.initialize(TIMEOUT).join();
        a1.add(generateAppendAndUpdateMetadata(0, a1.getMetadata().getId(), context));
        a1.add(generateSealAndUpdateMetadata(a1.getMetadata().getId(), context));

        a2.initialize(TIMEOUT).join();
        a2.add(generateAppendAndUpdateMetadata(0, a2.getMetadata().getId(), context));
        a2.add(generateSealAndUpdateMetadata(a2.getMetadata().getId(), context));
        flushAllSegments(context);
        Assert.assertEquals("Unexpected number of segments created.", 2, rolloverSizes.size());
        Assert.assertEquals("Unexpected rollover size when limited by configuration.",
                expectedRollover1, (long) rolloverSizes.get(a1.getMetadata().getName()));
        Assert.assertEquals("Unexpected rollover size when not limited by configuration.",
                expectedRollover2, (long) rolloverSizes.get(a2.getMetadata().getName()));
    }

    /**
     * Tests the case when a Segment's data is missing from the ReadIndex (but the Segment itself is not deleted).
     */
    @Test
    public void testSegmentMissingData() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Add one operation big enough to trigger a Flush.
        byte[] appendData = new byte[config.getFlushThresholdBytes() + 1];
        StorageOperation appendOp = generateAppendAndUpdateMetadata(SEGMENT_ID, appendData, context);

        context.segmentAggregator.add(appendOp);
        Assert.assertTrue("Unexpected value returned by mustFlush() (size threshold).", context.segmentAggregator.mustFlush());

        // Clear the append data.
        context.dataSource.clearAppendData();

        // Call flush() and verify it throws DataCorruptionException.
        AssertExtensions.assertSuppliedFutureThrows(
                "flush() did not throw when unable to read data from ReadIndex.",
                () -> context.segmentAggregator.flush(TIMEOUT),
                ex -> ex instanceof DataCorruptionException);
    }

    @Test
    public void testMismatchOperationAndStorageOffsets() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Add one operation big enough to trigger a Flush.
        byte[] appendData = new byte[config.getFlushThresholdBytes() + 1];
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        long offset = segmentMetadata.getLength();
        segmentMetadata.setLength(offset + appendData.length);

        // Normal Append so far.
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(appendData), null);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());

        context.dataSource.recordAppend(op);
        context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(op));
        Assert.assertTrue("Unexpected value returned by mustFlush() (size threshold).", context.segmentAggregator.mustFlush());

        // Call flush() and verify it throws DataCorruptionException.
        context.segmentAggregator.flush(TIMEOUT);

        // Add one operation big enough to trigger a Flush.
        segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        offset += segmentMetadata.getLength();
        segmentMetadata.setLength(offset + appendData.length);

        StreamSegmentAppendOperation op2 = new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(appendData), null);
        op2.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());

        // By some reason, assume that there is a discrepancy between the offset in Storage we know about this Segment
        // and the offset of the Append that is supposed to have.
        op2.setStreamSegmentOffset(offset - 1);
        context.dataSource.recordAppend(op2);

        // This should be detected upon adding the Operation to the Aggregator.
        AssertExtensions.assertThrows(DataCorruptionException.class, () -> context.segmentAggregator.add(new CachedStreamSegmentAppendOperation(op2)));
    }

    /**
     * Tests a scenario where the Segment has been deleted (in the metadata) while it was actively flushing. This verifies
     * that an ongoing flush operation will abort (i.e., eventually complete) so that the next iteration of the StorageWriter
     * may properly delete the segment.
     */
    @Test
    public void testSegmentDeletedWhileFlushing() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Add one append, followed by a Seal.
        StorageOperation appendOp = generateAppendAndUpdateMetadata(SEGMENT_ID, new byte[config.getFlushThresholdBytes() - 1], context);
        context.segmentAggregator.add(appendOp);
        context.segmentAggregator.add(generateSealAndUpdateMetadata(SEGMENT_ID, context));
        Assert.assertTrue("Unexpected value returned by mustFlush().", context.segmentAggregator.mustFlush());

        // Meanwhile, delete the segment (but do not notify the StorageWriter yet).
        val sm = (UpdateableSegmentMetadata) context.segmentAggregator.getMetadata();
        sm.markDeleted();
        val flushResult = context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Unexpected number of bytes flushed.", 0, flushResult.getFlushedBytes());
    }

    //endregion

    //region Unknown outcome operation reconciliation

    /**
     * Tests the ability of the SegmentAggregator to recover from situations when a Segment did not exist in Storage
     * when {@link SegmentAggregator#initialize} was invoked, but exists when the first byte needs to be appended.
     * This can happen when there are concurrent instances of the same Segment Container running at the same time and
     * one of them managed to create the Segment in Storage after the other one was initialized; when the second one tries
     * to do the same, it must gracefully recover from that situation.
     */
    @Test
    public void testReconcileCreateIfEmpty() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);

        // Initialize the Segment Aggregator, but do not yet create the segment.
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Write one operation.
        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
        StorageOperation appendOp = generateAppendAndUpdateMetadata(0, SEGMENT_ID, context);
        context.segmentAggregator.add(appendOp);
        getAppendData(appendOp, writtenData, context);

        // Create the segment in Storage.
        context.storage.create(SEGMENT_NAME, TIMEOUT).join();

        // Flush the data. The SegmentAggregator thinks the Segment does not exist in Storage, so this verifies that it
        // handles this situation elegantly.
        context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        Assert.assertTrue("Expecting mustFlush() == true.", context.segmentAggregator.mustFlush());
        val flushResult = context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of flushed bytes.", writtenData.size(), flushResult.getFlushedBytes());

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile AppendOperations (Cached/NonCached).
     */
    @Test
    public void testReconcileAppends() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int appendCount = 1000;
        final int failEvery = 3;
        final int partialFailEvery = 6;

        @Cleanup
        TestContext context = new TestContext(config);
        context.segmentAggregator.initialize(TIMEOUT).join();

        // The writes always succeed, but every few times we return some random error, indicating that they didn't.
        AtomicInteger writeCount = new AtomicInteger();
        AtomicReference<Exception> setException = new AtomicReference<>();
        context.storage.setWriteInterceptor((segmentName, offset, data, length, storage) -> {
            int wc = writeCount.incrementAndGet();
            if (wc % failEvery == 0) {
                if (wc % partialFailEvery == 0) {
                    // Only a part of the operation has been written. Verify that we can reconcile partially written
                    // operations as well.
                    length /= 2;
                }

                // Time to wreak some havoc.
                return storage.write(writeHandle(segmentName), offset, data, length, TIMEOUT)
                        .thenAccept(v -> {
                            IntentionalException ex = new IntentionalException(String.format("S=%s,O=%d", segmentName, offset));
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

        context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        while (context.segmentAggregator.mustFlush()) {
            // Call flush() and inspect the result.
            WriterFlushResult flushResult = null;

            try {
                flushResult = context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                Assert.assertNull("An exception was expected, but none was thrown.", setException.get());
                Assert.assertNotNull("No FlushResult provided.", flushResult);
            } catch (Exception ex) {
                if (setException.get() != null) {
                    Assert.assertEquals("Unexpected exception thrown.", setException.get(), Exceptions.unwrap(ex));
                } else {
                    // Only expecting a BadOffsetException after our own injected exception.
                    Throwable realEx = Exceptions.unwrap(ex);
                    Assert.assertTrue("Unexpected exception thrown: " + realEx, realEx instanceof BadOffsetException);
                }
            }

            // Check flush result.
            if (flushResult != null) {
                AssertExtensions.assertGreaterThan("Not enough bytes were flushed (time threshold).", 0, flushResult.getFlushedBytes());
                Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            }

            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        }

        // Verify data.
        byte[] expectedData = writtenData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile StreamSegmentSealOperations.
     */
    @Test
    public void testReconcileSeal() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create the segment. We test reconciliation with empty segments in testReconcileEmptySegment().
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        // The seal succeeds, but we throw some random error, indicating that it didn't.
        context.storage.setSealInterceptor((segmentName, storage) -> {
            storage.seal(writeHandle(segmentName), TIMEOUT).join();
            return Futures.failedFuture(new StreamSegmentNotExistsException(segmentName));
        });

        // Attempt to seal.
        StorageOperation sealOp = generateSealAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(sealOp);

        // First time: attempt to flush/seal, which must end in failure.
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException);

        context.storage.setSealInterceptor(null);
        // Second time: we are in reconciliation mode, so flush must succeed (and update internal state based on storage).
        context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify outcome.
        Assert.assertTrue("Segment not marked as sealed in storage (in metadata).", context.segmentAggregator.getMetadata().isSealedInStorage());
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile MergeTransactionOperations.
     */
    @Test
    public void testReconcileMerge() throws Exception {
        final int appendCount = 100;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a parent segment and one transaction segment.
        context.segmentAggregator.initialize(TIMEOUT).join();
        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
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
            transactionAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        }

        // The concat succeeds, but we throw some random error, indicating that it didn't.
        context.storage.setConcatInterceptor((targetSegment, offset, sourceSegment, storage) -> {
            storage.concat(writeHandle(targetSegment), offset, sourceSegment, TIMEOUT).join();
            throw new IntentionalException(String.format("T=%s,O=%d,S=%s", targetSegment, offset, sourceSegment));
        });

        // Attempt to concat.
        StorageOperation mergeOp = generateMergeTransactionAndUpdateMetadata(transactionAggregator.getMetadata().getId(), context);
        context.segmentAggregator.add(mergeOp);

        // First time: attempt to flush/seal, which must end in failure.
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> Exceptions.unwrap(ex) instanceof IntentionalException);

        // Second time: we are not yet in reconcilation mode, but we are about to detect that the Transaction segment
        // no longer exists
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> Exceptions.unwrap(ex) instanceof StreamSegmentNotExistsException);

        // Third time: we should be in reconciliation mode, and we should be able to recover from it.
        context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify outcome.
        Assert.assertFalse("Unexpected value from mustFlush() after merger reconciliation.", context.segmentAggregator.mustFlush());

        byte[] expectedData = transactionData.toByteArray();
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile an operation on the Source Segment after it has already been
     * merged in Storage. This situation would normally be detected by the initialize() method (as the Segment would be
     * deleted), however, in some cases, a previous instance of the same Container would still be running while we execute
     * the initialization, and it may be in the process of merging the Segment. If so, our call to initialize() would
     * still pick the Segment, but when we execute the operations in Storage, it would not be there anymore.
     */
    @Test
    public void testReconcileMergeSeal() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a parent segment and one transaction segment.
        context.segmentAggregator.initialize(TIMEOUT).join();

        SegmentAggregator transactionAggregator = context.transactionAggregators[0];
        context.storage.create(transactionAggregator.getMetadata().getName(), TIMEOUT).join();
        transactionAggregator.initialize(TIMEOUT).join();

        // This is the operation that should be reconciled.
        transactionAggregator.add(generateSealAndUpdateMetadata(transactionAggregator.getMetadata().getId(), context));
        val sm = context.containerMetadata.getStreamSegmentMetadata(transactionAggregator.getMetadata().getId());

        // Mark the Segment as merged, and then delete it from Storage.
        sm.markMerged();
        context.storage.delete(context.storage.openWrite(transactionAggregator.getMetadata().getName()).join(), TIMEOUT).join();

        // Verify the first invocation to flush() fails.
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected Segment to not exist.",
                () -> transactionAggregator.flush(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);

        // But the second time it must have worked.
        transactionAggregator.flush(TIMEOUT).join();
        Assert.assertTrue("Expected metadata to be updated properly.", sm.isDeleted());
    }

    /**
     * Tests the ability of the SegmentAggregator to reconcile StreamSegmentTruncateOperations.
     */
    @Test
    public void testReconcileTruncate() throws Exception {
        val rnd = new Random(0);
        byte[] storageData = new byte[100];
        rnd.nextBytes(storageData);

        // Write some data to the segment in Storage.
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();
        context.storage.openWrite(context.segmentAggregator.getMetadata().getName())
                       .thenCompose(h -> context.storage.write(h, 0, new ByteArrayInputStream(storageData), storageData.length, TIMEOUT)).join();
        val sm = context.containerMetadata.getStreamSegmentMetadata(context.segmentAggregator.getMetadata().getId());
        sm.setLength(storageData.length);
        sm.setStorageLength(storageData.length);

        // The truncate succeeds, but we throw some random error, indicating that it didn't.
        context.storage.setTruncateInterceptor((segmentName, offset, storage) -> {
            context.storage.truncateDirectly(writeHandle(segmentName), offset);
            throw new IntentionalException(String.format("S=%s", segmentName));
        });

        // Attempt to seal.
        StorageOperation truncateOp = generateTruncateAndUpdateMetadata(SEGMENT_ID, context);
        context.segmentAggregator.add(truncateOp);

        // First time: attempt to flush/truncate, which must end in failure.
        AssertExtensions.assertThrows(
                "IntentionalException did not propagate to flush() caller.",
                () -> context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS),
                ex -> Exceptions.unwrap(ex) instanceof IntentionalException);

        context.storage.setTruncateInterceptor(null);

        // Second time: we are in reconciliation mode, so flush must succeed (and update internal state based on storage).
        context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // Verify outcome.
        Assert.assertEquals("Unexpected truncation offset in Storage.", truncateOp.getStreamSegmentOffset(),
                context.storage.getTruncationOffset(context.segmentAggregator.getMetadata().getName()));
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

        @Cleanup
        TestContext context = new TestContext(config);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        context.segmentAggregator.initialize(TIMEOUT).join();

        @Cleanup
        ByteArrayOutputStream writtenData = new ByteArrayOutputStream();
        ArrayList<StorageOperation> appendOperations = new ArrayList<>();
        ArrayList<InputStream> appendData = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            // Add another operation and record its length.
            StorageOperation appendOp = generateAppendAndUpdateMetadata(i, SEGMENT_ID, context);
            appendOperations.add(appendOp);
            val adStream = new ByteBufferOutputStream((int) appendOp.getLength());
            getAppendData(appendOp, adStream, context);
            appendData.add(adStream.getData().getReader());
            writtenData.write(adStream.getData().getCopy());
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
                    context.storage.write(writeHandle(SEGMENT_NAME), offset, appendData.get(j), appendData.get(j).available(), TIMEOUT).join();
                }
            }

            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            int flushLoopCount = 0;
            while (context.segmentAggregator.mustFlush()) {
                try {
                    flushCount++;
                    context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    errorCount++;
                    Assert.assertTrue("", Exceptions.unwrap(ex) instanceof BadOffsetException);
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
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
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
        final int appendCount = 100;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            context.storage.create(a.getMetadata().getName(), TIMEOUT).join();
        }

        // Store written data by segment - so we can check it later.
        HashMap<Long, ByteArrayOutputStream> dataBySegment = new HashMap<>();
        ArrayList<StorageOperation> operations = new ArrayList<>();
        val expectedMergeOpAck = new ArrayList<Map.Entry<Long, Long>>();

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
                context.dataSource.deleteAllAttributes(context.containerMetadata.getStreamSegmentMetadata(transactionId), TIMEOUT).join();
                expectedMergeOpAck.add(new AbstractMap.SimpleImmutableEntry<>(context.segmentAggregator.getMetadata().getId(), transactionId));
            }
        }

        // Populate the storage.
        for (Map.Entry<Long, ByteArrayOutputStream> e : dataBySegment.entrySet()) {
            context.storage.write(
                    writeHandle(context.containerMetadata.getStreamSegmentMetadata(e.getKey()).getName()),
                    0,
                    new ByteArrayInputStream(e.getValue().toByteArray()),
                    e.getValue().size(),
                    TIMEOUT).join();
        }

        // Initialize the SegmentAggregators, before doing any Storage deletes. This will prevent them from properly
        // updating the Metadata with the appropriate state (we deliberately choose this order because it is more likely
        // that the Metadata and Storage state are out of sync during recovery).
        context.segmentAggregator.initialize(TIMEOUT).join();
        for (SegmentAggregator a : context.transactionAggregators) {
            a.initialize(TIMEOUT).join();
        }

        for (SegmentAggregator a : context.transactionAggregators) {
            if (a.getMetadata().isSealed()) {
                context.storage.seal(writeHandle(a.getMetadata().getName()), TIMEOUT).join();
            }

            if (a.getMetadata().isMerged() || a.getMetadata().isDeleted()) {
                context.storage.delete(writeHandle(a.getMetadata().getName()), TIMEOUT).join();
            }
        }

        // Add all operations we had so far.
        val actualMergeOpAck = new ArrayList<Map.Entry<Long, Long>>();
        context.dataSource.setCompleteMergeCallback((target, source) -> {
            // The ReadIndex performs a similar check.
            Preconditions.checkArgument(context.containerMetadata.getStreamSegmentMetadata(source).isDeleted(),
                    "Cannot completeMerge() a Segment that is not marked as Deleted.");
            actualMergeOpAck.add(new AbstractMap.SimpleImmutableEntry<Long, Long>(target, source));
        });
        for (StorageOperation o : operations) {
            int transactionIndex = (int) (o.getStreamSegmentId() - TRANSACTION_ID_START);
            SegmentAggregator a = transactionIndex < 0 ? context.segmentAggregator : context.transactionAggregators[transactionIndex];
            a.add(o);
        }
        context.dataSource.setCompleteMergeCallback(null);

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
        verifySegmentData(parentData.toByteArray(), context);
        AssertExtensions.assertListEquals("Unexpected callback calls to completeMerge for already processed operations.",
                expectedMergeOpAck, actualMergeOpAck, Map.Entry::equals);
    }

    /**
     * Tests a scenario where data that is about to be added already partially exists in Storage. This would most likely
     * happen in a recovery situation, where we committed a part of an append operation before failing over.
     */
    @Test
    public void testRecoveryPartialWrite() throws Exception {
        final int writeLength = 1024;
        final int partialWriteLength = writeLength / 2;

        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);
        context.storage.create(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join();

        // Store written data by segment - so we can check it later.
        ArrayList<StorageOperation> operations = new ArrayList<>();

        byte[] writtenData = new byte[writeLength];
        val rnd = new Random(0);
        rnd.nextBytes(writtenData);
        StorageOperation appendOp = generateAppendAndUpdateMetadata(context.segmentAggregator.getMetadata().getId(), writtenData, context);
        operations.add(appendOp);
        operations.add(generateSealAndUpdateMetadata(context.segmentAggregator.getMetadata().getId(), context));

        // Write half of the data to Storage.
        context.storage.write(
                writeHandle(context.segmentAggregator.getMetadata().getName()),
                0,
                new ByteArrayInputStream(writtenData),
                partialWriteLength,
                TIMEOUT).join();

        // Initialize the SegmentAggregator. This should pick up the half-written operation.
        context.segmentAggregator.initialize(TIMEOUT).join();
        Assert.assertEquals("", partialWriteLength, context.segmentAggregator.getMetadata().getStorageLength());

        // Add all operations we had so far.
        for (StorageOperation o : operations) {
            context.segmentAggregator.add(o);
        }

        flushAllSegments(context);

        // Verify that in the end, the contents of the parents is as expected.
        verifySegmentData(writtenData, context);
    }

    /**
     * Tests a scenario where a MergeSegmentOperation needs to be recovered but which has already been merged in Storage.
     */
    @Test
    public void testRecoveryEmptyMergeOperation() throws Exception {
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // Create a parent segment and one transaction segment.
        context.segmentAggregator.initialize(TIMEOUT).join();

        // Part 1: When the source segment is missing from Storage, but metadata does not reflect that.
        SegmentAggregator ta0 = context.transactionAggregators[0];
        context.storage.create(ta0.getMetadata().getName(), TIMEOUT).join();
        context.storage.openWrite(ta0.getMetadata().getName())
                .thenCompose(txnHandle -> context.storage.seal(txnHandle, TIMEOUT)).join();
        val txn0Metadata = context.containerMetadata.getStreamSegmentMetadata(ta0.getMetadata().getId());
        txn0Metadata.markSealed();
        txn0Metadata.markSealedInStorage();
        ta0.initialize(TIMEOUT).join();
        context.storage.delete(context.storage.openWrite(txn0Metadata.getName()).join(), TIMEOUT).join();

        // This is the operation that should be reconciled.
        context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(ta0.getMetadata().getId(), context));

        // Verify the operation was ack-ed.
        AtomicBoolean mergeAcked = new AtomicBoolean();
        context.dataSource.setCompleteMergeCallback((target, source) -> mergeAcked.set(true));
        context.segmentAggregator.flush(TIMEOUT).join();
        Assert.assertTrue("Merge was not ack-ed for deleted source segment.", mergeAcked.get());

        // Part 2: When the source segment's metadata indicates it was deleted.
        SegmentAggregator ta1 = context.transactionAggregators[1];
        context.storage.create(ta1.getMetadata().getName(), TIMEOUT).join();
        context.storage.openWrite(ta1.getMetadata().getName())
                .thenCompose(txnHandle -> context.storage.seal(txnHandle, TIMEOUT)).join();
        val txn1Metadata = context.containerMetadata.getStreamSegmentMetadata(ta1.getMetadata().getId());
        txn1Metadata.markDeleted();

        // This is the operation that should be reconciled.
        context.segmentAggregator.add(generateMergeTransactionAndUpdateMetadata(ta1.getMetadata().getId(), context));

        // Verify the operation was ack-ed.
        mergeAcked.set(false);
        context.dataSource.setCompleteMergeCallback((target, source) -> mergeAcked.set(true));
        context.segmentAggregator.flush(TIMEOUT).join();

        // Finally, verify that all operations were ack-ed back.
        Assert.assertTrue("Merge was not ack-ed for deleted source segment.", mergeAcked.get());
    }

    //endregion

    //region Helpers

    private void getAppendData(StorageOperation operation, OutputStream stream, TestContext context) {
        Assert.assertTrue("Not an append operation: " + operation, operation instanceof CachedStreamSegmentAppendOperation);
        BufferView result = context.dataSource.getAppendData(operation.getStreamSegmentId(), operation.getStreamSegmentOffset(), (int) operation.getLength());
        try {
            result.copyTo(stream);
        } catch (IOException ex) {
            Assert.fail("Not expecting this exception: " + ex);
        }
    }

    private StorageOperation generateMergeTransactionAndUpdateMetadata(long transactionId, TestContext context) {
        return generateMergeTransactionAndUpdateMetadata(context.transactionIds.get(transactionId), transactionId, context);
    }

    private StorageOperation generateMergeTransactionAndUpdateMetadata(long targetId, long sourceId, TestContext context) {
        UpdateableSegmentMetadata sourceMetadata = context.containerMetadata.getStreamSegmentMetadata(sourceId);
        UpdateableSegmentMetadata targetMetadata = context.containerMetadata.getStreamSegmentMetadata(targetId);

        MergeSegmentOperation op = new MergeSegmentOperation(targetMetadata.getId(), sourceMetadata.getId());
        op.setLength(sourceMetadata.getLength());
        op.setStreamSegmentOffset(targetMetadata.getLength());

        targetMetadata.setLength(targetMetadata.getLength() + sourceMetadata.getLength());
        sourceMetadata.markMerged();
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return op;
    }

    private StorageOperation generateSimpleMergeTransaction(long transactionId, TestContext context) {
        UpdateableSegmentMetadata transactionMetadata = context.containerMetadata.getStreamSegmentMetadata(transactionId);
        UpdateableSegmentMetadata parentMetadata = context.containerMetadata.getStreamSegmentMetadata(context.transactionIds.get(transactionMetadata.getId()));

        MergeSegmentOperation op = new MergeSegmentOperation(parentMetadata.getId(), transactionMetadata.getId());
        op.setLength(transactionMetadata.getLength());
        op.setStreamSegmentOffset(parentMetadata.getLength());

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
        sealOp.setStreamSegmentOffset(segmentMetadata.getLength());
        sealOp.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return sealOp;
    }

    private StorageOperation generateAppendAndUpdateMetadata(int appendId, long segmentId, TestContext context) {
        byte[] data = String.format("Append_%d", appendId).getBytes();
        return generateAppendAndUpdateMetadata(segmentId, data, context);
    }

    private StorageOperation generateAppendAndUpdateMetadata(long segmentId, byte[] data, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long offset = segmentMetadata.getLength();
        segmentMetadata.setLength(offset + data.length);

        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, new ByteArraySegment(data), null);
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());

        context.dataSource.recordAppend(op);
        return new CachedStreamSegmentAppendOperation(op);
    }

    private StorageOperation generateSimpleAppend(long segmentId, TestContext context) {
        byte[] data = "Append_Dummy".getBytes();
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long offset = segmentMetadata.getLength();
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, new ByteArraySegment(data), null);
        op.setStreamSegmentOffset(offset);
        return op;
    }

    private StorageOperation generateTruncateAndUpdateMetadata(long segmentId, TestContext context) {
        UpdateableSegmentMetadata segmentMetadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        long truncateOffset = segmentMetadata.getLength() / 2;
        segmentMetadata.setStartOffset(truncateOffset);
        StreamSegmentTruncateOperation op = new StreamSegmentTruncateOperation(segmentId, truncateOffset);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return op;
    }

    private StorageOperation generateDeleteAndUpdateMetadata(long segmentId, TestContext context) {
        UpdateableSegmentMetadata metadata = context.containerMetadata.getStreamSegmentMetadata(segmentId);
        metadata.markDeleted();
        DeleteSegmentOperation op = new DeleteSegmentOperation(segmentId);
        op.setStreamSegmentOffset(metadata.getLength());
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
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
                    WriterFlushResult transactionFlushResult = transactionAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                    anythingFlushed = anythingFlushed | transactionFlushResult.getFlushedBytes() > 0;
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                WriterFlushResult parentFlushResult = context.segmentAggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
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
                    WriterFlushResult transactionFlushResult = tryFlushSegment(transactionAggregator, exceptionProvider);
                    anythingFlushed = anythingFlushed | (transactionFlushResult == null || transactionFlushResult.getFlushedBytes() > 0);
                }
            }

            if (context.segmentAggregator.mustFlush()) {
                exceptionReset.run();
                WriterFlushResult parentFlushResult = tryFlushSegment(context.segmentAggregator, exceptionProvider);
                anythingFlushed = anythingFlushed | (parentFlushResult == null || (parentFlushResult.getFlushedBytes() + parentFlushResult.getMergedBytes()) > 0);
            }
        }
    }

    private <T extends Throwable> WriterFlushResult tryFlushSegment(SegmentAggregator aggregator, Supplier<T> exceptionProvider) {
        try {
            WriterFlushResult flushResult = aggregator.flush(TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            T expectedException = exceptionProvider.get();
            Assert.assertNull("Expected an exception but none got thrown.", expectedException);
            Assert.assertNotNull("Expected a FlushResult.", flushResult);
            return flushResult;
        } catch (Throwable ex) {
            ex = Exceptions.unwrap(ex);
            T expectedException = exceptionProvider.get();
            Assert.assertEquals("Unexpected exception or no exception got thrown.", expectedException, ex);
            return null;
        }
    }

    private void verifySegmentData(byte[] expectedData, TestContext context) {
        byte[] actualData = new byte[expectedData.length];
        long storageLength = context.storage.getStreamSegmentInfo(context.segmentAggregator.getMetadata().getName(), TIMEOUT).join().getLength();
        Assert.assertEquals("Unexpected number of bytes flushed/merged to Storage.", expectedData.length, storageLength);
        context.storage.read(readHandle(context.segmentAggregator.getMetadata().getName()), 0, actualData, 0, actualData.length, TIMEOUT).join();
        Assert.assertArrayEquals("Unexpected data written to storage.", expectedData, actualData);
    }

    private SegmentHandle writeHandle(String segmentName) {
        return InMemoryStorage.newHandle(segmentName, false);
    }

    private SegmentHandle readHandle(String segmentName) {
        return InMemoryStorage.newHandle(segmentName, true);
    }

    //endregion

    // region TestContext

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata containerMetadata;
        final TestWriterDataSource dataSource;
        final TestStorage storage;
        final ManualTimer timer;
        final SegmentAggregator segmentAggregator;
        final SegmentAggregator[] transactionAggregators;
        final Map<Long, Long> transactionIds;

        TestContext(WriterConfig config) {
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.storage = new TestStorage(new InMemoryStorage(), executorService());
            this.storage.initialize(1);
            this.timer = new ManualTimer();
            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = TestWriterDataSource.DataSourceConfig.NO_METADATA_CHECKPOINT;
            this.dataSource = new TestWriterDataSource(this.containerMetadata, executorService(), dataSourceConfig);
            this.transactionAggregators = new SegmentAggregator[TRANSACTION_COUNT];
            UpdateableSegmentMetadata segmentMetadata = initialize(this.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID));
            this.segmentAggregator = new SegmentAggregator(segmentMetadata, this.dataSource, this.storage, config, this.timer, executorService());
            this.transactionIds = new HashMap<>();
            for (int i = 0; i < TRANSACTION_COUNT; i++) {
                String name = TRANSACTION_NAME_PREFIX + i;
                long id = TRANSACTION_ID_START + i;
                this.transactionIds.put(id, SEGMENT_ID);
                UpdateableSegmentMetadata transactionMetadata = initialize(this.containerMetadata.mapStreamSegmentId(name, TRANSACTION_ID_START + i));
                this.transactionAggregators[i] = new SegmentAggregator(transactionMetadata, this.dataSource, this.storage, config, this.timer, executorService());
            }
        }

        void increaseTime(long deltaMillis) {
            this.timer.setElapsedMillis(this.timer.getElapsedMillis() + deltaMillis);
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
            segmentMetadata.setLength(0);
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

    // endregion
}
