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

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.logs.operations.AttributeUpdaterOperation;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ErrorInjector;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit test for the {@link AttributeAggregator} class.
 */
public class AttributeAggregatorTests extends ThreadPooledTestSuite {
    private static final int CONTAINER_ID = 0;
    private static final long SEGMENT_ID = 123;
    private static final String SEGMENT_NAME = "Segment";
    private static final byte[] APPEND_DATA = SEGMENT_NAME.getBytes();
    private static final AttributeId CORE_ATTRIBUTE_ID = Attributes.EVENT_COUNT;
    private static final List<AttributeId> EXTENDED_ATTRIBUTE_IDS = Collections.unmodifiableList(
            IntStream.range(0, 20).mapToObj(i -> AttributeId.randomUUID()).collect(Collectors.toList()));
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final Duration SHORT_TIMEOUT = Duration.ofSeconds(5);
    private static final WriterConfig DEFAULT_CONFIG = WriterConfig
            .builder()
            .with(WriterConfig.FLUSH_ATTRIBUTES_THRESHOLD, 10)
            .with(WriterConfig.FLUSH_THRESHOLD_MILLIS, 1000L)
            .with(WriterConfig.MIN_READ_TIMEOUT_MILLIS, 10L)
            .build();
    @Rule
    public Timeout globalTimeout = Timeout.seconds(TIMEOUT.getSeconds());

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the {@link AttributeAggregator#add} method with valid and invalid operations.
     */
    @Test
    public void testAdd() throws Exception {
        final AttributeId extendedId = EXTENDED_ATTRIBUTE_IDS.get(0);
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We want to make sure we do not prematurely acknowledge anything.
        context.dataSource.setCompleteMergeCallback((target, source) -> Assert.fail("Not expecting any merger callbacks yet."));

        // Add non-Attributes.
        context.aggregator.add(new StreamSegmentTruncateOperation(SEGMENT_ID, 1234));

        // Add some attributes
        context.aggregator.add(new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(new byte[123]),
                AttributeUpdateCollection.from(createAttributeUpdate(extendedId, 1))));
        context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(createAttributeUpdate(extendedId, 2))));
        Assert.assertFalse("Unexpected value from mustFlush().", context.aggregator.mustFlush());

        // Seal using operation.
        context.aggregator.add(new StreamSegmentSealOperation(SEGMENT_ID));
        Assert.assertTrue("Unexpected value from mustFlush() after sealing.", context.aggregator.mustFlush());

        // Verify further adds fail.
        AssertExtensions.assertThrows(
                "No operations should be allowed after sealing.",
                () -> context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                        AttributeUpdateCollection.from(createAttributeUpdate(extendedId, 3)))),
                ex -> ex instanceof DataCorruptionException);

        // Delete and verify nothing else changes.
        context.segmentMetadata.markDeleted();
        context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                AttributeUpdateCollection.from(createAttributeUpdate(extendedId, 4))));
        Assert.assertFalse("Unexpected value from mustFlush() after deleting.", context.aggregator.mustFlush());
    }

    /**
     * Tests the {@link AttributeAggregator#getLowestUncommittedSequenceNumber()} method under the following cases:
     * 1. No activity.
     * 2. We have pending operations, no pending flushes.
     * 3. No pending operations, with one pending flush.
     * 4. We have pending operations, with one pending flush.
     * 5. We have pending operations, with a completed flush.
     * 6. We have pending operations, with multiple pending flushes.
     * 7. No pending operations, no pending flushes.
     * 8. When errors are present.
     */
    @Test
    public void testLowestUncommittedSequenceNumber() throws Exception {
        WriterConfig config = DEFAULT_CONFIG;
        @Cleanup
        TestContext context = new TestContext(config);

        // Setup an interceptor that will record (and block) all calls to update the root pointer.
        val flushInterceptors = Collections.<RootPointerInterceptor>synchronizedList(new ArrayList<>());
        context.dataSource.setNotifyAttributesPersistedInterceptor((rootPointer, lastSeqNo) -> {
            val rpi = new RootPointerInterceptor(rootPointer, lastSeqNo);
            flushInterceptors.add(rpi);
            return rpi.future.thenApply(v -> false); // Do not perform root pointer validation here.
        });

        long expectedLUSN = Operation.NO_SEQUENCE_NUMBER; // Blank segment.

        // 1. New aggregator (No pending operations, no pending flushes).
        Assert.assertEquals("Unexpected LUSN for new aggregator.",
                expectedLUSN, context.aggregator.getLowestUncommittedSequenceNumber());

        // 2. Add some operations (Pending operations, no pending flushes).
        val op1 = generateUpdateAttributesAndUpdateMetadata(1, context);
        context.aggregator.add(op1);
        expectedLUSN = op1.getSequenceNumber();
        val op2 = generateUpdateAttributesAndUpdateMetadata(1, context);
        context.aggregator.add(op2);
        Assert.assertEquals("Unexpected LUSN before first flush.",
                expectedLUSN, context.aggregator.getLowestUncommittedSequenceNumber());

        // 3. Flush, but do not add any new operations (No pending operations, pending flush).
        Assert.assertEquals("Not expecting any calls yet.", 0, flushInterceptors.size());
        val flush1 = forceTimeFlush(context);
        Assert.assertEquals(2, flush1.getFlushedAttributes());
        assertEventuallyEquals("Expecting a new interceptor to be registered.", 1, flushInterceptors::size);
        Assert.assertEquals("Unexpected LUSN while first flush pending.",
                expectedLUSN, context.aggregator.getLowestUncommittedSequenceNumber());
        val rpi1 = flushInterceptors.get(0);
        Assert.assertEquals("Unexpected RP.LastSeqNo.", op2.getSequenceNumber(), rpi1.lastSeqNo);

        // 4. Add some pending operations (Pending operations, one pending flush). Not expecting any LUSN change.
        val op3 = generateUpdateAttributesAndUpdateMetadata(1, context);
        context.aggregator.add(op3);
        Assert.assertEquals("Unexpected LUSN with pending operations and flushes.",
                expectedLUSN, context.aggregator.getLowestUncommittedSequenceNumber());

        // 5. Complete the flush (Pending operations, no pending flushes). Expecting LUSN change.
        rpi1.future.complete(null);
        expectedLUSN = op3.getSequenceNumber();
        assertEventuallyEquals("Unexpected LUSN with pending operations and no pending flushes.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        // 6. Add multiple operations and multiple flushes.
        val flush2 = forceTimeFlush(context);
        Assert.assertEquals(1, flush2.getFlushedAttributes()); // op3
        assertEventuallyEquals("Expecting a new interceptor to be registered.", 2, flushInterceptors::size);
        val rpi2 = flushInterceptors.get(1);
        Assert.assertEquals("Unexpected RP.LastSeqNo.", op3.getSequenceNumber(), rpi2.lastSeqNo);

        val op4 = generateAppendAndUpdateMetadata(1, context);
        context.aggregator.add(op4);
        val flush3 = forceTimeFlush(context);
        Assert.assertEquals(1, flush3.getFlushedAttributes()); // op4
        Assert.assertEquals(2, flushInterceptors.size()); // Not expecting second one to register yet.

        val op5 = generateAppendAndUpdateMetadata(1, context);
        context.aggregator.add(op5);
        val flush4 = forceTimeFlush(context);
        Assert.assertEquals(1, flush4.getFlushedAttributes()); // op5
        Assert.assertEquals(2, flushInterceptors.size()); // Not expecting another one to register yet.

        assertEventuallyEquals("Unexpected LUSN with no pending operations and multiple pending flushes.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        val op6 = generateAppendAndUpdateMetadata(1, context);
        context.aggregator.add(op6);

        // The aggregator should only have op6 as outstanding, and have 3 pending flushes.
        assertEventuallyEquals("Unexpected LUSN with pending operations and multiple pending flushes.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        // Start releasing flushes. We should expect flushes 3 and 4 to be coalesced into a single one.
        rpi2.future.complete(null);

        // op3 has completed, but due to async callbacks, it's not possible to determine accurately the next op seq no,
        // so we can only expect it to increase by 1.
        expectedLUSN++;
        assertEventuallyEquals("Unexpected LUSN with pending operations and multiple pending flushes (completed 1 flush).",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        // Verify that the last 2 flushes have been merged into a single request.
        assertEventuallyEquals("Expecting a new interceptor to be registered.", 3, flushInterceptors::size);
        val rpi3 = flushInterceptors.get(2);
        Assert.assertEquals("Expected multiple flush requests to have combined notifications.", op5.getSequenceNumber(), rpi3.lastSeqNo);

        // Release the last flush.
        rpi3.future.complete(null);
        expectedLUSN = op6.getSequenceNumber();

        assertEventuallyEquals("Unexpected LUSN with one operation and multiple flushes having completed.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        val flush5 = forceTimeFlush(context);
        Assert.assertEquals(1, flush5.getFlushedAttributes()); // op6
        assertEventuallyEquals("Expecting a new interceptor to be registered.", 4, flushInterceptors::size);
        val rpi4 = flushInterceptors.get(3);
        Assert.assertEquals("Unexpected Last SeqNo for last flush..", op6.getSequenceNumber(), rpi4.lastSeqNo);
        rpi4.future.complete(null);

        expectedLUSN = Operation.NO_SEQUENCE_NUMBER;
        assertEventuallyEquals("Unexpected LUSN when no activity left in the aggregator.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);

        // Final check: error handling
        val op7 = generateUpdateAttributesAndUpdateMetadata(1, context);
        context.aggregator.add(op7);
        val flush6 = forceTimeFlush(context);
        Assert.assertEquals(1, flush6.getFlushedAttributes()); // op7
        assertEventuallyEquals("Expecting a new interceptor to be registered.", 5, flushInterceptors::size);
        val rpi5 = flushInterceptors.get(4);
        expectedLUSN = op7.getSequenceNumber();
        assertEventuallyEquals("Unexpected LUSN with single operation.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);
        rpi5.future.completeExceptionally(new IntentionalException());
        expectedLUSN = Operation.NO_SEQUENCE_NUMBER;
        assertEventuallyEquals("Unexpected LUSN after failure.",
                expectedLUSN, context.aggregator::getLowestUncommittedSequenceNumber);
    }

    /**
     * Tests {@link AttributeAggregator#flush}.
     */
    @Test
    public void testFlush() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int attributesPerUpdate = Math.max(1, config.getFlushAttributesThreshold() / 5);
        final int updateCount = config.getFlushAttributesThreshold() * 10;

        @Cleanup
        TestContext context = new TestContext(config);
        val outstandingAttributes = new HashSet<AttributeId>();
        val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);

        // Part 0: Empty operations.
        context.aggregator.add(generateUpdateAttributesAndUpdateMetadata(0, context));
        Assert.assertFalse("Unexpected value returned by mustFlush() after empty operation.", context.aggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after empty operation.",
                firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());

        // Part 1: flush triggered by accumulated counts.
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
            context.aggregator.add(op);

            boolean expectFlush = outstandingAttributes.size() >= config.getFlushAttributesThreshold();
            Assert.assertEquals("Unexpected value returned by mustFlush() (count threshold).",
                    expectFlush, context.aggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (count threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());

            // Call flush() and inspect the result.
            WriterFlushResult flushResult = context.aggregator.flush(TIMEOUT).join();
            if (expectFlush) {
                Assert.assertFalse("Unexpected value returned by mustFlush() after flush (count threshold).", context.aggregator.mustFlush());
                Assert.assertEquals("Not all attributes were flushed (count threshold).",
                        outstandingAttributes.size(), flushResult.getFlushedAttributes());
                checkAttributes(context);
                checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
                outstandingAttributes.clear();
                firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
                lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);

                assertEventuallyEquals(
                        "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (count threshold).",
                        firstOutstandingSeqNo.get(), context.aggregator::getLowestUncommittedSequenceNumber);
            } else {
                Assert.assertEquals(String.format("Not expecting a flush. OutstandingCount=%s, Threshold=%d",
                        outstandingAttributes.size(), config.getFlushThresholdBytes()),
                        0, flushResult.getFlushedBytes());
            }
        }

        // Part 2: flush triggered by time.
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
            context.aggregator.add(op);

            // Call flush() and inspect the result.
            Assert.assertFalse("Unexpected value returned by mustFlush() before time elapsed (time threshold).", context.aggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());
            val flushResult = forceTimeFlush(context);

            // We are always expecting a flush.
            Assert.assertEquals("Not all attributes were flushed (time threshold).", outstandingAttributes.size(), flushResult.getFlushedAttributes());
            checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
            outstandingAttributes.clear();
            firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
            lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);

            assertEventuallyEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator::getLowestUncommittedSequenceNumber);
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            checkAttributes(context);
        }
    }

    /**
     * Tests {@link AttributeAggregator#flush} with the force flag set.
     */
    @Test
    public void testFlushForce() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int attributesPerUpdate = Math.max(1, config.getFlushAttributesThreshold() / 5);
        final int updateCount = 10;

        @Cleanup
        TestContext context = new TestContext(config);
        val outstandingAttributes = new HashSet<AttributeId>();
        val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);

        // Part 0: Empty operations.
        context.aggregator.add(generateUpdateAttributesAndUpdateMetadata(0, context));
        Assert.assertFalse("Unexpected value returned by mustFlush() after empty operation.", context.aggregator.mustFlush());
        Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after empty operation.",
                firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());

        // Do an initial fill-up. This will verify that force-flush works (just like no-force-flush) when there's sufficient
        // attributes accumulated.
        for (int i = 0; i < config.getFlushAttributesThreshold(); i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
            context.aggregator.add(op);
        }

        Assert.assertTrue("Unexpected value returned by mustFlush() after initial fill-up.", context.aggregator.mustFlush());
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
            context.aggregator.add(op);

            boolean expectFlush = outstandingAttributes.size() >= config.getFlushAttributesThreshold();
            Assert.assertEquals("Unexpected value returned by mustFlush() (count threshold).",
                    expectFlush, context.aggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (count threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());

            // Call flush() and inspect the result.
            WriterFlushResult flushResult = context.aggregator.flush(true, TIMEOUT).join();
            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (count threshold).", context.aggregator.mustFlush());
            Assert.assertEquals("Not all attributes were flushed (count threshold).",
                    outstandingAttributes.size(), flushResult.getFlushedAttributes());
            checkAttributes(context);
            checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
            outstandingAttributes.clear();
            firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
            lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);

            assertEventuallyEquals(
                    "Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (count threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator::getLowestUncommittedSequenceNumber);
        }
    }

    /**
     * Tests {@link AttributeAggregator#flush} in the presence of generic errors.
     */
    @Test
    public void testFlushWithGenericErrors() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);

        // Add a single operation, which alone should trigger the flush.
        AttributeUpdaterOperation op = generateUpdateAttributesAndUpdateMetadata(config.getFlushAttributesThreshold(), context);
        context.aggregator.add(op);
        Assert.assertTrue("Unexpected result from mustFlush().", context.aggregator.mustFlush());

        // Cause the attribute update to fail, and validate that the error is bubbled up.
        context.dataSource.setPersistAttributesErrorInjector(new ErrorInjector<>(i -> true, IntentionalException::new));
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected flush() to have failed.",
                () -> context.aggregator.flush(TIMEOUT),
                ex -> ex instanceof IntentionalException);
        Assert.assertTrue("Unexpected result from mustFlush() after failed attempt.", context.aggregator.mustFlush());
        checkAutoAttributes(Operation.NO_SEQUENCE_NUMBER, context);

        // Now try again, without errors.
        context.dataSource.setPersistAttributesErrorInjector(null);
        val result = context.aggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Unexpected number of attributes flushed",
                op.getAttributeUpdates().size() - 1, result.getFlushedAttributes()); // Subtract 1 for core attributes.
        checkAttributes(context);
        checkAutoAttributesEventual(op.getSequenceNumber(), context);
    }

    /**
     * Tests {@link AttributeAggregator#flush} in the presence of expected errors (when the segment is sealed or deleted).
     */
    @Test
    public void testFlushWithExpectedErrors() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;

        @Cleanup
        TestContext context = new TestContext(config);

        // Add a single operation, which alone should trigger the flush.
        val op1 = generateUpdateAttributesAndUpdateMetadata(config.getFlushAttributesThreshold(), context);
        context.aggregator.add(op1);
        Assert.assertTrue("Unexpected result from mustFlush().", context.aggregator.mustFlush());

        // Segment is reported to be sealed.
        context.dataSource.setPersistAttributesErrorInjector(new ErrorInjector<>(i -> true, () -> new StreamSegmentSealedException(SEGMENT_NAME)));
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected flush() to have failed for a sealed attribute index (but unsealed segment).",
                () -> context.aggregator.flush(TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);
        context.segmentMetadata.markSealed();
        Assert.assertTrue("Unexpected result from mustFlush() after failed attempt (seal).", context.aggregator.mustFlush());

        val resultSeal = context.aggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Unexpected number of attributes flushed after seal.",
                op1.getAttributeUpdates().size() - 1, resultSeal.getFlushedAttributes()); // Subtract 1 for core attributes.
        Assert.assertFalse("Unexpected result from mustFlush() after successful attempt (seal).", context.aggregator.mustFlush());
        checkAutoAttributes(Operation.NO_SEQUENCE_NUMBER, context); // Segment is sealed, so we can't change this value

        // Segment is reported to be be deleted.
        val op2 = generateUpdateAttributesAndUpdateMetadata(config.getFlushAttributesThreshold(), context);
        context.aggregator.add(op2);
        context.dataSource.setPersistAttributesErrorInjector(new ErrorInjector<>(i -> true, () -> new StreamSegmentNotExistsException(SEGMENT_NAME)));
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected flush() to have failed for a deleted attribute index (but unsealed segment).",
                () -> context.aggregator.flush(TIMEOUT),
                ex -> ex instanceof StreamSegmentNotExistsException);
        Assert.assertTrue("Unexpected result from mustFlush() after failed attempt (deleted).", context.aggregator.mustFlush());

        context.segmentMetadata.markDeleted();
        Assert.assertFalse("Unexpected result from mustFlush() after segment deleted.", context.aggregator.mustFlush());
        val resultDelete = context.aggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Unexpected number of attributes flushed after delete.",
                0, resultDelete.getFlushedAttributes());
        checkAutoAttributes(Operation.NO_SEQUENCE_NUMBER, context); // Segment is deleted, so this shouldn't change.
    }

    /**
     * Tests the ability to seal the Attribute Index.
     */
    @Test
    public void testSeal() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int attributesPerUpdate = 1;
        final int updateCount = config.getFlushAttributesThreshold() - 1;

        @Cleanup
        TestContext context = new TestContext(config);
        val outstandingAttributes = new HashSet<AttributeId>();
        val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);

        // Add a few operations.
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
            context.aggregator.add(op);
        }

        Assert.assertFalse("Not expecting a flush yet.", context.aggregator.mustFlush());
        context.aggregator.add(generateSealAndUpdateMetadata(context));
        Assert.assertTrue("Expecting a flush after a seal operation.", context.aggregator.mustFlush());
        val flushResult = context.aggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Not all attributes were flushed.", outstandingAttributes.size(), flushResult.getFlushedAttributes());
        Assert.assertFalse("Not expecting a flush required after flushing everything.", context.aggregator.mustFlush());
        checkAttributes(context);
        checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected the attribute index to have been sealed.",
                () -> context.dataSource.persistAttributes(SEGMENT_ID, Collections.singletonMap(AttributeId.randomUUID(), 1L), TIMEOUT),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the ability to resume operations after a recovery.
     */
    @Test
    public void testRecovery() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int attributesPerUpdate = Math.max(1, config.getFlushAttributesThreshold() / 5);
        final int updateCount = config.getFlushAttributesThreshold() * 10;

        @Cleanup
        TestContext context = new TestContext(config);

        // Generate some data.
        val operations = new ArrayList<AttributeUpdaterOperation>();
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            operations.add(op);
        }

        // We perform a number of recovery iterations equal to the number of operations we have. At each iteration, we
        // include all operations with indices less than or equal to recoveryId and observe the results.
        for (int recoveryId = 0; recoveryId < operations.size(); recoveryId++) {
            long lastPersistedSeqNo = context.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Operation.NO_SEQUENCE_NUMBER);
            val outstandingAttributes = new HashSet<AttributeId>();
            val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            @Cleanup
            val aggregator = context.createAggregator();
            val expectedAttributes = new HashMap<AttributeId, Long>();
            for (int i = 0; i <= recoveryId; i++) {
                AttributeUpdaterOperation op = operations.get(i);

                // Collect the latest values from this update.
                op.getAttributeUpdates().stream()
                  .filter(au -> !Attributes.isCoreAttribute(au.getAttributeId()))
                  .forEach(au -> expectedAttributes.put(au.getAttributeId(), au.getValue()));

                aggregator.add(op);

                // We only expect to process an op if its SeqNo is beyond the last one we committed.
                boolean expectedToProcess = op.getSequenceNumber() > lastPersistedSeqNo;
                if (expectedToProcess) {
                    addExtendedAttributes(op, outstandingAttributes);
                    firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
                    lastOutstandingSeqNo.set(op.getSequenceNumber());
                }

                Assert.assertEquals("Unexpected LUSN.",
                        firstOutstandingSeqNo.get(), aggregator.getLowestUncommittedSequenceNumber());

                boolean expectFlush = outstandingAttributes.size() >= config.getFlushAttributesThreshold();
                Assert.assertEquals("Unexpected value returned by mustFlush() (count threshold).",
                        expectFlush, aggregator.mustFlush());

                if (expectFlush) {
                    // Call flush() and inspect the result.
                    WriterFlushResult flushResult = aggregator.flush(TIMEOUT).join();
                    Assert.assertEquals("Not all attributes were flushed (count threshold).",
                            outstandingAttributes.size(), flushResult.getFlushedAttributes());

                    // We want to verify just those attributes that we flushed, not all of them (not all may be in yet).
                    AssertExtensions.assertMapEquals("Unexpected attributes stored in AttributeIndex.",
                            expectedAttributes, context.dataSource.getPersistedAttributes(SEGMENT_ID));
                    checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
                    outstandingAttributes.clear();
                    firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
                    lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
                }
            }

            // We have reached the end. Flush the rest and perform a full check.
            if (recoveryId == operations.size() - 1) {
                aggregator.add(generateSealAndUpdateMetadata(context));
                aggregator.flush(TIMEOUT).join();
                checkAttributes(context);
                checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);
            }
        }
    }

    /**
     * Tests the ability to resume operations after a recovery on a sealed segment.
     */
    @Test
    public void testRecoverySealedSegment() throws Exception {
        final WriterConfig config = DEFAULT_CONFIG;
        final int attributesPerUpdate = 1;
        final int updateCount = config.getFlushAttributesThreshold() - 1;

        @Cleanup
        TestContext context = new TestContext(config);
        val outstandingAttributes = new HashSet<AttributeId>();
        val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);

        // Generate a few operations.
        val operations = new ArrayList<SegmentOperation>();
        for (int i = 0; i < updateCount; i++) {
            // Add another operation.
            AttributeUpdaterOperation op = i % 2 == 0
                    ? generateAppendAndUpdateMetadata(attributesPerUpdate, context)
                    : generateUpdateAttributesAndUpdateMetadata(attributesPerUpdate, context);
            operations.add(op);
            addExtendedAttributes(op, outstandingAttributes);
            firstOutstandingSeqNo.compareAndSet(Operation.NO_SEQUENCE_NUMBER, op.getSequenceNumber());
            lastOutstandingSeqNo.set(op.getSequenceNumber());
        }
        operations.add(generateSealAndUpdateMetadata(context));

        // Add them to the first aggregator, then flush them.
        for (val op : operations) {
            context.aggregator.add(op);
        }
        val flushResult = context.aggregator.flush(TIMEOUT).join();
        Assert.assertEquals("Not all attributes were flushed.", outstandingAttributes.size(), flushResult.getFlushedAttributes());
        checkAttributes(context);
        checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);

        // Create a second (recovered) aggregator and re-add the operations.
        @Cleanup
        val aggregator2 = context.createAggregator();
        for (val op : operations) {
            aggregator2.add(op);
        }

        Assert.assertEquals("Not expecting any operation outstanding in the second aggregator.", Operation.NO_SEQUENCE_NUMBER, aggregator2.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Expected a flush of the second aggregator.", aggregator2.mustFlush());
        val flushResult2 = aggregator2.flush(TIMEOUT).join();
        Assert.assertEquals("Not expecting any attributes to be flushed.", 0, flushResult2.getFlushedAttributes());
        checkAttributes(context);
        checkAutoAttributesEventual(lastOutstandingSeqNo.get(), context);

        // Create a third (recovered) aggregator, but clear out the auto-attributes.
        context.segmentMetadata.updateAttributes(Collections.singletonMap(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Attributes.NULL_ATTRIBUTE_VALUE));
        @Cleanup
        val aggregator3 = context.createAggregator();
        for (val op : operations) {
            aggregator3.add(op);
        }

        Assert.assertEquals("Unexpected LUSN for the third aggregator.", firstOutstandingSeqNo.get(), aggregator3.getLowestUncommittedSequenceNumber());
        Assert.assertTrue("Expected a flush of the third aggregator.", aggregator3.mustFlush());
        val flushResult3 = aggregator2.flush(TIMEOUT).join();
        Assert.assertEquals("Not expecting any attributes to be flushed.", 0, flushResult3.getFlushedAttributes());
        checkAttributes(context);
        checkAutoAttributesEventual(Operation.NO_SEQUENCE_NUMBER, context); // Segment is sealed, so it couldn't have updated this value.
    }

    private void addExtendedAttributes(AttributeUpdaterOperation op, Set<AttributeId> target) {
        op.getAttributeUpdates().stream()
          .map(AttributeUpdate::getAttributeId)
          .filter(id -> !Attributes.isCoreAttribute(id))
          .forEach(target::add);
    }

    private AttributeUpdaterOperation generateAppendAndUpdateMetadata(int attributeCount, TestContext context) {
        long offset = context.segmentMetadata.getLength();
        context.segmentMetadata.setLength(offset + APPEND_DATA.length);

        // Update some attributes.
        val updateOp = generateUpdateAttributesAndUpdateMetadata(attributeCount, context);
        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(context.segmentMetadata.getId(),
                new ByteArraySegment(APPEND_DATA), updateOp.getAttributeUpdates());
        op.setStreamSegmentOffset(offset);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        context.dataSource.recordAppend(op);
        return new CachedStreamSegmentAppendOperation(op);
    }

    private UpdateAttributesOperation generateUpdateAttributesAndUpdateMetadata(int attributeCount, TestContext context) {
        Assert.assertTrue(attributeCount <= EXTENDED_ATTRIBUTE_IDS.size());
        long coreAttributeValue = context.segmentMetadata.getAttributes().getOrDefault(CORE_ATTRIBUTE_ID, 0L) + 1;
        val attributeUpdates = new AttributeUpdateCollection();

        // Always add a Core Attribute - this should be ignored.
        attributeUpdates.add(new AttributeUpdate(CORE_ATTRIBUTE_ID, AttributeUpdateType.Accumulate, coreAttributeValue));
        val usedIndices = new HashSet<Integer>();
        for (int i = 0; i < attributeCount; i++) {
            int attributeIndex;
            do {
                attributeIndex = context.random.nextInt(EXTENDED_ATTRIBUTE_IDS.size());
            } while (usedIndices.contains(attributeIndex));
            usedIndices.add(attributeIndex);
            attributeUpdates.add(new AttributeUpdate(EXTENDED_ATTRIBUTE_IDS.get(attributeIndex), AttributeUpdateType.Replace, context.random.nextLong()));
        }
        context.segmentMetadata.updateAttributes(
                attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue)));
        UpdateAttributesOperation op = new UpdateAttributesOperation(context.segmentMetadata.getId(), attributeUpdates);
        op.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return op;
    }

    private StorageOperation generateSealAndUpdateMetadata(TestContext context) {
        context.segmentMetadata.markSealed();
        StreamSegmentSealOperation sealOp = new StreamSegmentSealOperation(context.segmentMetadata.getId());
        sealOp.setStreamSegmentOffset(context.segmentMetadata.getLength());
        sealOp.setSequenceNumber(context.containerMetadata.nextOperationSequenceNumber());
        return sealOp;
    }

    private AttributeUpdate createAttributeUpdate(AttributeId attributeId, long value) {
        return new AttributeUpdate(attributeId, AttributeUpdateType.Replace, value);
    }

    private WriterFlushResult forceTimeFlush(TestContext context) throws Exception {
        context.increaseTime(context.config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
        Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.aggregator.mustFlush());
        WriterFlushResult flushResult = context.aggregator.flush(TIMEOUT).get(SHORT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.aggregator.mustFlush());
        return flushResult;
    }

    private void checkAttributes(TestContext context) {
        val persistedAttributes = context.dataSource.getPersistedAttributes(context.segmentMetadata.getId());
        int extendedAttributeCount = 0;
        for (val e : context.segmentMetadata.getSnapshot().getAttributes().entrySet()) {
            if (Attributes.isCoreAttribute(e.getKey())) {
                Assert.assertFalse("Not expecting Core Attribute in Attribute Index for " + context.segmentMetadata.getId(),
                        persistedAttributes.containsKey(e.getKey()));
            } else {
                extendedAttributeCount++;
                Assert.assertEquals("Unexpected attribute value for " + e.getKey(),
                        e.getValue(), persistedAttributes.get(e.getKey()));
            }
        }

        Assert.assertEquals("Unexpected number of attributes in attribute index for " + context.segmentMetadata.getId(),
                extendedAttributeCount, persistedAttributes.size());
    }

    private void checkAutoAttributes(long lastSeqNo, TestContext context) {
        Assert.assertEquals("Unexpected value for ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO.",
                lastSeqNo, (long) context.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Operation.NO_SEQUENCE_NUMBER));
        Assert.assertEquals("Unexpected value for ATTRIBUTE_SEGMENT_ROOT_POINTER.",
                context.dataSource.getAttributeRootPointer(SEGMENT_ID),
                (long) context.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, Attributes.NULL_ATTRIBUTE_VALUE));
    }

    @SneakyThrows
    private void checkAutoAttributesEventual(long lastSeqNo, TestContext context) {
        assertEventuallyEquals(
                "Unexpected value for ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO.",
                lastSeqNo, () -> context.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_PERSIST_SEQ_NO, Operation.NO_SEQUENCE_NUMBER));

        assertEventuallyEquals("Unexpected value for ATTRIBUTE_SEGMENT_ROOT_POINTER.",
                context.dataSource.getAttributeRootPointer(SEGMENT_ID),
                () -> context.segmentMetadata.getAttributes().getOrDefault(Attributes.ATTRIBUTE_SEGMENT_ROOT_POINTER, Attributes.NULL_ATTRIBUTE_VALUE));
    }

    @SneakyThrows
    private <T> void assertEventuallyEquals(String message, T expected, Callable<T> valueGetter) {
        AssertExtensions.assertEventuallyEquals(message, expected, valueGetter, 10, TIMEOUT.toMillis());
    }

    private class TestContext implements AutoCloseable {
        final UpdateableContainerMetadata containerMetadata;
        final UpdateableSegmentMetadata segmentMetadata;
        final TestWriterDataSource dataSource;
        final ManualTimer timer;
        final AttributeAggregator aggregator;
        final WriterConfig config;
        final Random random;

        TestContext(WriterConfig config) {
            this.config = config;
            this.containerMetadata = new MetadataBuilder(CONTAINER_ID).build();
            this.timer = new ManualTimer();
            val dataSourceConfig = new TestWriterDataSource.DataSourceConfig();
            dataSourceConfig.autoInsertCheckpointFrequency = TestWriterDataSource.DataSourceConfig.NO_METADATA_CHECKPOINT;
            this.dataSource = new TestWriterDataSource(this.containerMetadata, executorService(), dataSourceConfig);
            this.segmentMetadata = initialize(this.containerMetadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID));
            this.aggregator = createAggregator();
            this.random = new Random(0);
        }

        void increaseTime(long deltaMillis) {
            this.timer.setElapsedMillis(this.timer.getElapsedMillis() + deltaMillis);
        }

        AttributeAggregator createAggregator() {
            return new AttributeAggregator(this.segmentMetadata, this.dataSource, this.config, this.timer, executorService());
        }

        @Override
        public void close() {
            this.aggregator.close();
            this.dataSource.close();
        }

        private UpdateableSegmentMetadata initialize(UpdateableSegmentMetadata segmentMetadata) {
            segmentMetadata.setStorageLength(0);
            segmentMetadata.setLength(0);
            return segmentMetadata;
        }
    }

    @RequiredArgsConstructor
    private static class RootPointerInterceptor {
        final long rootPointer;
        final long lastSeqNo;
        final CompletableFuture<Void> future = new CompletableFuture<>();
    }
}
