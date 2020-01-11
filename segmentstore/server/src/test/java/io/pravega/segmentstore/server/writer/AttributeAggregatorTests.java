/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeUpdate;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Cleanup;
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
    private static final UUID CORE_ATTRIBUTE_ID = Attributes.EVENT_COUNT;
    private static final List<UUID> EXTENDED_ATTRIBUTE_IDS = IntStream.range(0, 20).mapToObj(i -> UUID.randomUUID()).collect(Collectors.toList());
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
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
        final UUID extendedId = EXTENDED_ATTRIBUTE_IDS.get(0);
        @Cleanup
        TestContext context = new TestContext(DEFAULT_CONFIG);

        // We want to make sure we do not prematurely acknowledge anything.
        context.dataSource.setCompleteMergeCallback((target, source) -> Assert.fail("Not expecting any merger callbacks yet."));

        // Add non-Attributes.
        context.aggregator.add(new StreamSegmentTruncateOperation(SEGMENT_ID, 1234));

        // Add some attributes
        context.aggregator.add(new StreamSegmentAppendOperation(SEGMENT_ID, new ByteArraySegment(new byte[123]),
                Collections.singleton(createAttributeUpdate(extendedId, 1))));
        context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                Collections.singleton(createAttributeUpdate(extendedId, 2))));
        Assert.assertFalse("Unexpected value from mustFlush().", context.aggregator.mustFlush());

        // Seal using operation.
        context.aggregator.add(new StreamSegmentSealOperation(SEGMENT_ID));
        Assert.assertTrue("Unexpected value from mustFlush() after sealing.", context.aggregator.mustFlush());

        // Verify further adds fail
        AssertExtensions.assertThrows(
                "No operations should be allowed after sealing.",
                () -> context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                        Collections.singleton(createAttributeUpdate(extendedId, 3)))),
                ex -> ex instanceof DataCorruptionException);

        // Delete and verify nothing else changes.
        context.segmentMetadata.markDeleted();
        context.aggregator.add(new UpdateAttributesOperation(SEGMENT_ID,
                Collections.singleton(createAttributeUpdate(extendedId, 4))));
        Assert.assertFalse("Unexpected value from mustFlush() after deleting.", context.aggregator.mustFlush());
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
        val outstandingAttributes = new HashSet<UUID>();
        val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
        val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);

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
                Assert.assertEquals("Not all attributes were flushed (count threshold).",
                        outstandingAttributes.size(), flushResult.getFlushedAttributes());
                checkAttributes(context);
                checkAutoAttributes(lastOutstandingSeqNo.get(), context);
                outstandingAttributes.clear();
                firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
                lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
            } else {
                Assert.assertEquals(String.format("Not expecting a flush. OutstandingCount=%s, Threshold=%d",
                        outstandingAttributes.size(), config.getFlushThresholdBytes()),
                        0, flushResult.getFlushedBytes());
            }

            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (count threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());
            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (count threshold).", context.aggregator.mustFlush());
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
            context.increaseTime(config.getFlushThresholdTime().toMillis() + 1); // Force a flush by incrementing the time by a lot.
            Assert.assertTrue("Unexpected value returned by mustFlush() (time threshold).", context.aggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() before flush (time threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());

            WriterFlushResult flushResult = context.aggregator.flush(TIMEOUT).join();

            // We are always expecting a flush.
            Assert.assertEquals("Not all attributes were flushed (time threshold).", outstandingAttributes.size(), flushResult.getFlushedAttributes());
            checkAutoAttributes(lastOutstandingSeqNo.get(), context);
            outstandingAttributes.clear();
            firstOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);
            lastOutstandingSeqNo.set(Operation.NO_SEQUENCE_NUMBER);

            Assert.assertFalse("Unexpected value returned by mustFlush() after flush (time threshold).", context.aggregator.mustFlush());
            Assert.assertEquals("Unexpected value returned by getLowestUncommittedSequenceNumber() after flush (time threshold).",
                    firstOutstandingSeqNo.get(), context.aggregator.getLowestUncommittedSequenceNumber());
            Assert.assertEquals("Not expecting any merged bytes in this test.", 0, flushResult.getMergedBytes());
            checkAttributes(context);
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
        checkAutoAttributes(op.getSequenceNumber(), context);
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
        val outstandingAttributes = new HashSet<UUID>();
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
        checkAttributes(context);
        checkAutoAttributes(lastOutstandingSeqNo.get(), context);
        AssertExtensions.assertSuppliedFutureThrows(
                "Expected the attribute index to have been sealed.",
                () -> context.dataSource.persistAttributes(SEGMENT_ID, Collections.singletonMap(UUID.randomUUID(), 1L), TIMEOUT),
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
            val outstandingAttributes = new HashSet<UUID>();
            val firstOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            val lastOutstandingSeqNo = new AtomicLong(Operation.NO_SEQUENCE_NUMBER);
            @Cleanup
            val aggregator = context.createAggregator();
            val expectedAttributes = new HashMap<UUID, Long>();
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
                    checkAutoAttributes(lastOutstandingSeqNo.get(), context);
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
        val outstandingAttributes = new HashSet<UUID>();
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
        checkAutoAttributes(lastOutstandingSeqNo.get(), context);

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
        checkAutoAttributes(lastOutstandingSeqNo.get(), context);

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
        checkAutoAttributes(Operation.NO_SEQUENCE_NUMBER, context); // Segment is sealed, so it couldn't have updated this value.
    }

    private void addExtendedAttributes(AttributeUpdaterOperation op, Set<UUID> target) {
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
        val attributeUpdates = new ArrayList<AttributeUpdate>();

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

    private AttributeUpdate createAttributeUpdate(UUID attributeId, long value) {
        return new AttributeUpdate(attributeId, AttributeUpdateType.Replace, value);
    }

    private void checkAttributes(TestContext context) {
        val persistedAttributes = context.dataSource.getPersistedAttributes(context.segmentMetadata.getId());
        int extendedAttributeCount = 0;
        for (val e : context.segmentMetadata.getAttributes().entrySet()) {
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
}
