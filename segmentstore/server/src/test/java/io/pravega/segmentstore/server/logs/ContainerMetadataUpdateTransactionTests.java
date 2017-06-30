/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentMetadataComparer;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.TransactionMapOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the ContainerMetadataUpdateTransaction and SegmentMetadataUpdateTransaction classes.
 */
public class ContainerMetadataUpdateTransactionTests {
    private static final int CONTAINER_ID = 1234567;
    private static final String SEGMENT_NAME = "Segment_123";
    private static final long SEGMENT_ID = 123;
    private static final String SEALED_TRANSACTION_NAME = "Segment_123#Transaction_Sealed";
    private static final long SEALED_TRANSACTION_ID = 567;
    private static final String NOTSEALED_TRANSACTION_NAME = "Segment_123#Transaction_NotSealed";
    private static final long NOTSEALED_TRANSACTION_ID = 890;
    private static final long SEALED_TRANSACTION_LENGTH = 12;
    private static final long SEGMENT_LENGTH = 1234567;
    private static final byte[] DEFAULT_APPEND_DATA = "hello".getBytes();
    private static final AttributeUpdateType[] ATTRIBUTE_UPDATE_TYPES = new AttributeUpdateType[]{
            AttributeUpdateType.Replace, AttributeUpdateType.Accumulate};
    private static final Supplier<Long> NEXT_ATTRIBUTE_VALUE = System::nanoTime;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);
    private ManualTimer timeProvider;

    @Before
    public void before() {
        this.timeProvider = new ManualTimer();
    }

    //region StreamSegmentAppendOperation

    /**
     * Tests the preProcess method with StreamSegmentAppend operations.
     * Scenarios:
     * * Recovery Mode
     * * Non-recovery mode
     * * StreamSegment is Merged (both in-transaction and in-metadata)
     * * StreamSegment is Sealed (both in-transaction and in-metadata)
     */
    @Test
    public void testPreProcessStreamSegmentAppend() throws Exception {
        val metadata = createMetadata();
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When everything is OK (in recovery mode) - nothing should change.
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.preProcessOperation(appendOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentOffset after call to preProcess in recovery mode.",
                0, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state in recovery mode.",
                SEGMENT_LENGTH, txn1.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata in recovery mode.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                SEGMENT_LENGTH, txn2.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When StreamSegment is merged (via transaction).
        StreamSegmentAppendOperation transactionAppendOp = new StreamSegmentAppendOperation(SEALED_TRANSACTION_ID, DEFAULT_APPEND_DATA, null);
        MergeTransactionOperation mergeOp = createMerge();
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in transaction).",
                () -> txn2.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in metadata).",
                () -> txn2.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        StreamSegmentSealOperation sealOp = createSeal();
        txn2.preProcessOperation(sealOp);
        txn2.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in transaction).",
                () -> txn2.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in metadata).",
                () -> txn2.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentAppend operations.
     */
    @Test
    public void testAcceptStreamSegmentAppend() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(appendOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception.",
                SEGMENT_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When all is good.
        txn.preProcessOperation(appendOp);
        txn.acceptOperation(appendOp);
        Assert.assertEquals("acceptOperation did not update the transaction.",
                SEGMENT_LENGTH + appendOp.getData().length, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to process (and accept) StreamSegmentAppendOperations with
     * predefined offsets.
     */
    @Test
    public void testStreamSegmentAppendWithOffset() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Append #1 (at offset 0).
        long offset = metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength();
        StreamSegmentAppendOperation appendOp = createAppendWithOffset(offset);
        txn.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                offset, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                offset, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        txn.acceptOperation(appendOp);

        // Append #2 (after Append #1)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength();
        appendOp = createAppendWithOffset(offset);
        txn.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.",
                offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.",
                offset, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        txn.acceptOperation(appendOp);

        // Append #3 (wrong offset)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength() - 1;
        StreamSegmentAppendOperation badAppendOp = createAppendWithOffset(offset);
        AssertExtensions.assertThrows(
                "preProcessOperations accepted an append with the wrong offset.",
                () -> txn.preProcessOperation(badAppendOp),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertThrows(
                "acceptOperation accepted an append that was rejected during preProcessing.",
                () -> txn.acceptOperation(badAppendOp),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to handle StreamSegmentAppends with valid Attribute Updates.
     */
    @Test
    public void testStreamSegmentAppendWithAttributes() throws Exception {
        testWithAttributes(attributeUpdates -> new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, attributeUpdates));
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to reject StreamSegmentAppends with invalid Attribute Updates.
     */
    @Test
    public void testStreamSegmentAppendWithBadAttributes() throws Exception {
        testWithBadAttributes(attributeUpdates -> new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, attributeUpdates));
    }

    //endregion

    //region UpdateAttributesOperation

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to handle UpdateAttributeOperations with valid Attribute Updates.
     */
    @Test
    public void testUpdateAttributes() throws Exception {
        testWithAttributes(attributeUpdates -> new UpdateAttributesOperation(SEGMENT_ID, attributeUpdates));
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to reject UpdateAttributeOperations with invalid Attribute Updates.
     */
    @Test
    public void testUpdateAttributesWithBadValues() throws Exception {
        testWithBadAttributes(attributeUpdates -> new UpdateAttributesOperation(SEGMENT_ID, attributeUpdates));
    }

    private void testWithAttributes(Function<Collection<AttributeUpdate>, Operation> createOperation) throws Exception {
        final UUID attributeNoUpdate = UUID.randomUUID();
        final UUID attributeAccumulate = UUID.randomUUID();
        final UUID attributeReplace = UUID.randomUUID();
        final UUID attributeReplaceIfGreater = UUID.randomUUID();
        final UUID attributeReplaceIfEquals = UUID.randomUUID();

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Update #1.
        Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 1)); // Initial add, so it's ok.
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 1));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.Replace, 1)); // Need to initialize to something.
        val expectedValues = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));

        Operation op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // Verify that the AttributeUpdates still have the same values (there was nothing there prior) and that the updater
        // has internalized the attribute updates.
        verifyAttributeUpdates("after acceptOperation (1)", txn, attributeUpdates, expectedValues);

        // Update #2: update all attributes that can be updated.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1)); // 1 + 1 = 2
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 2, 1));
        expectedValues.put(attributeAccumulate, 2L);
        expectedValues.put(attributeReplace, 2L);
        expectedValues.put(attributeReplaceIfGreater, 2L);
        expectedValues.put(attributeReplaceIfEquals, 2L);

        op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // This is still in the transaction, so we need to add it for comparison sake.
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 1));
        verifyAttributeUpdates("after acceptOperation (2)", txn, attributeUpdates, expectedValues);

        // Update #3: after commit, verify that attributes are committed when they need to.
        val previousAcceptedValues = new HashMap<UUID, Long>(expectedValues);
        txn.commit(metadata);
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeAccumulate, AttributeUpdateType.Accumulate, 1)); // 2 + 1 = 3
        attributeUpdates.add(new AttributeUpdate(attributeReplace, AttributeUpdateType.Replace, 3));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 3));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 3, 2));
        expectedValues.put(attributeAccumulate, 3L);
        expectedValues.put(attributeReplace, 3L);
        expectedValues.put(attributeReplaceIfGreater, 3L);
        expectedValues.put(attributeReplaceIfEquals, 3L);

        op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after commit+acceptOperation, but prior to second commit.",
                previousAcceptedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
        verifyAttributeUpdates("after commit+acceptOperation", txn, attributeUpdates, expectedValues);

        // Final step: commit Append #3, and verify final segment metadata.
        txn.commit(metadata);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after final commit.",
                expectedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    private void testWithBadAttributes(Function<Collection<AttributeUpdate>, Operation> createOperation) throws Exception {
        final UUID attributeNoUpdate = UUID.randomUUID();
        final UUID attributeReplaceIfGreater = UUID.randomUUID();
        final UUID attributeReplaceIfEquals = UUID.randomUUID();

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Append #1.
        Collection<AttributeUpdate> attributeUpdates = new ArrayList<>();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 2)); // Initial add, so it's ok.
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.Replace, 2)); // Initial Add.
        val expectedValues = attributeUpdates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));

        Operation op = createOperation.apply(attributeUpdates);
        txn.preProcessOperation(op);
        txn.acceptOperation(op);

        // Append #2: Try to update attribute that cannot be updated.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 3));
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an unmodifiable attribute.",
                () -> txn.preProcessOperation(createOperation.apply(attributeUpdates)),
                ex -> ex instanceof BadAttributeUpdateException);

        // Append #3: Try to update attribute with bad value for ReplaceIfGreater attribute.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 1));
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an attribute with the wrong value for ReplaceIfGreater.",
                () -> txn.preProcessOperation(createOperation.apply(attributeUpdates)),
                ex -> ex instanceof BadAttributeUpdateException);

        // Append #4: Try to update attribute with bad value for ReplaceIfEquals attribute.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfEquals, 3, 3));
        AssertExtensions.assertThrows(
                "preProcessOperation accepted an operation that was trying to update an attribute with the wrong comparison value for ReplaceIfGreater.",
                () -> txn.preProcessOperation(createOperation.apply(attributeUpdates)),
                ex -> ex instanceof BadAttributeUpdateException);

        // Reset the attribute update list to its original state so we can do the final verification.
        attributeUpdates.clear();
        attributeUpdates.add(new AttributeUpdate(attributeNoUpdate, AttributeUpdateType.None, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfGreater, AttributeUpdateType.ReplaceIfGreater, 2));
        attributeUpdates.add(new AttributeUpdate(attributeReplaceIfEquals, AttributeUpdateType.ReplaceIfGreater, 2, 2));
        verifyAttributeUpdates("after rejected operations", txn, attributeUpdates, expectedValues);
        txn.commit(metadata);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in segment metadata after commit.",
                expectedValues, metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    //endregion

    //region StreamSegmentSealOperation

    /**
     * Tests the preProcess method with StreamSegmentSeal operations.
     * Scenarios:
     * * Recovery Mode
     * * Non-recovery mode
     * * StreamSegment is Merged (both in-transaction and in-metadata)
     * * StreamSegment is Sealed (both in-transaction and in-metadata)
     */
    @Test
    public void testPreProcessStreamSegmentSeal() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        StreamSegmentSealOperation sealOp = createSeal();

        // When everything is OK (in recovery mode) - nothing should change.
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.preProcessOperation(sealOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentLength after call to preProcess in recovery mode.",
                0, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state in recovery mode.",
                txn1.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata in recovery mode.",
                metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(sealOp);
        Assert.assertEquals("Unexpected StreamSegmentLength after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state.",
                txn2.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata.",
                metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When StreamSegment is merged (via transaction).
        StreamSegmentSealOperation transactionSealOp = new StreamSegmentSealOperation(SEALED_TRANSACTION_ID);
        MergeTransactionOperation mergeOp = createMerge();
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in transaction).",
                () -> txn2.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in metadata).",
                () -> txn2.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        txn2.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in transaction).",
                () -> txn2.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        txn2.commit(metadata);
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in metadata).",
                () -> txn2.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentSeal operations.
     */
    @Test
    public void testAcceptStreamSegmentSeal() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // Set some attributes.
        val segmentAttributes = createAttributes();
        segmentAttributes.put(Attributes.CREATION_TIME, 1L);
        UpdateableSegmentMetadata segmentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        segmentMetadata.updateAttributes(segmentAttributes);
        val txn = createUpdateTransaction(metadata);
        StreamSegmentSealOperation sealOp = createSeal();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(sealOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertFalse("acceptOperation updated the transaction even if it threw an exception.",
                txn.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When all is good.
        txn.preProcessOperation(sealOp);
        txn.acceptOperation(sealOp);
        Assert.assertTrue("acceptOperation did not update the transaction.", txn.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", segmentMetadata.isSealed());

        txn.commit(metadata);

        // Check attributes.
        segmentAttributes.keySet().removeIf(Attributes::isDynamic); // All dynamic attributes should be removed.
        SegmentMetadataComparer.assertSameAttributes("Unexpected set of attributes after commit.", segmentAttributes, segmentMetadata);
    }

    //endregion

    //region MergeTransactionOperation

    /**
     * Tests the preProcess method with MergeTransactionOperations.
     * Scenarios:
     * * Recovery/non-recovery mode
     * * Target StreamSegment is sealed
     * * Target StreamSegment is a Transaction
     * * Transaction StreamSegment is already merged
     * * Transaction StreamSegment is not sealed
     */
    @Test
    public void testPreProcessMergeTransaction() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // When everything is OK (recovery mode).
        MergeTransactionOperation recoveryMergeOp = createMerge();
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        AssertExtensions.assertThrows(
                "preProcess(Merge) handled an operation with no Transaction StreamSegment Length set.",
                () -> txn1.preProcessOperation(createMerge()),
                ex -> ex instanceof MetadataUpdateException);

        // In recovery mode, the updater does not set the length; it just validates that it has one.
        recoveryMergeOp.setLength(metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).getDurableLogLength());
        txn1.preProcessOperation(recoveryMergeOp);
        AssertExtensions.assertLessThan("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.",
                0, recoveryMergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(recoveryMergeOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.",
                txn1.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.",
                metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());

        // When everything is OK (non-recovery mode).
        MergeTransactionOperation mergeOp = createMerge();
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(mergeOp);
        Assert.assertEquals("Unexpected Transaction StreamSegmentLength after call to preProcess in non-recovery mode.",
                SEALED_TRANSACTION_LENGTH, mergeOp.getLength());
        Assert.assertEquals("Unexpected Target StreamSegmentOffset after call to preProcess in non-recovery mode.",
                SEGMENT_LENGTH, mergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(mergeOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in non-recovery mode.",
                txn2.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in non-recovery mode.",
                metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());

        // When Target StreamSegment is sealed.
        StreamSegmentSealOperation sealTargetOp = createSeal();
        txn2.preProcessOperation(sealTargetOp);
        txn2.acceptOperation(sealTargetOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is sealed.",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentSealedException);

        txn2.clear(); // Rollback the seal

        // When Target StreamSegment is a Transaction.
        MergeTransactionOperation mergeToTransactionOp = new MergeTransactionOperation(NOTSEALED_TRANSACTION_ID, SEALED_TRANSACTION_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is a Transaction.",
                () -> txn2.preProcessOperation(mergeToTransactionOp),
                ex -> ex instanceof MetadataUpdateException);

        // When Transaction is not sealed.
        MergeTransactionOperation mergeNonSealed = new MergeTransactionOperation(NOTSEALED_TRANSACTION_ID, SEGMENT_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is not sealed.",
                () -> txn2.preProcessOperation(mergeNonSealed),
                ex -> ex instanceof MetadataUpdateException);

        // When Transaction is already merged.
        txn2.preProcessOperation(mergeOp);
        txn2.acceptOperation(mergeOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in transaction).",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);

        txn2.commit(metadata);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in metadata).",
                () -> txn2.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);
    }

    /**
     * Tests the accept method with MergeTransactionOperations.
     */
    @Test
    public void testAcceptMergeTransaction() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        MergeTransactionOperation mergeOp = createMerge();

        // When no pre-process has happened
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> txn.acceptOperation(mergeOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception (parent segment).",
                SEGMENT_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata (parent segment).",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        txn.clear(); // This would naturally happen in case of a failure, so we need to simulate this here too.

        // When all is good.
        txn.preProcessOperation(mergeOp);
        txn.acceptOperation(mergeOp);
        Assert.assertTrue("acceptOperation did not update the transaction(Transaction).",
                txn.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("acceptOperation updated the metadata (Transaction).",
                metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertEquals("acceptOperation did not update the transaction.",
                SEGMENT_LENGTH + SEALED_TRANSACTION_LENGTH, txn.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.",
                SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
    }

    //endregion

    //region MetadataOperations

    /**
     * Tests the preProcessOperation and acceptOperation methods with StreamSegmentMap operations.
     */
    @Test
    public void testProcessStreamSegmentMap() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();

        // Part 1: recovery mode (no-op).
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);

        // Brand new StreamSegment.
        StreamSegmentMapOperation mapOp = createMap();
        txn1.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation did modify the StreamSegmentId on the operation in recovery mode.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        txn2.acceptOperation(mapOp);

        val updaterMetadata = txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to acceptOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getDurableLogLength());
        Assert.assertEquals("Unexpected value for isSealed after call to acceptOperation (in transaction).",
                mapOp.isSealed(), updaterMetadata.isSealed());
        Assert.assertNull("acceptOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // StreamSegmentName already exists (transaction) and we try to map with new id.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in transaction).",
                () -> txn2.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        txn2.commit(metadata);

        val segmentMetadata = metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        AssertExtensions.assertMapEquals("Unexpected attributes in SegmentMetadata after call to commit().",
                mapOp.getAttributes(), segmentMetadata.getAttributes());

        // StreamSegmentName already exists (metadata) and we try to map with new id.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in metadata).",
                () -> txn2.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        val durableLogLength = segmentMetadata.getDurableLogLength() + 5;
        val storageLength = segmentMetadata.getStorageLength() + 1;
        segmentMetadata.setDurableLogLength(durableLogLength);

        // StreamSegmentName already exists and we try to map with the same id. Verify that we are able to update its
        // StorageLength (if different).
        val updateMap = new StreamSegmentMapOperation(new StreamSegmentInformation(mapOp.getStreamSegmentName(),
                storageLength, true, false, createAttributes(), new ImmutableDate()));
        updateMap.setStreamSegmentId(mapOp.getStreamSegmentId());
        txn2.preProcessOperation(updateMap);
        txn2.acceptOperation(updateMap);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (in transaction).",
                storageLength, txn2.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        txn2.commit(metadata);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (post-commit).",
                storageLength, metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to acceptOperation with remap (post-commit).",
                durableLogLength, metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getDurableLogLength());
    }

    /**
     * Tests the processOperation and acceptOperation methods with TransactionMap operations.
     */
    @Test
    public void testPreProcessTransactionMap() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        val txn1 = createUpdateTransaction(metadata);

        // Parent does not exist.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a Transaction StreamSegment to an inexistent parent.",
                () -> txn1.preProcessOperation(createTransactionMap(12345)),
                ex -> ex instanceof MetadataUpdateException);

        // Brand new Transaction (and parent).
        StreamSegmentMapOperation mapParent = createMap();
        txn1.preProcessOperation(mapParent); // Create parent.
        txn1.acceptOperation(mapParent);
        txn1.commit(metadata);

        // Part 1: recovery mode.
        metadata.enterRecoveryMode();
        TransactionMapOperation mapOp = createTransactionMap(mapParent.getStreamSegmentId());
        val txn2 = createUpdateTransaction(metadata);
        txn2.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation changed the StreamSegmentId on the operation in recovery mode.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        val txn3 = createUpdateTransaction(metadata);
        txn3.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.",
                ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", txn3.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        txn3.acceptOperation(mapOp);
        val updaterMetadata = txn3.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        Assert.assertEquals("Unexpected StorageLength after call to processMetadataOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to processMetadataOperation (in transaction).",
                mapOp.getLength(), updaterMetadata.getDurableLogLength());
        Assert.assertEquals("Unexpected value for isSealed after call to processMetadataOperation (in transaction).",
                mapOp.isSealed(), updaterMetadata.isSealed());
        Assert.assertNull("processMetadataOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // Transaction StreamSegmentName exists (transaction).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a TransactionStreamSegment with the same Name already exists (in transaction).",
                () -> txn3.preProcessOperation(createTransactionMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        txn3.commit(metadata);

        val segmentMetadata = metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId());
        AssertExtensions.assertMapEquals("Unexpected attributes in SegmentMetadata after call to commit().",
                mapOp.getAttributes(), segmentMetadata.getAttributes());

        // Transaction StreamSegmentName exists (metadata).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a TransactionStreamSegment with the same Name already exists (in metadata).",
                () -> txn3.preProcessOperation(createTransactionMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // StreamSegmentName already exists and we try to map with the same id. Verify that we are able to update its
        // StorageLength (if different).
        val updateMap = new TransactionMapOperation(mapOp.getParentStreamSegmentId(),
                new StreamSegmentInformation(mapOp.getStreamSegmentName(), mapOp.getLength() + 1, true, false, createAttributes(), new ImmutableDate()));
        updateMap.setStreamSegmentId(mapOp.getStreamSegmentId());
        txn3.preProcessOperation(updateMap);
        txn3.acceptOperation(updateMap);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (in transaction).",
                updateMap.getLength(), txn3.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getLength());
        txn3.commit(metadata);
        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation with remap (post-commit).",
                updateMap.getLength(), metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getLength());
    }

    /**
     * Tests the ability to reject new StreamSegment/Transaction map operations that would exceed the max allowed counts.
     */
    @Test
    public void testSegmentMapMax() throws Exception {
        UpdateableContainerMetadata metadata = new MetadataBuilder(CONTAINER_ID)
                .withMaxActiveSegmentCount(3).build();
        metadata.mapStreamSegmentId("a", SEGMENT_ID);
        metadata.mapStreamSegmentId("a_txn1", 123457, SEGMENT_ID);

        // Non-recovery mode.
        val txn1 = createUpdateTransaction(metadata);

        // Map one segment, which should fill up the quota.
        StreamSegmentMapOperation acceptedMap = createMap();
        txn1.preProcessOperation(acceptedMap);
        txn1.acceptOperation(acceptedMap);

        // Verify non-recovery mode.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a StreamSegment that would exceed the active segment quota.",
                () -> txn1.preProcessOperation(createMap("foo")),
                ex -> ex instanceof TooManyActiveSegmentsException);

        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a StreamSegment that would exceed the active segment quota.",
                () -> txn1.preProcessOperation(createTransactionMap(SEGMENT_ID, "foo")),
                ex -> ex instanceof TooManyActiveSegmentsException);

        // Verify recovery mode.
        metadata.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        //updater.setOperationSequenceNumber(10000);
        StreamSegmentMapOperation secondMap = createMap("c");
        secondMap.setStreamSegmentId(1234);
        txn2.preProcessOperation(secondMap);
        txn2.acceptOperation(secondMap);

        TransactionMapOperation secondTxnMap = createTransactionMap(SEGMENT_ID, "a_txn2");
        secondTxnMap.setStreamSegmentId(1235);
        txn2.preProcessOperation(secondTxnMap);
        txn2.acceptOperation(secondTxnMap);
        txn2.commit(metadata);
        metadata.exitRecoveryMode();

        Assert.assertNotNull("Updater did not create metadata for new segment in recovery mode even if quota is exceeded.",
                metadata.getStreamSegmentMetadata(secondMap.getStreamSegmentId()));
        Assert.assertNotNull("Updater did not create metadata for new transaction in recovery mode even if quota is exceeded.",
                metadata.getStreamSegmentMetadata(secondTxnMap.getStreamSegmentId()));
    }

    /**
     * Tests the processMetadataOperation method with MetadataCheckpoint operations.
     */
    @Test
    public void testProcessMetadataCheckpoint() throws Exception {
        // When encountering MetadataCheckpoint in non-Recovery Mode, the ContainerMetadataUpdateTransaction serializes a snapshot
        // of the current metadata inside the Operation.
        // When encountering MetadataCheckpoint in Recovery Mode, the ContainerMetadataUpdateTransaction deserializes the snapshot-ted
        // metadata in it and applies it to the container metadata (inside the transaction). All existing metadata updates
        // are cleared.
        String newSegmentName = "NewSegmentId";
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata.
        this.timeProvider.setElapsedMillis(1234);
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);

        // Checkpoint 1: original metadata.
        // Checkpoint 2: Checkpoint 1 + 1 StreamSegment and 1 Transaction + 1 Append
        MetadataCheckpointOperation checkpoint1 = createMetadataCheckpoint();
        MetadataCheckpointOperation checkpoint2 = createMetadataCheckpoint();

        // Checkpoint 1 Should have original metadata.
        processOperation(checkpoint1, txn, seqNo::incrementAndGet);
        UpdateableContainerMetadata checkpointedMetadata = getCheckpointedMetadata(checkpoint1);
        assertMetadataSame("Unexpected metadata before any operation.", metadata, checkpointedMetadata);

        // Map another StreamSegment, and add an append
        StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(
                new StreamSegmentInformation(newSegmentName, SEGMENT_LENGTH, false, false, new ImmutableDate()));
        processOperation(mapOp, txn, seqNo::incrementAndGet);
        processOperation(new StreamSegmentAppendOperation(mapOp.getStreamSegmentId(), DEFAULT_APPEND_DATA, createAttributeUpdates()), txn, seqNo::incrementAndGet);
        processOperation(checkpoint2, txn, seqNo::incrementAndGet);

        // Checkpoint 2 should have Checkpoint 1 + New StreamSegment + Append.
        txn.commit(metadata);
        checkpointedMetadata = getCheckpointedMetadata(checkpoint2);
        assertMetadataSame("Unexpected metadata after deserializing checkpoint.", metadata, checkpointedMetadata);
    }

    /**
     * Tests the processMetadataOperation method with StorageMetadataCheckpoint operations.
     */
    @Test
    public void testProcessStorageMetadataCheckpoint() throws Exception {
        // When encountering StorageMetadataCheckpoint in non-Recovery Mode, the ContainerMetadataUpdateTransaction serializes a
        // snapshot of the relevant metadata fields inside the Operation.
        // When encountering StorageMetadataCheckpoint in Recovery Mode, the ContainerMetadataUpdateTransaction deserializes the
        // snapshot-ted metadata in it and applies it to the container metadata (inside the transaction).
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata and seal a segment.
        this.timeProvider.setElapsedMillis(1234);
        val metadata1 = createMetadata();
        val txn1 = createUpdateTransaction(metadata1);
        val segmentMetadata1 = metadata1.getStreamSegmentMetadata(SEGMENT_ID);
        segmentMetadata1.markSealed(); // Need to mark segment sealed before being able to set sealed in storage.

        // Take a full snapshot of the metadata before making any changes.
        val fullCheckpoint1 = createMetadataCheckpoint();
        processOperation(fullCheckpoint1, txn1, seqNo::incrementAndGet);

        // Update the storage state for this segment: increment its length and mark it as sealed in storage.
        segmentMetadata1.setStorageLength(segmentMetadata1.getStorageLength() + 1);
        segmentMetadata1.markSealedInStorage();

        // Take a storage checkpoint.
        val storageCheckpoint = createStorageMetadataCheckpoint();
        processOperation(storageCheckpoint, txn1, seqNo::incrementAndGet);

        // Apply current metadata.
        txn1.commit(metadata1);

        // Create a new metadata and apply the checkpoint during recovery.
        val metadata2 = createBlankMetadata();
        metadata2.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata2);
        processOperation(fullCheckpoint1, txn2, () -> 1L);
        txn2.preProcessOperation(storageCheckpoint);
        txn2.commit(metadata2);
        metadata2.exitRecoveryMode();

        // Verify that the Storage Length & StorageSealed status are applied correctly.
        assertMetadataSame("Unexpected metadata after applying storage checkpoint.", metadata1, metadata2);
    }

    /**
     * Tests the processMetadataOperation method with MetadataCheckpoint operations, when such checkpoints are skipped over
     * because they are after other operations.
     */
    @Test
    public void testProcessMetadataCheckpointIgnored() throws Exception {
        long newSegmentId = 897658;
        String newSegmentName = "NewSegmentId";
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata.
        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        MetadataCheckpointOperation checkpointedMetadata = createMetadataCheckpoint();
        processOperation(checkpointedMetadata, txn, seqNo::incrementAndGet);

        // Create a blank metadata, and add an operation to the updater (which would result in mapping a new StreamSegment).
        metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn2 = createUpdateTransaction(metadata);
        StreamSegmentMapOperation mapOp = createMap(newSegmentName);
        mapOp.setStreamSegmentId(newSegmentId);
        processOperation(mapOp, txn2, seqNo::incrementAndGet);

        // Now try to process the checkpoint
        processOperation(checkpointedMetadata, txn2, seqNo::incrementAndGet);
        txn2.commit(metadata);

        // Verify the checkpointed metadata hasn't been applied
        Assert.assertNull("Newly added StreamSegment Id was not removed after applying checkpoint.",
                metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNotNull("Checkpoint seems to have not been applied.", metadata.getStreamSegmentMetadata(SEGMENT_ID));
    }

    //endregion

    //region Other tests

    /**
     * Tests the behavior of preProcessOperation and acceptOperation when encountering an invalid StreamSegmentId, or
     * when encountering a StreamSegment Id for a deleted StreamSegment.
     */
    @Test
    public void testPreProcessAndAcceptWithInvalidSegmentId() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        val txn = createUpdateTransaction(metadata);

        ArrayList<StorageOperation> testOperations = new ArrayList<>();
        testOperations.add(createAppendNoOffset());
        testOperations.add(createSeal());
        testOperations.add(createMerge());

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for a non-existent Segment: " + op,
                    () -> txn.preProcessOperation(op),
                    ex -> ex instanceof MetadataUpdateException);

            AssertExtensions.assertThrows(
                    "Unexpected behavior from acceptOperation when processing an operation for a non-existent Segment: " + op,
                    () -> txn.acceptOperation(op),
                    ex -> ex instanceof MetadataUpdateException);
        }

        // If the StreamSegment was previously marked as deleted.
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId("foo", SEGMENT_ID);
        segmentMetadata.markDeleted();

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for deleted Segment: " + op,
                    () -> txn.preProcessOperation(op),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to commit all outstanding changes to the same base
     * metadata.
     */
    @Test
    public void testCommitSameTarget() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        testCommit(metadata, metadata);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to commit all outstanding changes to a new target.
     */
    @Test
    public void testCommitNewTarget() throws Exception {
        val referenceMetadata = createMetadata(); // We don't touch this.
        // This just simulates what we're doing in testCommit().
        referenceMetadata.getStreamSegmentMetadata(SEGMENT_ID).updateAttributes(Collections.singletonMap(Attributes.CREATION_TIME, 0L));
        val baseMetadata = createMetadata();
        val newMetadata = createMetadata();
        testCommit(baseMetadata, newMetadata);
        Assert.assertEquals("SequenceNumber changed on base metadata.",
                referenceMetadata.getOperationSequenceNumber(), baseMetadata.getOperationSequenceNumber());
        AssertExtensions.assertContainsSameElements("SegmentIds changed on base metadata",
                referenceMetadata.getAllStreamSegmentIds(), baseMetadata.getAllStreamSegmentIds());
        for (long segmentId : referenceMetadata.getAllStreamSegmentIds()) {
            val referenceSegmentMetadata = referenceMetadata.getStreamSegmentMetadata(segmentId);
            val baseSegmentMetadata = baseMetadata.getStreamSegmentMetadata(segmentId);
            SegmentMetadataComparer.assertEquals("SegmentMetadata changed on base metadata",
                    referenceSegmentMetadata, baseSegmentMetadata);
        }
    }

    /**
     * Tests the rebase() method, albeit in a bit unorthodox manner, since this is not how rebase will really be used -
     * we just want to make sure it actually rebases the UpdateTransaction.
     */
    @Test
    public void testRebase() throws Exception {
        val baseMetadata = createMetadata();
        val newMetadata1 = createMetadata();
        testCommit(baseMetadata, newMetadata1);

        val txn = createUpdateTransaction(baseMetadata);
        txn.rebase(newMetadata1);

        AssertExtensions.assertContainsSameElements("Unexpected SegmentIds after rebase",
                newMetadata1.getAllStreamSegmentIds(), txn.getAllStreamSegmentIds());

        for (long segmentId : newMetadata1.getAllStreamSegmentIds()) {
            val referenceSegmentMetadata = newMetadata1.getStreamSegmentMetadata(segmentId);
            val baseSegmentMetadata = txn.getStreamSegmentMetadata(segmentId);
            SegmentMetadataComparer.assertEquals("Unexpected SegmentMetadata after rebase",
                    referenceSegmentMetadata, baseSegmentMetadata);
        }
    }

    private void testCommit(UpdateableContainerMetadata baseMetadata, UpdateableContainerMetadata targetMetadata) throws Exception {
        // Create a few appends, merge a Transaction and seal the parent stream. Verify all changes have been applied after
        // a call to commit().
        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppendNoOffset());
        }

        operations.add(createMerge());
        operations.add(createSeal());

        baseMetadata.getStreamSegmentMetadata(SEGMENT_ID).updateAttributes(Collections.singletonMap(Attributes.CREATION_TIME, 0L));
        val txn = createUpdateTransaction(baseMetadata);
        long expectedLastUsedParent = -1;
        long expectedLastUsedTransaction = -1;
        long seqNo = 0;
        for (StorageOperation op : operations) {
            txn.preProcessOperation(op);
            op.setSequenceNumber(++seqNo);
            txn.acceptOperation(op);
            if (op.getStreamSegmentId() == SEGMENT_ID) {
                expectedLastUsedParent = op.getSequenceNumber();
            }

            if (op instanceof MergeTransactionOperation) {
                expectedLastUsedParent = op.getSequenceNumber();
                expectedLastUsedTransaction = op.getSequenceNumber();
            }
        }

        txn.commit(targetMetadata);
        Assert.assertEquals("commit() seems to have modified the metadata sequence number while not in recovery mode.",
                ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER, targetMetadata.nextOperationSequenceNumber() - 1);

        long expectedLength = SEGMENT_LENGTH + appendCount * DEFAULT_APPEND_DATA.length + SEALED_TRANSACTION_LENGTH;

        SegmentMetadata parentMetadata = targetMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in metadata after commit.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertTrue("Unexpected value for isSealed in metadata after commit.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Parent after commit.", expectedLastUsedParent, parentMetadata);

        SegmentMetadata transactionMetadata = targetMetadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertTrue("Unexpected value for isSealed in Transaction metadata after commit.", transactionMetadata.isSealed());
        Assert.assertTrue("Unexpected value for isMerged in Transaction metadata after commit.", transactionMetadata.isMerged());
        Assert.assertEquals("Unexpected number of attributes for parent segment.", 1, parentMetadata.getAttributes().size());
        Assert.assertEquals("Unexpected number of attributes for transaction.", 0, transactionMetadata.getAttributes().size());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Transaction after commit.", expectedLastUsedTransaction, transactionMetadata);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to set the Sequence Number on the Metadata, when in Recovery Mode.
     */
    @Test
    public void testCommitSequenceNumber() throws Exception {
        final long newSeqNo = 1235;
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn1 = createUpdateTransaction(metadata);
        txn1.setOperationSequenceNumber(newSeqNo);
        txn1.commit(metadata);
        metadata.exitRecoveryMode();
        Assert.assertEquals("commit() did not set the metadata sequence number while in recovery mode.", newSeqNo + 1, metadata.nextOperationSequenceNumber());

        // Now try again in non-recovery mode.
        val txn2 = createUpdateTransaction(metadata);
        AssertExtensions.assertThrows(
                "setOperationSequence number should not work in non-recovery mode.",
                () -> txn2.setOperationSequenceNumber(newSeqNo + 10),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability of the ContainerMetadataUpdateTransaction to rollback all outstanding changes.
     */
    @Test
    public void testRollback() throws Exception {
        // Create a couple of operations, commit them, and then create a few more appends, merge a Transaction and seal the parent stream.
        // Then call rollback(); verify no changes have been applied after the call to commit().

        UpdateableContainerMetadata metadata = createMetadata();
        val txn = createUpdateTransaction(metadata);
        StreamSegmentAppendOperation committedAppend = createAppendNoOffset();
        txn.preProcessOperation(committedAppend);
        txn.acceptOperation(committedAppend);
        txn.commit(metadata); // This is the last extent of the modifications to the metadata.

        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppendNoOffset());
        }

        operations.add(createMerge());
        operations.add(createSeal());

        long seqNo = 0;
        for (StorageOperation op : operations) {
            txn.preProcessOperation(op);
            op.setSequenceNumber(++seqNo);
            txn.acceptOperation(op);
        }

        txn.clear();

        long expectedLength = SEGMENT_LENGTH + DEFAULT_APPEND_DATA.length;

        // Verify metadata is untouched and that the updater has truly rolled back.
        SegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in metadata after rollback.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertFalse("Unexpected value for isSealed in metadata after rollback.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Parent after rollback.", 0, parentMetadata);

        SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertFalse("Unexpected value for isMerged in transaction segment metadata after rollback.", transactionMetadata.isMerged());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Transaction segment after rollback.", 0, transactionMetadata);

        // Now the updater
        parentMetadata = txn.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in transaction after rollback.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertFalse("Unexpected value for isSealed in transaction after rollback.", parentMetadata.isSealed());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Parent (txn) after rollback.", 0, parentMetadata);

        transactionMetadata = txn.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertFalse("Unexpected value for isMerged in transaction segment in update transaction after rollback.", transactionMetadata.isMerged());
        checkLastKnownSequenceNumber("Unexpected lastUsed for Transaction segment in update transaction after rollback.", 0, transactionMetadata);
    }

    /**
     * Tests the correctness of getActiveSegmentCount(), especially in the case when the real ContainerMetadata has
     * changed its value.
     */
    @Test
    public void testGetActiveSegmentCount() throws Exception {
        // Create a base metadata and make sure it has one active segment.
        UpdateableContainerMetadata metadata = createMetadata();
        int expected = 3;
        Assert.assertEquals("Unexpected initial Active Segment Count for base metadata.",
                expected, metadata.getActiveSegmentCount());

        // Create an UpdateTransaction for it and map a new segment.
        val txn1 = new ContainerMetadataUpdateTransaction(metadata, metadata, 0);
        Assert.assertEquals("Unexpected Active Segment Count for first transaction.",
                expected, txn1.getActiveSegmentCount());
        val map1 = createMap("NewSegment1");
        txn1.preProcessOperation(map1);
        map1.setSequenceNumber(metadata.nextOperationSequenceNumber());
        txn1.acceptOperation(map1);
        expected++;
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after map.",
                expected, txn1.getActiveSegmentCount());

        // Create a second UpdateTransaction for it and map a new segment.
        val txn2 = new ContainerMetadataUpdateTransaction(txn1, metadata, 0);

        Assert.assertEquals("Unexpected Active Segment Count for second transaction.",
                expected, txn2.getActiveSegmentCount());
        val map2 = createMap("NewSegment2");
        txn2.preProcessOperation(map2);
        map2.setSequenceNumber(metadata.nextOperationSequenceNumber());
        txn2.acceptOperation(map2);
        expected++;
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after map.",
                expected, txn2.getActiveSegmentCount());

        // Clean up base metadata - simplest way to deactivate segments (even if this will cause a TxnCommit to fail,
        // but we don't test that here).
        metadata.enterRecoveryMode();
        metadata.reset();
        metadata.exitRecoveryMode();
        expected = 0;

        // Check Active Segment Count after the cleanup.
        Assert.assertEquals("Unexpected Active Segment Count for base metadata after cleanup.",
                expected, metadata.getActiveSegmentCount());
        Assert.assertEquals("Unexpected Active Segment Count for first transaction after cleanup.",
                expected + 1, txn1.getActiveSegmentCount());
        Assert.assertEquals("Unexpected Active Segment Count for second transaction after cleanup.",
                expected + 2, txn2.getActiveSegmentCount());
    }

    //endregion

    //region Helpers

    private UpdateableContainerMetadata createBlankMetadata() {
        return new MetadataBuilder(CONTAINER_ID).build();
    }

    private UpdateableContainerMetadata createMetadata() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        segmentMetadata.setDurableLogLength(SEGMENT_LENGTH);
        segmentMetadata.setStorageLength(SEGMENT_LENGTH - 1); // Different from DurableLogOffset.

        segmentMetadata = metadata.mapStreamSegmentId(SEALED_TRANSACTION_NAME, SEALED_TRANSACTION_ID, SEGMENT_ID);
        segmentMetadata.setDurableLogLength(SEALED_TRANSACTION_LENGTH);
        segmentMetadata.setStorageLength(SEALED_TRANSACTION_LENGTH);
        segmentMetadata.markSealed();

        segmentMetadata = metadata.mapStreamSegmentId(NOTSEALED_TRANSACTION_NAME, NOTSEALED_TRANSACTION_ID, SEGMENT_ID);
        segmentMetadata.setDurableLogLength(0);
        segmentMetadata.setStorageLength(0);

        return metadata;
    }

    private ContainerMetadataUpdateTransaction createUpdateTransaction(ContainerMetadata metadata) {
        return new ContainerMetadataUpdateTransaction(metadata, metadata, 0);
    }

    private StreamSegmentAppendOperation createAppendNoOffset() {
        return new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, createAttributeUpdates());
    }

    private StreamSegmentAppendOperation createAppendWithOffset(long offset) {
        return new StreamSegmentAppendOperation(SEGMENT_ID, offset, DEFAULT_APPEND_DATA, createAttributeUpdates());
    }

    private Collection<AttributeUpdate> createAttributeUpdates() {
        return Arrays.stream(ATTRIBUTE_UPDATE_TYPES)
                     .map(ut -> new AttributeUpdate(UUID.randomUUID(), ut, NEXT_ATTRIBUTE_VALUE.get()))
                     .collect(Collectors.toList());
    }

    private Map<UUID, Long> createAttributes() {
        return Arrays.stream(ATTRIBUTE_UPDATE_TYPES)
                     .collect(Collectors.toMap(a -> UUID.randomUUID(), a -> NEXT_ATTRIBUTE_VALUE.get()));
    }

    private StreamSegmentSealOperation createSeal() {
        return new StreamSegmentSealOperation(SEGMENT_ID);
    }

    private MergeTransactionOperation createMerge() {
        return new MergeTransactionOperation(SEGMENT_ID, SEALED_TRANSACTION_ID);
    }

    private StreamSegmentMapOperation createMap() {
        return createMap(SEGMENT_NAME);
    }

    private StreamSegmentMapOperation createMap(String name) {
        return new StreamSegmentMapOperation(new StreamSegmentInformation(name, SEGMENT_LENGTH, true, false, createAttributes(), new ImmutableDate()));
    }

    private TransactionMapOperation createTransactionMap(long parentId) {
        return createTransactionMap(parentId, SEALED_TRANSACTION_NAME);
    }

    private TransactionMapOperation createTransactionMap(long parentId, String name) {
        return new TransactionMapOperation(parentId, new StreamSegmentInformation(name, SEALED_TRANSACTION_LENGTH, true, false, createAttributes(), new ImmutableDate()));
    }

    private MetadataCheckpointOperation createMetadataCheckpoint() {
        return new MetadataCheckpointOperation();
    }

    private StorageMetadataCheckpointOperation createStorageMetadataCheckpoint() {
        return new StorageMetadataCheckpointOperation();
    }

    private void processOperation(Operation operation, ContainerMetadataUpdateTransaction txn, Supplier<Long> getSeqNo) throws Exception {
        txn.preProcessOperation(operation);
        if (operation.getSequenceNumber() < 0) {
            operation.setSequenceNumber(getSeqNo.get());
        }

        txn.acceptOperation(operation);
    }

    private UpdateableContainerMetadata getCheckpointedMetadata(MetadataCheckpointOperation operation) throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        val txn = createUpdateTransaction(metadata);
        processOperation(operation, txn, () -> 1L);
        txn.commit(metadata);
        return metadata;
    }

    private void checkNoSequenceNumberAssigned(Operation op, String context) {
        AssertExtensions.assertLessThan(
                "Unexpected Sequence Number after " + context + ".",
                0,
                op.getSequenceNumber());
    }

    /**
     * Verify that the given ContainerMetadata objects contain the same data.
     */
    static void assertMetadataSame(String message, ContainerMetadata expected, ContainerMetadata actual) {
        Assert.assertEquals("Unexpected ContainerId.", expected.getContainerId(), actual.getContainerId());
        Collection<Long> expectedSegmentIds = expected.getAllStreamSegmentIds();
        Collection<Long> actualSegmentIds = actual.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements(message + " Unexpected Segments mapped.", expectedSegmentIds, actualSegmentIds);
        for (long streamSegmentId : expectedSegmentIds) {
            SegmentMetadata expectedSegmentMetadata = expected.getStreamSegmentMetadata(streamSegmentId);
            SegmentMetadata actualSegmentMetadata = actual.getStreamSegmentMetadata(streamSegmentId);
            Assert.assertNotNull(message + " No metadata for Segment " + streamSegmentId, actualSegmentMetadata);
            SegmentMetadataComparer.assertEquals(message, expectedSegmentMetadata, actualSegmentMetadata);
        }
    }

    private void verifyAttributeUpdates(String stepName, ContainerMetadata containerMetadata, Collection<AttributeUpdate> attributeUpdates, Map<UUID, Long> expectedValues) {
        // Verify that the Attribute Updates have their expected values and that the updater has internalized the attribute updates.
        val transactionMetadata = containerMetadata.getStreamSegmentMetadata(SEGMENT_ID);
        val expectedTransactionAttributes = new HashMap<UUID, Long>(expectedValues);
        attributeUpdates.forEach(au -> expectedTransactionAttributes.put(au.getAttributeId(), au.getValue()));
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes in transaction metadata " + stepName + ".",
                expectedTransactionAttributes, transactionMetadata);
        for (AttributeUpdate au : attributeUpdates) {
            Assert.assertEquals("Unexpected updated value for AttributeUpdate[" + au.getUpdateType() + "] " + stepName,
                    (long) expectedValues.get(au.getAttributeId()), au.getValue());
        }
    }

    private void checkLastKnownSequenceNumber(String message, long expectedLastUsed, SegmentMetadata metadata) {
        Assert.assertTrue("Unexpected type of metadata.", metadata instanceof StreamSegmentMetadata);
        Assert.assertEquals(message, expectedLastUsed, metadata.getLastUsed());
    }

    //endregion
}
