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

package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.BadEventNumberException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentMergedException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentContainerMetadata;
import com.emc.pravega.service.server.logs.operations.MergeTransactionOperation;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.logs.operations.TransactionMapOperation;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Unit tests for OperationMetadataUpdater class.
 */
public class OperationMetadataUpdaterTests {
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
    private final AtomicLong nextEventNumber = new AtomicLong();
    private final Supplier<AppendContext> nextAppendContext = () -> new AppendContext(UUID.randomUUID(), nextEventNumber.incrementAndGet());

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
        UpdateableContainerMetadata metadata = createMetadata();
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When everything is OK (in recovery mode) - nothing should change.
        metadata.enterRecoveryMode();
        OperationMetadataUpdater recoveryUpdater = createUpdater(metadata);
        recoveryUpdater.preProcessOperation(appendOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentOffset after call to preProcess in recovery mode.", 0, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state in recovery mode.", SEGMENT_LENGTH, recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata in recovery mode.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata); // Need to create again since we exited recovery mode.
        updater.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.", SEGMENT_LENGTH, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.", SEGMENT_LENGTH, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When StreamSegment is merged (via transaction).
        StreamSegmentAppendOperation transactionAppendOp = new StreamSegmentAppendOperation(SEALED_TRANSACTION_ID, DEFAULT_APPEND_DATA, nextAppendContext.get());
        MergeTransactionOperation mergeOp = createMerge();
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in transaction).",
                () -> updater.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        updater.commit();
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in metadata).",
                () -> updater.preProcessOperation(transactionAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        StreamSegmentSealOperation sealOp = createSeal();
        updater.preProcessOperation(sealOp);
        updater.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in transaction).",
                () -> updater.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        updater.commit();
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in metadata).",
                () -> updater.preProcessOperation(createAppendNoOffset()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentAppend operations.
     */
    @Test
    public void testAcceptStreamSegmentAppend() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> updater.acceptOperation(appendOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception.", SEGMENT_LENGTH, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When all is good.
        updater.preProcessOperation(appendOp);
        updater.acceptOperation(appendOp);
        Assert.assertEquals("acceptOperation did not update the transaction.", SEGMENT_LENGTH + appendOp.getData().length, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
    }

    /**
     * Tests the ability of the OperationMetadataUpdater to process (and accept) StreamSegmentAppendOperations with
     * predefined offsets.
     */
    @Test
    public void testStreamSegmentAppendWithOffset() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Append #1 (at offset 0).
        long offset = metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength();
        StreamSegmentAppendOperation appendOp = createAppendWithOffset(offset);
        updater.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.", offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.", offset, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.", offset, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        updater.acceptOperation(appendOp);

        // Append #2 (after Append #1)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength();
        appendOp = createAppendWithOffset(offset);
        updater.preProcessOperation(appendOp);
        Assert.assertEquals("Unexpected StreamSegmentOffset after call to preProcess in non-recovery mode.", offset, appendOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(appendOp, "call to preProcess in non-recovery mode");
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.", offset, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        updater.acceptOperation(appendOp);

        // Append #3 (wrong offset)
        offset = appendOp.getStreamSegmentOffset() + appendOp.getLength() - 1;
        StreamSegmentAppendOperation badAppendOp = createAppendWithOffset(offset);
        AssertExtensions.assertThrows(
                "preProcessOperations accepted an append with the wrong offset.",
                () -> updater.preProcessOperation(badAppendOp),
                ex -> ex instanceof BadOffsetException);
        AssertExtensions.assertThrows(
                "acceptOperation accepted an append that was rejected during preProcessing.",
                () -> updater.acceptOperation(badAppendOp),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the ability of the OperationMetadataUpdater to reject StreamSegmentAppends with out-of-order EventNumbers.
     */
    @Test
    public void testStreamSegmentAppendWithBadEventNumbers() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Append #1.
        StreamSegmentAppendOperation appendOp = createAppendNoOffset();
        updater.preProcessOperation(appendOp);
        updater.acceptOperation(appendOp);

        // Append #2 (same context as Append #1)
        StreamSegmentAppendOperation badAppendOp = new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, appendOp.getAppendContext());
        AssertExtensions.assertThrows(
                "preProcessOperation accepted a StreamSegmentAppendOperation with an out-of-order Event Number. (test #1)",
                () -> updater.preProcessOperation(badAppendOp),
                ex -> ex instanceof BadEventNumberException);

        // Append #3 (same EventNumber, but different clientId).
        StreamSegmentAppendOperation differentClientAppend = new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, new AppendContext(UUID.randomUUID(), appendOp.getAppendContext().getEventNumber()));
        updater.preProcessOperation(differentClientAppend);
        updater.acceptOperation(differentClientAppend);

        // Append #4 (same context as Append #3).
        StreamSegmentAppendOperation badAppendOp2 = new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, differentClientAppend.getAppendContext());
        AssertExtensions.assertThrows(
                "preProcessOperation accepted a StreamSegmentAppendOperation with an out-of-order Event Number. (test #2)",
                () -> updater.preProcessOperation(badAppendOp2),
                ex -> ex instanceof BadEventNumberException);
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
        OperationMetadataUpdater recoveryUpdater = createUpdater(metadata);
        recoveryUpdater.preProcessOperation(sealOp);
        AssertExtensions.assertLessThan("Unexpected StreamSegmentLength after call to preProcess in recovery mode.", 0, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata); // Need to create again since we exited recovery mode.
        updater.preProcessOperation(sealOp);
        Assert.assertEquals("Unexpected StreamSegmentLength after call to preProcess in non-recovery mode.", SEGMENT_LENGTH, sealOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state.", recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When StreamSegment is merged (via transaction).
        StreamSegmentSealOperation transactionSealOp = new StreamSegmentSealOperation(SEALED_TRANSACTION_ID);
        MergeTransactionOperation mergeOp = createMerge();
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        Assert.assertFalse("Transaction should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in transaction).",
                () -> updater.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        updater.commit();
        Assert.assertTrue("Transaction should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in metadata).",
                () -> updater.preProcessOperation(transactionSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        updater.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in transaction).",
                () -> updater.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        updater.commit();
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is sealed (in metadata).",
                () -> updater.preProcessOperation(createSeal()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

    /**
     * Tests the accept method with StreamSegmentSeal operations.
     */
    @Test
    public void testAcceptStreamSegmentSeal() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        StreamSegmentSealOperation sealOp = createSeal();

        // When no pre-process has happened.
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> updater.acceptOperation(sealOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertFalse("acceptOperation updated the transaction even if it threw an exception.", updater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When all is good.
        updater.preProcessOperation(sealOp);
        updater.acceptOperation(sealOp);
        Assert.assertTrue("acceptOperation did not update the transaction.", updater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("acceptOperation updated the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
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
        OperationMetadataUpdater recoveryUpdater = createUpdater(metadata);
        AssertExtensions.assertThrows(
                "preProcess(Merge) handled an operation with no Transaction StreamSegment Length set.",
                () -> recoveryUpdater.preProcessOperation(createMerge()),
                ex -> ex instanceof MetadataUpdateException);

        // In recovery mode, the updater does not set the length; it just validates that it has one.
        recoveryMergeOp.setLength(metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).getDurableLogLength());
        recoveryUpdater.preProcessOperation(recoveryMergeOp);
        AssertExtensions.assertLessThan("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.", 0, recoveryMergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(recoveryMergeOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());

        // When everything is OK (non-recovery mode).
        MergeTransactionOperation mergeOp = createMerge();
        metadata.exitRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata);
        updater.preProcessOperation(mergeOp);
        Assert.assertEquals("Unexpected Transaction StreamSegmentLength after call to preProcess in recovery mode.", SEALED_TRANSACTION_LENGTH, mergeOp.getLength());
        Assert.assertEquals("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.", SEGMENT_LENGTH, mergeOp.getStreamSegmentOffset());
        checkNoSequenceNumberAssigned(mergeOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());

        // When Target StreamSegment is sealed.
        StreamSegmentSealOperation sealTargetOp = createSeal();
        updater.preProcessOperation(sealTargetOp);
        updater.acceptOperation(sealTargetOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is sealed.",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentSealedException);

        updater.rollback(); // Rollback the seal

        // When Target StreamSegment is a Transaction.
        MergeTransactionOperation mergeToTransactionOp = new MergeTransactionOperation(NOTSEALED_TRANSACTION_ID, SEALED_TRANSACTION_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is a Transaction.",
                () -> updater.preProcessOperation(mergeToTransactionOp),
                ex -> ex instanceof MetadataUpdateException);

        // When Transaction is not sealed.
        MergeTransactionOperation mergeNonSealed = new MergeTransactionOperation(NOTSEALED_TRANSACTION_ID, SEGMENT_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is not sealed.",
                () -> updater.preProcessOperation(mergeNonSealed),
                ex -> ex instanceof MetadataUpdateException);

        // When Transaction is already merged.
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in transaction).",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);

        updater.commit();
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Transaction StreamSegment is already merged (in metadata).",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);
    }

    /**
     * Tests the accept method with MergeTransactionOperations.
     */
    @Test
    public void testAcceptMergeTransaction() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        MergeTransactionOperation mergeOp = createMerge();

        // When no pre-process has happened
        AssertExtensions.assertThrows(
                "Unexpected behavior from acceptOperation() when no pre-processing was made.",
                () -> updater.acceptOperation(mergeOp),
                ex -> ex instanceof MetadataUpdateException);

        Assert.assertEquals("acceptOperation updated the transaction even if it threw an exception (parent segment).", SEGMENT_LENGTH, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata (parent segment).", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        updater.rollback(); // This would naturally happen in case of a failure, so we need to simulate this here too.

        // When all is good.
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        Assert.assertTrue("acceptOperation did not update the transaction(Transaction).", updater.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertFalse("acceptOperation updated the metadata (Transaction).", metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID).isMerged());
        Assert.assertEquals("acceptOperation did not update the transaction.", SEGMENT_LENGTH + SEALED_TRANSACTION_LENGTH, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
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
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Brand new StreamSegment.
        StreamSegmentMapOperation mapOp = createMap();
        updater.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation did modified the StreamSegmentId on the operation in recovery mode.", ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        updater.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.", ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        updater.acceptOperation(mapOp);

        Assert.assertEquals("Unexpected StorageLength after call to acceptOperation (in transaction).", mapOp.getLength(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to acceptOperation (in transaction).", mapOp.getLength(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getDurableLogLength());
        Assert.assertEquals("Unexpected value for isSealed after call to acceptOperation (in transaction).", mapOp.isSealed(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).isSealed());
        Assert.assertNull("acceptOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // StreamSegmentName already exists (transaction).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in transaction).",
                () -> updater.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        updater.commit();

        // StreamSegmentName already exists (metadata).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a StreamSegment with the same Name already exists (in metadata).",
                () -> updater.preProcessOperation(createMap(mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the processOperation and acceptOperation methods with TransactionMap operations.
     */
    @Test
    public void testPreProcessTransactionMap() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Parent does not exist.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a Transaction StreamSegment to an inexistent parent.",
                () -> updater.preProcessOperation(createTransactionMap(12345)),
                ex -> ex instanceof MetadataUpdateException);

        // Brand new Transaction (and parent).
        StreamSegmentMapOperation mapParent = createMap();
        updater.preProcessOperation(mapParent); // Create parent.
        updater.acceptOperation(mapParent);

        // Part 1: recovery mode.
        metadata.enterRecoveryMode();
        TransactionMapOperation mapOp = createTransactionMap(mapParent.getStreamSegmentId());
        updater.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation changed the StreamSegmentId on the operation in recovery mode.", ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        updater.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.", ContainerMetadata.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        updater.acceptOperation(mapOp);
        Assert.assertEquals("Unexpected StorageLength after call to processMetadataOperation (in transaction).", mapOp.getLength(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to processMetadataOperation (in transaction).", 0, updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getDurableLogLength());
        Assert.assertEquals("Unexpected value for isSealed after call to processMetadataOperation (in transaction).", mapOp.isSealed(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).isSealed());
        Assert.assertNull("processMetadataOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // Transaction StreamSegmentName exists (transaction).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a TransactionStreamSegment with the same Name already exists (in transaction).",
                () -> updater.preProcessOperation(createTransactionMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        updater.commit();

        // Transaction StreamSegmentName exists (metadata).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a TransactionStreamSegment with the same Name already exists (in metadata).",
                () -> updater.preProcessOperation(createTransactionMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);
    }

    /**
     * Tests the processMetadataOperation method with MetadataCheckpoint operations.
     */
    @Test
    public void testProcessMetadataCheckpoint() throws Exception {
        // When encountering MetadataCheckpoint in non-Recovery Mode, the OperationMetadataUpdater serializes a snapshot
        // of the current metadata inside the MetadataPersisted.
        // When encountering MetadataCheckpoint in Recovery Mode, the OperationMetadataUpdater deserializes the snapshot-ted
        // metadata in it and applies it to the container metadata (inside the transaction). All existing metadata updates
        // are cleared.

        String newSegmentName = "NewSegmentId";
        AtomicLong seqNo = new AtomicLong();

        // Create a non-empty metadata.
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Checkpoint 1: original metadata.
        // Checkpoint 2: Checkpoint 1 + 1 StreamSegment and 1 Transaction + 1 Append
        MetadataCheckpointOperation checkpoint1 = createMetadataPersisted();
        MetadataCheckpointOperation checkpoint2 = createMetadataPersisted();

        // Checkpoint 1 Should have original metadata.
        processOperation(checkpoint1, updater, seqNo::incrementAndGet);
        UpdateableContainerMetadata checkpointedMetadata = getCheckpointedMetadata(checkpoint1);
        MetadataHelpers.assertMetadataEquals("Unexpected metadata before any operation.", metadata, checkpointedMetadata);

        // Map another StreamSegment, and add an append
        StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(new StreamSegmentInformation(newSegmentName, SEGMENT_LENGTH, false, false, new Date()));
        processOperation(mapOp, updater, seqNo::incrementAndGet);
        processOperation(new StreamSegmentAppendOperation(mapOp.getStreamSegmentId(), DEFAULT_APPEND_DATA, nextAppendContext.get()), updater, seqNo::incrementAndGet);
        processOperation(checkpoint2, updater, seqNo::incrementAndGet);

        // Checkpoint 2 should have Checkpoint 1 + New StreamSegment + Append.
        updater.commit();
        checkpointedMetadata = getCheckpointedMetadata(checkpoint2);
        MetadataHelpers.assertMetadataEquals("Unexpected metadata after deserializing checkpoint.", metadata, checkpointedMetadata);
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
        OperationMetadataUpdater updater = createUpdater(metadata);
        MetadataCheckpointOperation checkpointedMetadata = createMetadataPersisted();
        processOperation(checkpointedMetadata, updater, seqNo::incrementAndGet);

        // Create a blank metadata, and add an operation to the updater (which would result in mapping a new StreamSegment).
        metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        updater = createUpdater(metadata);
        StreamSegmentMapOperation mapOp = createMap(newSegmentName);
        mapOp.setStreamSegmentId(newSegmentId);
        processOperation(mapOp, updater, seqNo::incrementAndGet);

        // Now try to process the checkpoint
        processOperation(checkpointedMetadata, updater, seqNo::incrementAndGet);
        updater.commit();

        // Verify the checkpointed metadata hasn't been applied
        Assert.assertNull("Newly added StreamSegment Id was not removed after applying checkpoint.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
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
        OperationMetadataUpdater updater = createUpdater(metadata);

        ArrayList<StorageOperation> testOperations = new ArrayList<>();
        testOperations.add(createAppendNoOffset());
        testOperations.add(createSeal());
        testOperations.add(createMerge());

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for a non-existent Segment: " + op,
                    () -> updater.preProcessOperation(op),
                    ex -> ex instanceof MetadataUpdateException);

            AssertExtensions.assertThrows(
                    "Unexpected behavior from acceptOperation when processing an operation for a non-existent Segment: " + op,
                    () -> updater.acceptOperation(op),
                    ex -> ex instanceof MetadataUpdateException);
        }

        // If the StreamSegment was previously marked as deleted.
        UpdateableSegmentMetadata segmentMetadata = metadata.mapStreamSegmentId("foo", SEGMENT_ID);
        segmentMetadata.markDeleted();

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for deleted Segment: " + op,
                    () -> updater.preProcessOperation(op),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

    /**
     * Tests the functionality of the nextOperationSequenceNumber() method.
     */
    @Test
    public void testGetNewOperationSequenceNumber() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        for (int i = 0; i < 100; i++) {
            long seqNo1 = updater.nextOperationSequenceNumber();
            long seqNo2 = metadata.nextOperationSequenceNumber();
            Assert.assertEquals("Unexpected behavior from nextOperationSequenceNumber.", seqNo2 - 1, seqNo1);
        }
    }

    /**
     * Tests the ability of the OperationMetadataUpdater to commit all outstanding changes.
     */
    @Test
    public void testCommit() throws Exception {
        // Create a few appends, merge a Transaction and seal the parent stream. Verify all changes have been applied after
        // a call to commit().
        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppendNoOffset());
        }

        operations.add(createMerge());
        operations.add(createSeal());

        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        for (StorageOperation op : operations) {
            updater.preProcessOperation(op);
            updater.acceptOperation(op);
        }

        updater.commit();
        Assert.assertEquals("commit() seems to have modified the metadata sequence number while not in recovery mode.", ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER, metadata.nextOperationSequenceNumber() - 1);

        long expectedLength = SEGMENT_LENGTH + appendCount * DEFAULT_APPEND_DATA.length + SEALED_TRANSACTION_LENGTH;
        SegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in metadata after commit.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertTrue("Unexpected value for isSealed in metadata after commit.", parentMetadata.isSealed());
        SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertTrue("Unexpected value for isSealed in Transaction metadata after commit.", transactionMetadata.isSealed());
        Assert.assertTrue("Unexpected value for isMerged in metadata after commit.", transactionMetadata.isMerged());
    }

    /**
     * Tests the ability of the OperationMetadataUpdater to set the Sequence Number on the Metadata, when in Recovery Mode.
     */
    @Test
    public void testCommitSequenceNumber() throws Exception {
        final long newSeqNo = 1235;
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata);
        updater.setOperationSequenceNumber(newSeqNo);
        updater.commit();
        metadata.exitRecoveryMode();
        Assert.assertEquals("commit() did not set the metadata sequence number while in recovery mode.", newSeqNo + 1, metadata.nextOperationSequenceNumber());

        // Now try again in non-recovery mode.
        OperationMetadataUpdater nonRecoveryUpdater = createUpdater(metadata);
        AssertExtensions.assertThrows(
                "setOperationSequence number should not work in non-recovery mode.",
                () -> nonRecoveryUpdater.setOperationSequenceNumber(newSeqNo + 10),
                ex -> ex instanceof IllegalStateException);
    }

    /**
     * Tests the ability of the OperationMetadataUpdater to rollback all outstanding changes.
     */
    @Test
    public void testRollback() throws Exception {
        // Create a couple of operations, commit them, and then create a few more appends, merge a Transaction and seal the parent stream.
        // Then call rollback(); verify no changes have been applied after the call to commit().

        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        StreamSegmentAppendOperation committedAppend = createAppendNoOffset();
        updater.preProcessOperation(committedAppend);
        updater.acceptOperation(committedAppend);
        updater.commit(); // This is the last extent of the modifications to the metadata.

        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppendNoOffset());
        }

        operations.add(createMerge());
        operations.add(createSeal());

        for (StorageOperation op : operations) {
            updater.preProcessOperation(op);
            updater.acceptOperation(op);
        }

        updater.rollback();

        long expectedLength = SEGMENT_LENGTH + DEFAULT_APPEND_DATA.length;

        // Verify metadata is untouched and that the updater has truly rolled back.
        SegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in metadata after rollback.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertFalse("Unexpected value for isSealed in metadata after rollback.", parentMetadata.isSealed());
        SegmentMetadata transactionMetadata = metadata.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertFalse("Unexpected value for isMerged in metadata after rollback.", transactionMetadata.isMerged());

        // Now the updater
        parentMetadata = updater.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertNull("Unexpected state of the updater after rollback.", parentMetadata);
        transactionMetadata = updater.getStreamSegmentMetadata(SEALED_TRANSACTION_ID);
        Assert.assertNull("Unexpected state of the updater after rollback.", transactionMetadata);
    }

    /**
     * Tests the recordTruncationMarker() method.
     */
    @Test
    public void testRecordTruncationMarker() {
        int recordCount = 100;

        // Record 100 entries, and make sure the TruncationMarkerCollection contains them as soon as recorded.
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater u = new OperationMetadataUpdater(metadata);
        LogAddress previousMarker = null;
        for (int i = 0; i < recordCount; i++) {
            LogAddress dfAddress = new LogAddress(i * i) {
            };
            u.recordTruncationMarker(i, dfAddress);
            LogAddress actualMarker = metadata.getClosestTruncationMarker(i);
            Assert.assertEquals("Unexpected value for truncation marker (pre-commit) for Operation Sequence Number " + i, previousMarker, actualMarker);
            u.commit();
            actualMarker = metadata.getClosestTruncationMarker(i);
            Assert.assertEquals("Unexpected value for truncation marker (post-commit) for Operation Sequence Number " + i, dfAddress, actualMarker);
            previousMarker = actualMarker;
        }
    }

    //endregion

    //region Helpers

    private UpdateableContainerMetadata createBlankMetadata() {
        return new StreamSegmentContainerMetadata(CONTAINER_ID);
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

    private OperationMetadataUpdater createUpdater(UpdateableContainerMetadata metadata) {
        return new OperationMetadataUpdater(metadata);
    }

    private StreamSegmentAppendOperation createAppendNoOffset() {
        return new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, nextAppendContext.get());
    }

    private StreamSegmentAppendOperation createAppendWithOffset(long offset) {
        return new StreamSegmentAppendOperation(SEGMENT_ID, offset, DEFAULT_APPEND_DATA, nextAppendContext.get());
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
        return new StreamSegmentMapOperation(new StreamSegmentInformation(name, SEGMENT_LENGTH, true, false, new Date()));
    }

    private TransactionMapOperation createTransactionMap(long parentId) {
        return createTransactionMap(parentId, SEALED_TRANSACTION_NAME);
    }

    private TransactionMapOperation createTransactionMap(long parentId, String name) {
        return new TransactionMapOperation(parentId, new StreamSegmentInformation(name, SEALED_TRANSACTION_LENGTH, true, false, new Date()));
    }

    private MetadataCheckpointOperation createMetadataPersisted() {
        return new MetadataCheckpointOperation();
    }

    private void checkNoSequenceNumberAssigned(Operation op, String context) {
        AssertExtensions.assertLessThan(
                "Unexpected Sequence Number after " + context + ".",
                0,
                op.getSequenceNumber());
    }

    private void processOperation(Operation operation, OperationMetadataUpdater updater, Supplier<Long> getSeqNo) throws Exception {
        updater.preProcessOperation(operation);
        if (operation.getSequenceNumber() < 0) {
            operation.setSequenceNumber(getSeqNo.get());
        }

        updater.acceptOperation(operation);
    }

    private UpdateableContainerMetadata getCheckpointedMetadata(MetadataCheckpointOperation operation) throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.enterRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata);
        processOperation(operation, updater, () -> 1L);
        boolean success = updater.commit();
        Assert.assertTrue("OperationMetadataUpdater.commit() did not make any modifications.", success);
        return metadata;
    }

    //endregion
}
