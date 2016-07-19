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

package com.emc.logservice.server.logs;

import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.StreamSegmentMergedException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.server.ContainerMetadata;
import com.emc.logservice.server.MetadataHelpers;
import com.emc.logservice.server.SegmentMetadata;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.StreamSegmentInformation;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.containers.StreamSegmentContainerMetadata;
import com.emc.logservice.server.logs.operations.BatchMapOperation;
import com.emc.logservice.server.logs.operations.MergeBatchOperation;
import com.emc.logservice.server.logs.operations.MetadataCheckpointOperation;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentMapOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentSealOperation;
import com.emc.nautilus.testcommon.AssertExtensions;
import com.google.common.base.Supplier;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for OperationMetadataUpdater class.
 */
public class OperationMetadataUpdaterTests {
    private static final String CONTAINER_ID = "TestContainer";
    private static final String SEGMENT_NAME = "Segment_123";
    private static final long SEGMENT_ID = 123;
    private static final String SEALED_BATCH_NAME = "Segment_123#Batch_Sealed";
    private static final long SEALED_BATCH_ID = 567;
    private static final String NOTSEALED_BATCH_NAME = "Segment_123#Batch_NotSealed";
    private static final long NOTSEALED_BATCH_ID = 890;
    private static final long SEALED_BATCH_LENGTH = 12;
    private static final long SEGMENT_LENGTH = 1234567;
    private static final AppendContext DEFAULT_APPEND_CONTEXT = new AppendContext(UUID.randomUUID(), 0);
    private static final byte[] DEFAULT_APPEND_DATA = "hello".getBytes();

    /**
     * Tests the behavior of preProcessOperation and acceptOperation when encountering an invalid StreamSegmentId, or
     * when encountering a StreamSegment Id for a deleted StreamSegment.
     *
     * @throws Exception
     */
    @Test
    public void testPreProcessAndAcceptWithInvalidSegmentId() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        ArrayList<StorageOperation> testOperations = new ArrayList<>();
        testOperations.add(createAppend());
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
        metadata.mapStreamSegmentId("foo", SEGMENT_ID);
        metadata.getStreamSegmentMetadata(SEGMENT_ID).markDeleted();

        for (StorageOperation op : testOperations) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior from preProcessOperation when processing an operation for deleted Segment: " + op,
                    () -> updater.preProcessOperation(op),
                    ex -> ex instanceof StreamSegmentNotExistsException);
        }
    }

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

        StreamSegmentAppendOperation appendOp = createAppend();

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
        Assert.assertEquals("preProcess(Append) seems to have changed the Updater internal state.", SEGMENT_LENGTH, recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("preProcess(Append) seems to have changed the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());

        // When StreamSegment is merged (via transaction).
        StreamSegmentAppendOperation batchAppendOp = new StreamSegmentAppendOperation(SEALED_BATCH_ID, DEFAULT_APPEND_DATA, DEFAULT_APPEND_CONTEXT);
        MergeBatchOperation mergeOp = createMerge();
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        Assert.assertFalse("BatchStreamSegment should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in transaction).",
                () -> updater.preProcessOperation(batchAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        updater.commit();
        Assert.assertTrue("BatchStreamSegment should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is merged (in metadata).",
                () -> updater.preProcessOperation(batchAppendOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is sealed (via transaction).
        StreamSegmentSealOperation sealOp = createSeal();
        updater.preProcessOperation(sealOp);
        updater.acceptOperation(sealOp);
        Assert.assertFalse("StreamSegment should not be sealed in metadata (yet).", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in transaction).",
                () -> updater.preProcessOperation(createAppend()),
                ex -> ex instanceof StreamSegmentSealedException);

        // When StreamSegment is sealed (via metadata).
        updater.commit();
        Assert.assertTrue("StreamSegment should have been sealed in metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Append) when Segment is sealed (in metadata).",
                () -> updater.preProcessOperation(createAppend()),
                ex -> ex instanceof StreamSegmentSealedException);
    }

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
        AssertExtensions.assertLessThan("Unexpected StreamSegmentLength after call to preProcess in recovery mode.", 0, sealOp.getStreamSegmentLength());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When everything is OK (no recovery mode).
        metadata.exitRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata); // Need to create again since we exited recovery mode.
        updater.preProcessOperation(sealOp);
        Assert.assertEquals("Unexpected StreamSegmentLength after call to preProcess in non-recovery mode.", SEGMENT_LENGTH, sealOp.getStreamSegmentLength());
        checkNoSequenceNumberAssigned(sealOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Seal) seems to have changed the Updater internal state.", recoveryUpdater.getStreamSegmentMetadata(SEGMENT_ID).isSealed());
        Assert.assertFalse("preProcess(Seal) seems to have changed the metadata.", metadata.getStreamSegmentMetadata(SEGMENT_ID).isSealed());

        // When StreamSegment is merged (via transaction).
        StreamSegmentSealOperation batchSealOp = new StreamSegmentSealOperation(SEALED_BATCH_ID);
        MergeBatchOperation mergeOp = createMerge();
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        Assert.assertFalse("BatchStreamSegment should not be merged in metadata (yet).", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in transaction).",
                () -> updater.preProcessOperation(batchSealOp),
                ex -> ex instanceof StreamSegmentMergedException);

        // When StreamSegment is merged (via metadata).
        updater.commit();
        Assert.assertTrue("BatchStreamSegment should have been merged in metadata.", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Seal) when Segment is merged (in metadata).",
                () -> updater.preProcessOperation(batchSealOp),
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
     * Tests the preProcess method with MergeBatch operations.
     * Scenarios:
     * * Recovery/non-recovery mode
     * * Target StreamSegment is sealed
     * * Target StreamSegment is a batch
     * * Batch StreamSegment is already merged
     * * Batch StreamSegment is not sealed
     */
    @Test
    public void testPreProcessMergeBatch() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();

        // When everything is OK (recovery mode).
        MergeBatchOperation recoveryMergeOp = createMerge();
        metadata.enterRecoveryMode();
        OperationMetadataUpdater recoveryUpdater = createUpdater(metadata);
        AssertExtensions.assertThrows(
                "preProcess(Merge) handled an operation with no Batch StreamSegment Length set.",
                () -> recoveryUpdater.preProcessOperation(createMerge()),
                ex -> ex instanceof MetadataUpdateException);

        // In recovery mode, the updater does not set the length; it just validates that it has one.
        recoveryMergeOp.setBatchStreamSegmentLength(metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).getDurableLogLength());
        recoveryUpdater.preProcessOperation(recoveryMergeOp);
        AssertExtensions.assertLessThan("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.", 0, recoveryMergeOp.getTargetStreamSegmentOffset());
        checkNoSequenceNumberAssigned(recoveryMergeOp, "call to preProcess in recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());

        // When everything is OK (non-recovery mode).
        MergeBatchOperation mergeOp = createMerge();
        metadata.exitRecoveryMode();
        OperationMetadataUpdater updater = createUpdater(metadata);
        updater.preProcessOperation(mergeOp);
        Assert.assertEquals("Unexpected Batch StreamSegmentLength after call to preProcess in recovery mode.", SEALED_BATCH_LENGTH, mergeOp.getBatchStreamSegmentLength());
        Assert.assertEquals("Unexpected Target StreamSegmentOffset after call to preProcess in recovery mode.", SEGMENT_LENGTH, mergeOp.getTargetStreamSegmentOffset());
        checkNoSequenceNumberAssigned(mergeOp, "call to preProcess in non-recovery mode");
        Assert.assertFalse("preProcess(Merge) seems to have changed the Updater internal state in recovery mode.", recoveryUpdater.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        Assert.assertFalse("preProcess(Merge) seems to have changed the metadata in recovery mode.", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());

        // When Target StreamSegment is sealed.
        StreamSegmentSealOperation sealTargetOp = createSeal();
        updater.preProcessOperation(sealTargetOp);
        updater.acceptOperation(sealTargetOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is sealed.",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentSealedException);

        updater.rollback(); // Rollback the seal

        // When Target StreamSegment is a Batch.
        MergeBatchOperation mergeToBatchOp = new MergeBatchOperation(NOTSEALED_BATCH_ID, SEALED_BATCH_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Target StreamSegment is a batch.",
                () -> updater.preProcessOperation(mergeToBatchOp),
                ex -> ex instanceof MetadataUpdateException);

        // When Batch is not sealed.
        MergeBatchOperation mergeNonSealed = new MergeBatchOperation(NOTSEALED_BATCH_ID, SEGMENT_ID);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Batch StreamSegment is not sealed.",
                () -> updater.preProcessOperation(mergeNonSealed),
                ex -> ex instanceof MetadataUpdateException);

        // When Batch is already merged.
        updater.preProcessOperation(mergeOp);
        updater.acceptOperation(mergeOp);
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Batch StreamSegment is already merged (in transaction).",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);

        updater.commit();
        AssertExtensions.assertThrows(
                "Unexpected behavior for preProcess(Merge) when Batch StreamSegment is already merged (in metadata).",
                () -> updater.preProcessOperation(createMerge()),
                ex -> ex instanceof StreamSegmentMergedException);
    }

    /**
     * Tests the accept method with StreamSegmentAppend operations.
     */
    @Test
    public void testAcceptStreamSegmentAppend() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        StreamSegmentAppendOperation appendOp = createAppend();

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

    /**
     * Tests the accept method with MergeBatch operations.
     */
    @Test
    public void testAcceptMergeBatch() throws Exception {
        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        MergeBatchOperation mergeOp = createMerge();

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
        Assert.assertTrue("acceptOperation did not update the transaction(batch segment).", updater.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        Assert.assertFalse("acceptOperation updated the metadata (batch segment).", metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).isMerged());
        Assert.assertEquals("acceptOperation did not update the transaction.", SEGMENT_LENGTH + SEALED_BATCH_LENGTH, updater.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
        Assert.assertEquals("acceptOperation updated the metadata.", SEGMENT_LENGTH, metadata.getStreamSegmentMetadata(SEGMENT_ID).getDurableLogLength());
    }

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
        Assert.assertEquals("preProcessOperation did modified the StreamSegmentId on the operation in recovery mode.", SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        updater.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.", SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
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
     * Tests the processOperation and acceptOperation methods with BatchMap operations.
     */
    @Test
    public void testPreProcessBatchMap() throws Exception {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);

        // Parent does not exist.
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when attempting to map a Batch StreamSegment to an inexistent parent.",
                () -> updater.preProcessOperation(createBatchMap(12345))
                , ex -> ex instanceof MetadataUpdateException);

        // Brand new batch (and parent).
        StreamSegmentMapOperation mapParent = createMap();
        updater.preProcessOperation(mapParent); // Create parent.
        updater.acceptOperation(mapParent);

        // Part 1: recovery mode.
        metadata.enterRecoveryMode();
        BatchMapOperation mapOp = createBatchMap(mapParent.getStreamSegmentId());
        updater.preProcessOperation(mapOp);
        Assert.assertEquals("preProcessOperation changed the StreamSegmentId on the operation in recovery mode.", SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());

        // Part 2: non-recovery mode.
        metadata.exitRecoveryMode();
        updater.preProcessOperation(mapOp);
        Assert.assertNotEquals("preProcessOperation did not set the StreamSegmentId on the operation.", SegmentMetadataCollection.NO_STREAM_SEGMENT_ID, mapOp.getStreamSegmentId());
        Assert.assertNull("preProcessOperation modified the current transaction.", updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));
        Assert.assertNull("preProcessOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        updater.acceptOperation(mapOp);
        Assert.assertEquals("Unexpected StorageLength after call to processMetadataOperation (in transaction).", mapOp.getLength(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getStorageLength());
        Assert.assertEquals("Unexpected DurableLogLength after call to processMetadataOperation (in transaction).", 0, updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).getDurableLogLength());
        Assert.assertEquals("Unexpected value for isSealed after call to processMetadataOperation (in transaction).", mapOp.isSealed(), updater.getStreamSegmentMetadata(mapOp.getStreamSegmentId()).isSealed());
        Assert.assertNull("processMetadataOperation modified the underlying metadata.", metadata.getStreamSegmentMetadata(mapOp.getStreamSegmentId()));

        // Batch StreamSegmentName exists (transaction).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a BatchStreamSegment with the same Name already exists (in transaction).",
                () -> updater.preProcessOperation(createBatchMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
                ex -> ex instanceof MetadataUpdateException);

        // Make changes permanent.
        updater.commit();

        // Batch StreamSegmentName exists (metadata).
        AssertExtensions.assertThrows(
                "Unexpected behavior from preProcessOperation when a BatchStreamSegment with the same Name already exists (in metadata).",
                () -> updater.preProcessOperation(createBatchMap(mapParent.getStreamSegmentId(), mapOp.getStreamSegmentName())),
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
        // Checkpoint 2: Checkpoint 1 + 1 StreamSegment and 1 Batch + 1 Append
        MetadataCheckpointOperation checkpoint1 = createMetadataPersisted();
        MetadataCheckpointOperation checkpoint2 = createMetadataPersisted();

        // Checkpoint 1 Should have original metadata.
        processOperation(checkpoint1, updater, seqNo::incrementAndGet);
        UpdateableContainerMetadata checkpointedMetadata = getCheckpointedMetadata(checkpoint1);
        MetadataHelpers.assertMetadataEquals("Unexpected metadata before any operation.", metadata, checkpointedMetadata);

        // Map another StreamSegment, and add an append
        StreamSegmentMapOperation mapOp = new StreamSegmentMapOperation(new StreamSegmentInformation(newSegmentName, SEGMENT_LENGTH, false, false, new Date()));
        processOperation(mapOp, updater, seqNo::incrementAndGet);
        processOperation(new StreamSegmentAppendOperation(mapOp.getStreamSegmentId(), DEFAULT_APPEND_DATA, DEFAULT_APPEND_CONTEXT), updater, seqNo::incrementAndGet);
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
        // Create a few appends, merge a batch and seal the parent stream. Verify all changes have been applied after
        // a call to commit().
        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppend());
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

        long expectedLength = SEGMENT_LENGTH + appendCount * DEFAULT_APPEND_DATA.length + SEALED_BATCH_LENGTH;
        SegmentMetadata parentMetadata = metadata.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertEquals("Unexpected DurableLogLength in metadata after commit.", expectedLength, parentMetadata.getDurableLogLength());
        Assert.assertTrue("Unexpected value for isSealed in metadata after commit.", parentMetadata.isSealed());
        SegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(SEALED_BATCH_ID);
        Assert.assertTrue("Unexpected value for isSealed in batch metadata after commit.", batchMetadata.isSealed());
        Assert.assertTrue("Unexpected value for isMerged in metadata after commit.", batchMetadata.isMerged());
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
        // Create a couple of operations, commit them, and then create a few more appends, merge a batch and seal the parent stream.
        // Then call rollback(); verify no changes have been applied after the call to commit().

        UpdateableContainerMetadata metadata = createMetadata();
        OperationMetadataUpdater updater = createUpdater(metadata);
        StreamSegmentAppendOperation committedAppend = createAppend();
        updater.preProcessOperation(committedAppend);
        updater.acceptOperation(committedAppend);
        updater.commit(); // This is the last extent of the modifications to the metadata.

        int appendCount = 500;
        ArrayList<StorageOperation> operations = new ArrayList<>();
        for (int i = 0; i < appendCount; i++) {
            operations.add(createAppend());
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
        SegmentMetadata batchMetadata = metadata.getStreamSegmentMetadata(SEALED_BATCH_ID);
        Assert.assertFalse("Unexpected value for isMerged in metadata after rollback.", batchMetadata.isMerged());

        // Now the updater
        parentMetadata = updater.getStreamSegmentMetadata(SEGMENT_ID);
        Assert.assertNull("Unexpected state of the updater after rollback.", parentMetadata);
        batchMetadata = updater.getStreamSegmentMetadata(SEALED_BATCH_ID);
        Assert.assertNull("Unexpected state of the updater after rollback.", batchMetadata);
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
        long previousMarker = -1;
        for (int i = 0; i < recordCount; i++) {
            long dfSeqNo = i * i;
            u.recordTruncationMarker(i, dfSeqNo);
            long actualMarker = metadata.getClosestTruncationMarker(i);
            Assert.assertEquals("Unexpected value for truncation marker (pre-commit) for Operation Sequence Number " + i, previousMarker, actualMarker);
            u.commit();
            actualMarker = metadata.getClosestTruncationMarker(i);
            Assert.assertEquals("Unexpected value for truncation marker (post-commit) for Operation Sequence Number " + i, dfSeqNo, actualMarker);
            previousMarker = actualMarker;
        }
    }

    private UpdateableContainerMetadata createBlankMetadata() {
        return new StreamSegmentContainerMetadata(CONTAINER_ID);
    }

    private UpdateableContainerMetadata createMetadata() {
        UpdateableContainerMetadata metadata = createBlankMetadata();
        metadata.mapStreamSegmentId(SEGMENT_NAME, SEGMENT_ID);
        metadata.getStreamSegmentMetadata(SEGMENT_ID).setDurableLogLength(SEGMENT_LENGTH);
        metadata.getStreamSegmentMetadata(SEGMENT_ID).setStorageLength(SEGMENT_LENGTH - 1); // Different from DurableLogOffset.

        metadata.mapStreamSegmentId(SEALED_BATCH_NAME, SEALED_BATCH_ID, SEGMENT_ID);
        metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).setDurableLogLength(SEALED_BATCH_LENGTH);
        metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).setStorageLength(SEALED_BATCH_LENGTH);
        metadata.getStreamSegmentMetadata(SEALED_BATCH_ID).markSealed();

        metadata.mapStreamSegmentId(NOTSEALED_BATCH_NAME, NOTSEALED_BATCH_ID, SEGMENT_ID);
        metadata.getStreamSegmentMetadata(NOTSEALED_BATCH_ID).setDurableLogLength(0);
        metadata.getStreamSegmentMetadata(NOTSEALED_BATCH_ID).setStorageLength(0);

        return metadata;
    }

    private OperationMetadataUpdater createUpdater(UpdateableContainerMetadata metadata) {
        return new OperationMetadataUpdater(metadata);
    }

    private StreamSegmentAppendOperation createAppend() {
        return new StreamSegmentAppendOperation(SEGMENT_ID, DEFAULT_APPEND_DATA, DEFAULT_APPEND_CONTEXT);
    }

    private StreamSegmentSealOperation createSeal() {
        return new StreamSegmentSealOperation(SEGMENT_ID);
    }

    private MergeBatchOperation createMerge() {
        return new MergeBatchOperation(SEGMENT_ID, SEALED_BATCH_ID);
    }

    private StreamSegmentMapOperation createMap() {
        return createMap(SEGMENT_NAME);
    }

    private StreamSegmentMapOperation createMap(String name) {
        return new StreamSegmentMapOperation(new StreamSegmentInformation(name, SEGMENT_LENGTH, true, false, new Date()));
    }

    private BatchMapOperation createBatchMap(long parentId) {
        return createBatchMap(parentId, SEALED_BATCH_NAME);
    }

    private BatchMapOperation createBatchMap(long parentId, String name) {
        return new BatchMapOperation(parentId, new StreamSegmentInformation(name, SEALED_BATCH_LENGTH, true, false, new Date()));
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
}
