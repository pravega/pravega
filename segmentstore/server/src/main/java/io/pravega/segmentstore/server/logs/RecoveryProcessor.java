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
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.Timer;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CheckpointOperationBase;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.LogAddress;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper class (for the DurableLog) that is used to execute the recovery process.
 */
@Slf4j
class RecoveryProcessor {
    //region Members

    private final UpdateableContainerMetadata metadata;
    private final DurableDataLog durableDataLog;
    private final MemoryStateUpdater stateUpdater;
    private final String traceObjectId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RecoveryProcessor class.
     *
     * @param metadata         The UpdateableContainerMetadata to use for recovery.
     * @param durableDataLog   The (uninitialized) DurableDataLog to read data from for recovery.
     * @param stateUpdater     A MemoryStateUpdater that can be used to apply the recovered operations.
     */
    RecoveryProcessor(UpdateableContainerMetadata metadata, DurableDataLog durableDataLog, MemoryStateUpdater stateUpdater) {
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        this.durableDataLog = Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        this.stateUpdater = Preconditions.checkNotNull(stateUpdater, "stateUpdater");
        this.traceObjectId = String.format("RecoveryProcessor[%s]", this.metadata.getContainerId());
    }

    //endregion

    //region Operations

    /**
     * Executes a DurableLog recovery using data from DurableDataLog. During this process, the following will happen:
     * 1. Metadata will be reset and put into recovery mode.
     * 2. DurableDataLog will be initialized. This will fail if the DurableDataLog has already been initialized.
     * 3. Reads the entire contents of the DurableDataLog, extracts Operations, and updates the Metadata and other
     * components (via MemoryStateUpdater) based on their contents.
     * 4. Metadata is taken out of recovery mode.
     *
     * @return The number of Operations recovered.
     * @throws Exception If an exception occurred. This could be one of the following:
     *                   * DataLogWriterNotPrimaryException: If unable to acquire DurableDataLog ownership or the ownership
     *                   has been lost in the process.
     *                   * DataCorruptionException: If an unrecoverable corruption has been detected with the recovered data.
     *                   * SerializationException: If a DataFrame or Operation was unable to be deserialized.
     *                   * IOException: If a general IO exception occurred.
     */
    public int performRecovery() throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "performRecovery");
        Timer timer = new Timer();
        log.info("{} Recovery started.", this.traceObjectId);

        // Put metadata (and entire container) into 'Recovery Mode'.
        this.metadata.enterRecoveryMode();

        // Reset metadata.
        this.metadata.reset();

        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(this.metadata);
        this.stateUpdater.enterRecoveryMode(metadataUpdater);

        boolean successfulRecovery = false;
        int recoveredItemCount;
        try {
            recoveredItemCount = recoverAllOperations(metadataUpdater);
            this.metadata.setContainerEpoch(this.durableDataLog.getEpoch());
            long timeElapsed = timer.getElapsedMillis();
            log.info("{} Recovery completed. Epoch = {}, Items Recovered = {}, Time = {}ms.", this.traceObjectId,
                    this.metadata.getContainerEpoch(), recoveredItemCount, timeElapsed);
            SegmentStoreMetrics.recoveryCompleted(timeElapsed, this.metadata.getContainerId());
            successfulRecovery = true;
        } finally {
            // We must exit recovery mode when done, regardless of outcome.
            this.metadata.exitRecoveryMode();
            this.stateUpdater.exitRecoveryMode(successfulRecovery);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "performRecovery", traceId);
        return recoveredItemCount;
    }

    /**
     * Recovers the Operations from the DurableLog using the given OperationMetadataUpdater. Searches the DurableDataLog
     * until the first MetadataCheckpointOperation is encountered. All Operations prior to this one are skipped over.
     * Recovery starts with the first MetadataCheckpointOperation and runs until the end of the DurableDataLog is reached.
     * Subsequent MetadataCheckpointOperations are ignored (as they contain redundant information - which has already
     * been built up using the Operations up to them).
     *
     * @param metadataUpdater The OperationMetadataUpdater to use for updates.
     * @return The number of Operations recovered.
     */
    private int recoverAllOperations(OperationMetadataUpdater metadataUpdater) throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "recoverAllOperations");
        int skippedOperationCount = 0;
        int skippedDataFramesCount = 0;
        int recoveredItemCount = 0;

        // Read all entries from the DataFrameLog and append them to the InMemoryOperationLog.
        // Also update metadata along the way.
        try (DataFrameReader<Operation> reader = new DataFrameReader<>(this.durableDataLog, OperationSerializer.DEFAULT, this.metadata.getContainerId())) {
            DataFrameRecord<Operation> dataFrameRecord;

            // We can only recover starting from a MetadataCheckpointOperation; find the first one.
            while (true) {
                // Fetch the next operation.
                dataFrameRecord = reader.getNext();
                if (dataFrameRecord == null) {
                    // We have reached the end and have not found any MetadataCheckpointOperations.
                    log.warn("{}: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading {} Operations and {} Data Frames.",
                            this.traceObjectId, skippedOperationCount, skippedDataFramesCount);
                    break;
                } else if (dataFrameRecord.getItem() instanceof MetadataCheckpointOperation) {
                    // We found a checkpoint. Start recovering from here.
                    log.info("{}: Starting recovery from Sequence Number {} (skipped {} Operations and {} Data Frames).",
                            this.traceObjectId, dataFrameRecord.getItem().getSequenceNumber(), skippedOperationCount, skippedDataFramesCount);
                    break;
                } else if (dataFrameRecord.isLastFrameEntry()) {
                    skippedDataFramesCount++;
                }

                skippedOperationCount++;
                log.debug("{}: Not recovering operation because no MetadataCheckpointOperation encountered so far ({}).",
                        this.traceObjectId, dataFrameRecord.getItem());
            }

            // Now continue with the recovery from here.
            while (dataFrameRecord != null) {
                recordTruncationMarker(dataFrameRecord);
                recoverOperation(dataFrameRecord, metadataUpdater);
                recoveredItemCount++;

                // Fetch the next operation.
                dataFrameRecord = reader.getNext();
            }
        }

        // Commit whatever changes we have in the metadata updater to the Container Metadata.
        // This code will only be invoked if we haven't encountered any exceptions during recovery.
        metadataUpdater.commitAll();
        LoggerHelpers.traceLeave(log, this.traceObjectId, "recoverAllOperations", traceId, recoveredItemCount);
        return recoveredItemCount;
    }

    protected void recoverOperation(DataFrameRecord<Operation> dataFrameRecord, OperationMetadataUpdater metadataUpdater) throws DataCorruptionException {
        // Update Metadata Sequence Number.
        Operation operation = dataFrameRecord.getItem();
        metadataUpdater.setOperationSequenceNumber(operation.getSequenceNumber());

        // Update the metadata with the information from the Operation.
        try {
            log.debug("{} Recovering {}.", this.traceObjectId, operation);
            metadataUpdater.preProcessOperation(operation);
            metadataUpdater.acceptOperation(operation);
        } catch (StreamSegmentException | ContainerException ex) {
            // Metadata update failures should not happen during recovery.
            throw new DataCorruptionException(String.format("Unable to update metadata for Log Operation '%s'.", operation), ex);
        }

        // Update in-memory structures.
        this.stateUpdater.process(operation);

        // Perform necessary read index cleanups if possible.
        if (operation instanceof CheckpointOperationBase) {
            this.stateUpdater.cleanupReadIndex();
        }
    }

    private void recordTruncationMarker(DataFrameRecord<Operation> dataFrameRecord) {
        // Truncation Markers are stored directly in the ContainerMetadata. There is no need for an OperationMetadataUpdater
        // to do this.
        // Determine and record Truncation Markers, but only if the current operation spans multiple DataFrames
        // or it's the last entry in a DataFrame.
        LogAddress lastFullAddress = dataFrameRecord.getLastFullDataFrameAddress();
        LogAddress lastUsedAddress = dataFrameRecord.getLastUsedDataFrameAddress();
        if (lastFullAddress != null && lastFullAddress.getSequence() != lastUsedAddress.getSequence()) {
            // This operation spans multiple DataFrames. The TruncationMarker should be set on the last DataFrame
            // that ends with a part of it.
            this.metadata.recordTruncationMarker(dataFrameRecord.getItem().getSequenceNumber(), lastFullAddress);
        } else if (dataFrameRecord.isLastFrameEntry()) {
            // The operation was the last one in the frame. This is a Truncation Marker.
            this.metadata.recordTruncationMarker(dataFrameRecord.getItem().getSequenceNumber(), lastUsedAddress);
        }
    }

    //endregion
}
