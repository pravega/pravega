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

import com.emc.logservice.common.EnhancedByteArrayOutputStream;
import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.AppendContext;
import com.emc.logservice.contracts.StreamSegmentException;
import com.emc.logservice.contracts.StreamSegmentMergedException;
import com.emc.logservice.contracts.StreamSegmentNotExistsException;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.server.ContainerMetadata;
import com.emc.logservice.server.SegmentMetadata;
import com.emc.logservice.server.SegmentMetadataCollection;
import com.emc.logservice.server.UpdateableContainerMetadata;
import com.emc.logservice.server.UpdateableSegmentMetadata;
import com.emc.logservice.server.containers.StreamSegmentMetadata;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.server.logs.operations.BatchMapOperation;
import com.emc.logservice.server.logs.operations.MergeBatchOperation;
import com.emc.logservice.server.logs.operations.MetadataCheckpointOperation;
import com.emc.logservice.server.logs.operations.MetadataOperation;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentMapOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentSealOperation;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * Transaction-based Metadata Updater for Log Operations.
 */
@Slf4j
public class OperationMetadataUpdater implements SegmentMetadataCollection {
    //region Members

    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private final TruncationMarkerCollection truncationMarkers;
    private UpdateTransaction currentTransaction;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationMetadataUpdater class.
     *
     * @param metadata The Container Metadata to update.
     * @throws NullPointerException If any of the arguments are null.
     */
    public OperationMetadataUpdater(UpdateableContainerMetadata metadata, TruncationMarkerCollection truncationMarkers) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(truncationMarkers, "truncationMarkers");

        this.traceObjectId = String.format("OperationMetadataUpdater[%s]", metadata.getContainerId());
        this.metadata = metadata;
        this.truncationMarkers = truncationMarkers;
        this.currentTransaction = null;
    }

    //endregion

    //region SegmentMetadataCollection Implementation

    @Override
    public SegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        UpdateTransaction transaction = this.currentTransaction;
        if (transaction == null) {
            return null;
        }

        try {
            return transaction.getStreamSegmentMetadata(streamSegmentId);
        } catch (MetadataUpdateException ex) {
            return null;
        }
    }

    @Override
    public long getStreamSegmentId(String streamSegmentName) {
        UpdateTransaction transaction = this.currentTransaction;
        if (transaction == null) {
            return ContainerMetadata.NO_STREAM_SEGMENT_ID;
        }

        return transaction.getExistingStreamSegmentId(streamSegmentName);
    }

    //endregion

    //region Processing

    /**
     * Commits all outstanding changes to the base Container Metadata.
     *
     * @return True if anything was committed, false otherwise.
     */
    public boolean commit() {
        log.trace("{}: Commit (Anything = {}).", this.traceObjectId, this.currentTransaction != null);
        if (this.currentTransaction == null) {
            return false;
        }

        this.currentTransaction.commit();
        this.currentTransaction = null;
        return true;
    }

    /**
     * Discards any outstanding changes.
     */
    public void rollback() {
        log.trace("{}: Rollback (Anything = {}).", this.traceObjectId, this.currentTransaction != null);
        this.currentTransaction = null;
    }

    /**
     * Records a Truncation Marker.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param dataFrameSequenceNumber The Sequence Number of the corresponding Data Frame that can be truncated (up to, and including).
     */
    public void recordTruncationMarker(long operationSequenceNumber, long dataFrameSequenceNumber) {
        log.debug("{}: RecordTruncationMarker OperationSequenceNumber = {}, DataFrameSequenceNumber = {}.", this.traceObjectId, operationSequenceNumber, dataFrameSequenceNumber);
        this.truncationMarkers.recordTruncationMarker(operationSequenceNumber, dataFrameSequenceNumber);
    }

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return
     */
    public long getNewOperationSequenceNumber() {
        return this.metadata.getNewOperationSequenceNumber();
    }

    /**
     * Phase 1/2 of processing a Operation.
     * <p/>
     * If the given operation is a StorageOperation, the Operation is validated against the base Container Metadata and
     * the pending transaction and it is updated accordingly (if needed).
     * <p/>
     * If the given operation is a MetadataCheckpointOperation, the current state of the metadata (including pending
     * transactions) is serialized to it.
     * <p/>
     * For all other kinds of MetadataOperations (i.e., StreamSegmentMapOperation, BatchMapOperation) this method only
     * does anything if the base Container Metadata is in Recovery Mode (in which case the given MetadataOperation) is
     * recorded in the pending transaction.
     *
     * @param operation The operation to pre-process.
     * @throws MetadataUpdateException         If the given operation was rejected given the current state of the metadata.
     * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked as deleted.
     * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously sealed and
     *                                         that is incompatible with a sealed stream.
     * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously merged.
     * @throws NullPointerException            If the operation is null.
     */
    public void preProcessOperation(Operation operation) throws MetadataUpdateException, StreamSegmentException {
        log.trace("{}: PreProcess {}.", this.traceObjectId, operation);
        getCurrentTransaction().preProcessOperation(operation);
    }

    /**
     * Phase 2/2 of processing an Operation. The Operation's effects are reflected in the pending transaction.
     * <p/>
     * This method only has an effect on StorageOperations. It does nothing for MetadataOperations, regardless of whether
     * the base Container Metadata is in Recovery Mode or not.
     *
     * @param operation The operation to accept.
     * @throws MetadataUpdateException If the given operation was rejected given the current state of the metadata.
     * @throws NullPointerException    If the operation is null.
     */
    public void acceptOperation(Operation operation) throws MetadataUpdateException {
        log.trace("{}: Accept {}.", this.traceObjectId, operation);
        getCurrentTransaction().acceptOperation(operation);
    }

    private UpdateTransaction getCurrentTransaction() {
        if (this.currentTransaction == null) {
            this.currentTransaction = new UpdateTransaction(this.metadata);
        }

        return this.currentTransaction;
    }

    //endregion

    //region UpdateTransaction

    /**
     * A Metadata Update Transaction. Keeps all pending changes, until they are ready to be committed to the base Container Metadata.
     */
    private static class UpdateTransaction {
        private static final byte CURRENT_SERIALIZATION_VERSION = 0;
        private final HashMap<Long, TemporaryStreamSegmentMetadata> streamSegmentUpdates;
        private final HashMap<Long, UpdateableSegmentMetadata> newStreamSegments;
        private final HashMap<String, Long> newStreamSegmentNames;
        private final UpdateableContainerMetadata containerMetadata;
        private boolean processedCheckpoint;

        /**
         * Creates a new instance of the UpdateTransaction class.
         *
         * @param containerMetadata The base Container Metadata.
         */
        public UpdateTransaction(UpdateableContainerMetadata containerMetadata) {
            assert containerMetadata != null : "containerMetadata is null";
            this.streamSegmentUpdates = new HashMap<>();
            this.containerMetadata = containerMetadata;
            if (containerMetadata.isRecoveryMode()) {
                this.newStreamSegments = new HashMap<>();
                this.newStreamSegmentNames = new HashMap<>();
            } else {
                this.newStreamSegments = null;
                this.newStreamSegmentNames = null;
            }
        }

        /**
         * Commits all pending changes to the base Container Metadata.
         */
        public void commit() {
            if (this.containerMetadata.isRecoveryMode()) {
                if (this.processedCheckpoint) {
                    // If we processed a checkpoint during recovery, we need to wipe the metadata clean. We are setting
                    // a brand new one.
                    assert this.streamSegmentUpdates.size() == 0 : "Not expecting StreamSegmentUpdates in recovery if we encountered a checkpoint.";
                    this.containerMetadata.reset();
                }
            }

            // Commit all temporary changes to their respective sources.
            this.streamSegmentUpdates.values().forEach(TemporaryStreamSegmentMetadata::apply);

            // If we are in recovery mode, append new stream metadata to the container metadata.
            if (this.containerMetadata.isRecoveryMode()) {
                // We must first copy the Standalone StreamSegments, and then the Batch StreamSegments. That's because
                // the Batch StreamSegments may refer to one of these newly created StreamSegments, and the metadata
                // will reject the operation if it can't find the parent.
                // We need this because HashMap does not necessarily preserve order when iterating via values().
                copySegmentMetadataToSource(newStreamSegments.values(), s -> s.getParentId() == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID);
                copySegmentMetadataToSource(newStreamSegments.values(), s -> s.getParentId() != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID);
            }

            // We are done. Clear the transaction.
            rollback();
        }

        /**
         * Gets all pending changes for the given StreamSegment.
         *
         * @param streamSegmentId
         * @return The result
         * @throws MetadataUpdateException If no metadata entry exists for the given StreamSegment Id.
         */
        public TemporaryStreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) throws MetadataUpdateException {
            TemporaryStreamSegmentMetadata tsm = this.streamSegmentUpdates.getOrDefault(streamSegmentId, null);
            if (tsm == null) {
                UpdateableSegmentMetadata streamSegmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
                if (streamSegmentMetadata == null) {
                    if (this.newStreamSegments != null) {
                        streamSegmentMetadata = this.newStreamSegments.getOrDefault(streamSegmentId, null);
                    }

                    if (streamSegmentMetadata == null) {
                        throw new MetadataUpdateException(String.format("No metadata entry exists for StreamSegment Id %d.", streamSegmentId));
                    }
                }

                tsm = new TemporaryStreamSegmentMetadata(streamSegmentMetadata, this.containerMetadata.isRecoveryMode());
                this.streamSegmentUpdates.put(streamSegmentId, tsm);
            }

            return tsm;
        }

        /**
         * Pre-processes the given Operation. See OperationMetadataUpdater.preProcessOperation for more details on behavior.
         *
         * @param operation The operation to pre-process.
         * @throws MetadataUpdateException         If the given operation was rejected given the current state of the metadata.
         * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked as deleted.
         * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously sealed and
         *                                         that is incompatible with a sealed stream.
         * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously merged.
         * @throws NullPointerException            If the operation is null.
         */
        public void preProcessOperation(Operation operation) throws MetadataUpdateException, StreamSegmentException {
            if (operation instanceof StorageOperation) {
                TemporaryStreamSegmentMetadata streamMetadata = getStreamSegmentMetadata(((StorageOperation) operation).getStreamSegmentId());
                if (streamMetadata.isDeleted()) {
                    throw new StreamSegmentNotExistsException(streamMetadata.getName());
                }

                if (operation instanceof StreamSegmentAppendOperation) {
                    streamMetadata.preProcessOperation((StreamSegmentAppendOperation) operation);
                } else if (operation instanceof StreamSegmentSealOperation) {
                    streamMetadata.preProcessOperation((StreamSegmentSealOperation) operation);
                } else if (operation instanceof MergeBatchOperation) {
                    MergeBatchOperation mbe = (MergeBatchOperation) operation;
                    TemporaryStreamSegmentMetadata batchStreamMetadata = getStreamSegmentMetadata(mbe.getBatchStreamSegmentId());
                    batchStreamMetadata.preProcessAsBatchStreamSegment(mbe);
                    streamMetadata.preProcessAsParentStreamSegment(mbe, batchStreamMetadata);
                }
            } else if (operation instanceof MetadataOperation) {
                // MetadataOperations do not require preProcess and accept; they can be handled in a single stage.
                // Note that only some of these operations actually do any work in non-recovery mode
                if (operation instanceof StreamSegmentMapOperation) {
                    processMetadataOperation((StreamSegmentMapOperation) operation);
                } else if (operation instanceof BatchMapOperation) {
                    processMetadataOperation((BatchMapOperation) operation);
                } else if (operation instanceof MetadataCheckpointOperation) {
                    processMetadataOperation((MetadataCheckpointOperation) operation);
                }
            }
        }

        /**
         * Accepts the given Operation. The Operation's effects are reflected in the pending transaction.
         * This method has no effect on Metadata Operations.
         * See OperationMetadataUpdater.acceptOperation for more details on behavior.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException If the given operation was rejected given the current state of the metadata.
         * @throws NullPointerException    If the operation is null.
         */
        public void acceptOperation(Operation operation) throws MetadataUpdateException {
            if (!(operation instanceof StorageOperation)) {
                // Accept only makes sense for StorageOperations.
                return;
            }

            TemporaryStreamSegmentMetadata streamMetadata = getStreamSegmentMetadata(((StorageOperation) operation).getStreamSegmentId());
            if (operation instanceof StreamSegmentAppendOperation) {
                streamMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
            } else if (operation instanceof StreamSegmentSealOperation) {
                streamMetadata.acceptOperation((StreamSegmentSealOperation) operation);
            } else if (operation instanceof MergeBatchOperation) {
                MergeBatchOperation mbe = (MergeBatchOperation) operation;
                TemporaryStreamSegmentMetadata batchStreamMetadata = getStreamSegmentMetadata(mbe.getBatchStreamSegmentId());
                batchStreamMetadata.acceptAsBatchStreamSegment(mbe);
                streamMetadata.acceptAsParentStreamSegment(mbe, batchStreamMetadata);
            }
        }

        private void copySegmentMetadataToSource(Collection<UpdateableSegmentMetadata> newStreams, Predicate<SegmentMetadata> filter) {
            for (SegmentMetadata newMetadata : newStreams) {
                if (!filter.test(newMetadata)) {
                    continue;
                }

                //TODO: should we check (again?) if the container metadata has knowledge of this stream?
                if (newMetadata.getParentId() != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                    this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId(), newMetadata.getParentId());
                } else {
                    this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId());
                }

                // Update real metadata with all the information from the new metadata.
                UpdateableSegmentMetadata existingMetadata = this.containerMetadata.getStreamSegmentMetadata(newMetadata.getId());
                existingMetadata.copyFrom(newMetadata);
            }
        }

        private void processMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
            if (!this.containerMetadata.isRecoveryMode()) {
                // Nothing to do in non-recovery mode.
                return;
            }

            // Verify Stream does not exist.
            UpdateableSegmentMetadata streamSegmentMetadata = getExistingMetadata(operation.getStreamSegmentId());
            if (streamSegmentMetadata != null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Stream Id that is already mapped in the metadata. Entry: %d->'%s', Metadata: %d->'%s'.", operation.getSequenceNumber(), operation.getStreamSegmentId(), operation.getStreamSegmentName(), streamSegmentMetadata.getId(), streamSegmentMetadata.getName()));
            }

            // Verify Stream Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamSegmentId(operation.getStreamSegmentName());
            if (existingStreamId != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Stream Name that is already mapped in the metadata. Stream Name = '%s', Existing Id = %d, New Id = %d.", operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamId, operation.getStreamSegmentId()));
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            streamSegmentMetadata = recordNewStreamSegment(operation.getStreamSegmentName(), operation.getStreamSegmentId(), NO_STREAM_SEGMENT_ID);
            streamSegmentMetadata.setStorageLength(operation.getStreamSegmentLength());
            streamSegmentMetadata.setDurableLogLength(0);
            if (operation.isSealed()) {
                streamSegmentMetadata.markSealed();
            }
        }

        private void processMetadataOperation(BatchMapOperation operation) throws MetadataUpdateException {
            if (!this.containerMetadata.isRecoveryMode()) {
                // Nothing to do in non-recovery mode.
                return;
            }

            // Verify Parent Stream Exists.
            UpdateableSegmentMetadata parentMetadata = getExistingMetadata(operation.getParentStreamSegmentId());
            if (parentMetadata == null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a StreamSegment to a Parent StreamSegment Id that does not exist. Parent StreamSegmentId = %d, Batch StreamSegmentId = %d, Batch Stream Name = %s.", operation.getSequenceNumber(), operation.getParentStreamSegmentId(), operation.getBatchStreamSegmentId(), operation.getBatchStreamSegmentName()));
            }

            // Verify Batch Stream does not exist.
            UpdateableSegmentMetadata batchStreamSegmentMetadata = getExistingMetadata(operation.getBatchStreamSegmentId());
            if (batchStreamSegmentMetadata != null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Batch StreamSegmentId that is already mapped in the metadata. Entry: %d->'%s', Metadata: %d->'%s'.", operation.getSequenceNumber(), operation.getBatchStreamSegmentId(), operation.getBatchStreamSegmentName(), batchStreamSegmentMetadata.getId(), batchStreamSegmentMetadata.getName()));
            }

            // Verify Stream Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamSegmentId(operation.getBatchStreamSegmentName());
            if (existingStreamId != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Batch StreamSegment Name that is already mapped in the metadata. StreamSegmentName = '%s', Existing Id = %d, New Id = %d.", operation.getSequenceNumber(), operation.getBatchStreamSegmentName(), existingStreamId, operation.getBatchStreamSegmentId()));
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            batchStreamSegmentMetadata = recordNewStreamSegment(operation.getBatchStreamSegmentName(), operation.getBatchStreamSegmentId(), operation.getParentStreamSegmentId());
            batchStreamSegmentMetadata.setStorageLength(operation.getBatchStreamSegmentLength());
            batchStreamSegmentMetadata.setDurableLogLength(0);
            if (operation.isBatchSealed()) {
                batchStreamSegmentMetadata.markSealed();
            }
        }

        private void processMetadataOperation(MetadataCheckpointOperation operation) throws MetadataUpdateException {
            try {
                if (containerMetadata.isRecoveryMode()) {
                    // In Recovery Mode, a MetadataCheckpointOperation means the entire, up-to-date state of the
                    // metadata is serialized in this operation. We need to discard whatever we have accumulated so far
                    // and rebuild the metadata from the information we have so far.
                    rollback();
                    deserializeFrom(operation);
                } else {
                    // In non-Recovery Mode, a MetadataCheckpointOperation means we need to serialize the current state of
                    // the Metadata, both the base Container Metadata and the current Transaction.
                    serializeTo(operation);
                }
            } catch (IOException | SerializationException ex) {
                throw new MetadataUpdateException("Unable to process MetadataCheckpointOperation " + operation, ex);
            }
        }

        private void rollback() {
            this.streamSegmentUpdates.clear();
            if (this.newStreamSegments != null) {
                this.newStreamSegments.clear();
            }

            if (this.newStreamSegmentNames != null) {
                this.newStreamSegmentNames.clear();
            }

            this.processedCheckpoint = false;
        }

        private long getExistingStreamSegmentId(String streamSegmentName) {
            long existingStreamId = this.containerMetadata.getStreamSegmentId(streamSegmentName);
            if (existingStreamId == SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                existingStreamId = this.newStreamSegmentNames.getOrDefault(streamSegmentName, SegmentMetadataCollection.NO_STREAM_SEGMENT_ID);
            }

            return existingStreamId;
        }

        private UpdateableSegmentMetadata getExistingMetadata(long streamSegmentId) {
            UpdateableSegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            if (sm == null) {
                sm = this.newStreamSegments.getOrDefault(streamSegmentId, null);
            }

            return sm;
        }

        private UpdateableSegmentMetadata recordNewStreamSegment(String streamSegmentName, long streamSegmentId, long parentId) {
            UpdateableSegmentMetadata metadata;
            if (parentId == NO_STREAM_SEGMENT_ID) {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId);
            } else {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentId);
            }

            this.newStreamSegments.put(metadata.getId(), metadata);
            this.newStreamSegmentNames.put(metadata.getName(), metadata.getId());

            return metadata;
        }

        /**
         * Deserializes the Metadata from the given stream.
         *
         * @param operation The MetadataCheckpointOperation to deserialize from..
         * @throws IOException            If the stream threw one.
         * @throws SerializationException If the given Stream is an invalid metadata serialization.
         * @throws IllegalStateException  If the Metadata is not in Recovery Mode.
         */
        private void deserializeFrom(MetadataCheckpointOperation operation) throws IOException, SerializationException {
            Preconditions.checkState(this.containerMetadata.isRecoveryMode(), "Cannot deserialize Metadata in recovery mode.");

            DataInputStream stream = new DataInputStream(operation.getContents().getReader());

            // 1. Version.
            byte version = stream.readByte();
            if (version != CURRENT_SERIALIZATION_VERSION) {
                throw new SerializationException("Metadata.deserialize", String.format("Unsupported version: %d.", version));
            }

            // 2. Container id.
            String containerId = stream.readUTF();
            if (!this.containerMetadata.getContainerId().equals(containerId)) {
                throw new SerializationException("Metadata.deserialize", String.format("Invalid StreamSegmentContainerId. Expected '%s', actual '%s'.", this.containerMetadata.getContainerId(), containerId));
            }

            // This is not retrieved from serialization, but rather from the operation itself.
            this.containerMetadata.setOperationSequenceNumber(operation.getSequenceNumber());

            // 3. Segment Count.
            int segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeSegmentMetadata(stream);
            }
        }

        private void serializeTo(MetadataCheckpointOperation operation) throws IOException {
            assert operation != null : "operation is null";
            Preconditions.checkState(!this.containerMetadata.isRecoveryMode(), "Cannot serialize Metadata in recovery mode.");
            assert this.newStreamSegments == null : "newStreamSegments != null but we are not in recovery mode.";

            EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(byteStream);

            // 1. Version.
            stream.writeByte(CURRENT_SERIALIZATION_VERSION);

            // 2. Container Id.
            stream.writeUTF(this.containerMetadata.getContainerId());

            // Intentionally skipping over the Sequence Number. There is no need for that here; it will be set on the
            // operation anyway when it gets serialized.

            // Total Segment count is base segment count (updates are a subset of base segments and we cannot have new
            // segments in non-recovery mode).
            // 3. SegmentCount
            Collection<Long> baseStreamSegmentIds = this.containerMetadata.getAllStreamSegmentIds();
            int segmentCount = baseStreamSegmentIds.size();
            stream.writeInt(segmentCount);

            // Serialize unchanged StreamSegment Metadata.
            for (long segmentId : baseStreamSegmentIds) {
                if (!this.streamSegmentUpdates.containsKey(segmentId)) {
                    SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(segmentId);
                    serializeSegmentMetadata(sm, stream);
                }
            }

            // Serialize StreamSegment Metadata that changed.
            for (SegmentMetadata sm : this.streamSegmentUpdates.values()) {
                serializeSegmentMetadata(sm, stream);
            }

            operation.setContents(byteStream.getData());
        }

        private void serializeSegmentMetadata(SegmentMetadata sm, DataOutputStream stream) throws IOException {
            // S1. StreamSegmentId.
            stream.writeLong(sm.getId());
            // S2. ParentId.
            stream.writeLong(sm.getParentId());
            // S3. Name.
            stream.writeUTF(sm.getName());
            // S4. DurableLogLength.
            stream.writeLong(sm.getDurableLogLength());
            // S5. StorageLength.
            stream.writeLong(sm.getStorageLength());
            // S6. Merged.
            stream.writeBoolean(sm.isMerged());
            // S7. Sealed.
            stream.writeBoolean(sm.isSealed());
            // S8. Deleted.
            stream.writeBoolean(sm.isDeleted());
            // S9. LastModified.
            stream.writeLong(sm.getLastModified().getTime());

            // TODO: determine if we want to snapshot the client ids and their offsets too. This might be a long list, especially if we don't clean it up.
            //sm.getKnownClientIds(); // TODO: if we do this, we also have to read them upon deserialization.
        }

        private void deserializeSegmentMetadata(DataInputStream stream) throws IOException {
            // S1. StreamSegmentId.
            long segmentId = stream.readLong();
            // S2. ParentId.
            long parentId = stream.readLong();
            // S3. Name.
            String name = stream.readUTF();

            UpdateableSegmentMetadata metadata = recordNewStreamSegment(name, segmentId, parentId);

            // S4. DurableLogLength.
            metadata.setDurableLogLength(stream.readLong());
            // S5. StorageLength.
            metadata.setStorageLength(stream.readLong());
            // S6. Merged.
            boolean isMerged = stream.readBoolean();
            if (isMerged) {
                metadata.markMerged();
            }
            // S7. Sealed.
            boolean isSealed = stream.readBoolean();
            if (isSealed) {
                metadata.markSealed();
            }
            // S8. Deleted.
            boolean isDeleted = stream.readBoolean();
            if (isDeleted) {
                metadata.markDeleted();
            }
            // S9. LastModified.
            Date lastModified = new java.util.Date(stream.readLong());
            metadata.setLastModified(lastModified);
        }
    }

    //endregion

    //region TemporaryStreamSegmentMetadata

    /**
     * Pending StreamSegment Metadata.
     */
    private static class TemporaryStreamSegmentMetadata implements SegmentMetadata {
        //region Members

        private final UpdateableSegmentMetadata streamSegmentMetadata;
        private final boolean isRecoveryMode;
        private final AbstractMap<UUID, AppendContext> lastCommittedAppends;
        private long currentDurableLogLength;
        private boolean sealed;
        private boolean merged;
        private boolean deleted;
        private int changeCount;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the TemporaryStreamSegmentMetadata class.
         *
         * @param streamSegmentMetadata The base StreamSegment Metadata.
         * @param isRecoveryMode        Whether the metadata is currently in recovery model
         */
        public TemporaryStreamSegmentMetadata(UpdateableSegmentMetadata streamSegmentMetadata, boolean isRecoveryMode) {
            assert streamSegmentMetadata != null : "streamSegmentMetadata is null";
            this.streamSegmentMetadata = streamSegmentMetadata;
            this.isRecoveryMode = isRecoveryMode;
            this.currentDurableLogLength = this.streamSegmentMetadata.getDurableLogLength();
            this.sealed = this.streamSegmentMetadata.isSealed();
            this.merged = this.streamSegmentMetadata.isMerged();
            this.deleted = this.streamSegmentMetadata.isDeleted();
            this.changeCount = 0;
            this.lastCommittedAppends = new HashMap<>();
        }

        //endregion

        //region StreamProperties Implementation

        @Override
        public String getName() {
            return this.streamSegmentMetadata.getName();
        }

        @Override
        public boolean isSealed() {
            return this.sealed;
        }

        @Override
        public boolean isDeleted() {
            return this.deleted;
        }

        @Override
        public long getLength() {
            return this.currentDurableLogLength; // ReadableLength == DurableLogLength.
        }

        @Override
        public Date getLastModified() {
            return new Date(); //TODO: implement properly.
        }

        //endregion

        //region SegmentMetadata Implementation

        @Override
        public long getId() {
            return this.streamSegmentMetadata.getId();
        }

        @Override
        public long getParentId() {
            return this.streamSegmentMetadata.getParentId();
        }

        @Override
        public boolean isMerged() {
            return this.merged;
        }

        @Override
        public long getStorageLength() {
            return this.streamSegmentMetadata.getStorageLength();
        }

        @Override
        public long getDurableLogLength() {
            return this.currentDurableLogLength;
        }

        @Override
        public AppendContext getLastAppendContext(UUID clientId) {
            AppendContext result = this.lastCommittedAppends.getOrDefault(clientId, null);
            return result != null ? result : this.streamSegmentMetadata.getLastAppendContext(clientId);
        }

        @Override
        public Collection<UUID> getKnownClientIds() {
            return this.lastCommittedAppends.keySet();
        }

        //endregion

        //region StreamSegmentAppendOperation

        /**
         * Pre-processes a StreamSegmentAppendOperation.
         * After this method returns, the given operation will have its StreamSegmentOffset property set to the current StreamSegmentLength.
         *
         * @param operation The operation to pre-process.
         * @throws StreamSegmentSealedException If the StreamSegment is sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        public void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException {
            ensureStreamId(operation);
            if (this.merged) {
                // We do not allow any operation after merging (since after merging the StreamSegment disappears).
                throw new StreamSegmentMergedException(this.streamSegmentMetadata.getName());
            }

            if (this.sealed) {
                throw new StreamSegmentSealedException(this.streamSegmentMetadata.getName());
            }

            if (!isRecoveryMode) {
                // Assign entry offset and update stream offset afterwards.
                operation.setStreamSegmentOffset(this.currentDurableLogLength);
            }
        }

        /**
         * Accepts a StreamSegmentAppendOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation StreamSegmentOffset is different from the current StreamSegment Length.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        public void acceptOperation(StreamSegmentAppendOperation operation) throws MetadataUpdateException {
            ensureStreamId(operation);
            if (operation.getStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(String.format("StreamSegmentAppendOperation offset mismatch. Expected %d, actual %d.", this.currentDurableLogLength, operation.getStreamSegmentOffset()));
            }

            this.currentDurableLogLength += operation.getData().length;
            this.lastCommittedAppends.put(operation.getAppendContext().getClientId(), operation.getAppendContext());
            this.changeCount++;
        }

        //endregion

        //region StreamSegmentSealOperation

        /**
         * Pre-processes a StreamSegmentSealOperation.
         * After this method returns, the operation will have its StreamSegmentLength property set to the current length of the StreamSegment.
         *
         * @param operation The Operation.
         * @throws StreamSegmentSealedException If the StreamSegment is already sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        public void preProcessOperation(StreamSegmentSealOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException {
            ensureStreamId(operation);
            if (this.merged) {
                // We do not allow any operation after merging (since after merging the Stream disappears).
                throw new StreamSegmentMergedException(this.streamSegmentMetadata.getName());
            }

            if (this.sealed) {
                // We do not allow re-sealing an already sealed stream.
                throw new StreamSegmentSealedException(this.streamSegmentMetadata.getName());
            }

            if (!this.isRecoveryMode) {
                // Assign entry Stream Length.
                operation.setStreamSegmentLength(this.currentDurableLogLength);
            }
        }

        /**
         * Accepts a StreamSegmentSealOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation hasn't been pre-processed.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        public void acceptOperation(StreamSegmentSealOperation operation) throws MetadataUpdateException {
            ensureStreamId(operation);
            if (operation.getStreamSegmentLength() < 0) {
                throw new MetadataUpdateException("StreamSegmentSealOperation cannot be accepted if it hasn't been pre-processed.");
            }

            this.sealed = true;
            this.changeCount++;
        }

        //endregion

        //region MergeBatchOperation

        /**
         * Pre-processes the given MergeBatchOperation as a Parent StreamSegment.
         * After this method returns, the operation will have its TargetStreamSegmentOffset set to the length of the Parent StreamSegment.
         *
         * @param operation           The operation to pre-process.
         * @param batchStreamMetadata The metadata for the Batch Stream Segment to merge.
         * @throws StreamSegmentSealedException If the parent stream is already sealed.
         * @throws MetadataUpdateException      If the operation cannot be processed because of the current state of the metadata.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        public void preProcessAsParentStreamSegment(MergeBatchOperation operation, TemporaryStreamSegmentMetadata batchStreamMetadata) throws StreamSegmentSealedException, MetadataUpdateException {
            ensureStreamId(operation);

            if (this.sealed) {
                // We do not allow merging into sealed streams.
                throw new StreamSegmentSealedException(this.streamSegmentMetadata.getName());
            }

            if (this.streamSegmentMetadata.getParentId() != SegmentMetadataCollection.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException("Cannot merge a StreamSegment into a Batch StreamSegment.");
            }

            // Check that the batch has been properly sealed and has its length set.
            if (!batchStreamMetadata.isSealed()) {
                throw new MetadataUpdateException("Batch StreamSegment to be merged needs to be sealed.");
            }

            long batchLength = operation.getBatchStreamSegmentLength();
            if (batchLength < 0) {
                throw new MetadataUpdateException("MergeBatchOperation does not have its Batch StreamSegment Length set.");
            }

            if (!this.isRecoveryMode) {
                // Assign entry Stream offset and update stream offset afterwards.
                operation.setTargetStreamSegmentOffset(this.currentDurableLogLength);
            }
        }

        /**
         * Accepts the given MergeBatchOperation as a Parent StreamSegment.
         *
         * @param operation           The operation to accept.
         * @param batchStreamMetadata The metadata for the Batch Stream Segment to merge.
         * @throws MetadataUpdateException  If the operation cannot be processed because of the current state of the metadata.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        public void acceptAsParentStreamSegment(MergeBatchOperation operation, TemporaryStreamSegmentMetadata batchStreamMetadata) throws MetadataUpdateException {
            ensureStreamId(operation);

            if (operation.getTargetStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(String.format("MergeBatchOperation target offset mismatch. Expected %d, actual %d.", this.currentDurableLogLength, operation.getTargetStreamSegmentOffset()));
            }

            long batchLength = operation.getBatchStreamSegmentLength();
            if (batchLength < 0 || batchLength != batchStreamMetadata.currentDurableLogLength) {
                throw new MetadataUpdateException("MergeBatchOperation does not seem to have been pre-processed.");
            }

            this.currentDurableLogLength += batchLength;
            this.changeCount++;
        }

        /**
         * Pre-processes the given operation as a Batch StreamSegment.
         *
         * @param operation The operation
         * @throws IllegalArgumentException     If the operation is for a different stream segment.
         * @throws MetadataUpdateException      If the StreamSegment is not sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is already merged.
         */
        public void preProcessAsBatchStreamSegment(MergeBatchOperation operation) throws MetadataUpdateException, StreamSegmentMergedException {
            Exceptions.checkArgument(this.streamSegmentMetadata.getId() == operation.getBatchStreamSegmentId(), "operation", "Invalid Operation BatchStreamSegment Id.");

            if (this.merged) {
                throw new StreamSegmentMergedException(this.streamSegmentMetadata.getName());
            }

            if (!this.sealed) {
                throw new MetadataUpdateException("Batch StreamSegment to be merged needs to be sealed.");
            }

            if (!this.isRecoveryMode) {
                operation.setBatchStreamSegmentLength(this.currentDurableLogLength);
            }
        }

        /**
         * Accepts the given operation as a Batch Stream Segment.
         *
         * @param operation The operation
         * @throws IllegalArgumentException If the operation is for a different stream segment.
         */
        public void acceptAsBatchStreamSegment(MergeBatchOperation operation) {
            Exceptions.checkArgument(this.streamSegmentMetadata.getId() == operation.getBatchStreamSegmentId(), "operation", "Invalid Operation BatchStreamSegment Id.");

            this.sealed = true;
            this.merged = true;
            this.changeCount++;
        }

        //endregion

        //region Operations

        /**
         * Applies all the outstanding changes to the base StreamSegmentMetadata object.
         */
        public void apply() {
            if (this.changeCount <= 0) {
                // No changes made.
                return;
            }

            // Apply to base metadata.
            this.lastCommittedAppends.values().forEach(this.streamSegmentMetadata::recordAppendContext);
            this.streamSegmentMetadata.setDurableLogLength(this.currentDurableLogLength);
            if (this.isSealed()) {
                this.streamSegmentMetadata.markSealed();
            }
            if (this.isMerged()) {
                this.streamSegmentMetadata.markMerged();
            }
        }

        private void ensureStreamId(StorageOperation operation) {
            Exceptions.checkArgument(this.streamSegmentMetadata.getId() == operation.getStreamSegmentId(), "operation", "Invalid Log Operation StreamSegment Id.");
        }

        //endregion
    }

    //endregion
}
