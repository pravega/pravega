/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ImmutableDate;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.AttributeUpdateType;
import io.pravega.service.contracts.Attributes;
import io.pravega.service.contracts.BadAttributeUpdateException;
import io.pravega.service.contracts.BadOffsetException;
import io.pravega.service.contracts.ContainerException;
import io.pravega.service.contracts.StreamSegmentException;
import io.pravega.service.contracts.StreamSegmentMergedException;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.contracts.StreamSegmentSealedException;
import io.pravega.service.contracts.TooManyActiveSegmentsException;
import io.pravega.service.server.AttributeSerializer;
import io.pravega.service.server.ContainerMetadata;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.UpdateableContainerMetadata;
import io.pravega.service.server.UpdateableSegmentMetadata;
import io.pravega.service.server.containers.StreamSegmentMetadata;
import io.pravega.service.server.logs.operations.MergeTransactionOperation;
import io.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.service.server.logs.operations.MetadataOperation;
import io.pravega.service.server.logs.operations.Operation;
import io.pravega.service.server.logs.operations.SegmentOperation;
import io.pravega.service.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.service.server.logs.operations.StorageOperation;
import io.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.service.server.logs.operations.TransactionMapOperation;
import io.pravega.service.server.logs.operations.UpdateAttributesOperation;
import io.pravega.service.storage.LogAddress;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Transaction-based Metadata Updater for Log Operations.
 */
@Slf4j
class OperationMetadataUpdater implements ContainerMetadata {
    //region Members

    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private UpdateTransaction currentTransaction;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationMetadataUpdater class.
     *
     * @param metadata The Container Metadata to update.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationMetadataUpdater(UpdateableContainerMetadata metadata) {
        Preconditions.checkNotNull(metadata, "metadata");

        this.traceObjectId = String.format("OperationMetadataUpdater[%d]", metadata.getContainerId());
        this.metadata = metadata;
        this.currentTransaction = null;
    }

    //endregion

    //region ContainerMetadata Implementation

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
    public long getStreamSegmentId(String streamSegmentName, boolean updateLastUsed) {
        // We ignore the 'updateLastUsed' argument here since this is an internal call, and there is no need to update the metadata stats.
        UpdateTransaction transaction = this.currentTransaction;
        if (transaction == null) {
            return ContainerMetadata.NO_STREAM_SEGMENT_ID;
        }

        return transaction.getExistingStreamSegmentId(streamSegmentName);
    }

    @Override
    public int getContainerId() {
        return this.metadata.getContainerId();
    }

    @Override
    public long getContainerEpoch() {
        return this.metadata.getContainerEpoch();
    }

    @Override
    public boolean isRecoveryMode() {
        return this.metadata.isRecoveryMode();
    }

    @Override
    public long getOperationSequenceNumber() {
        return this.metadata.getOperationSequenceNumber();
    }

    //endregion

    //region Processing

    /**
     * Commits all outstanding changes to the base Container Metadata.
     *
     * @return True if anything was committed, false otherwise.
     */
    boolean commit() {
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
    void rollback() {
        log.trace("{}: Rollback (Anything = {}).", this.traceObjectId, this.currentTransaction != null);
        this.currentTransaction = null;
    }

    /**
     * Records a Truncation Marker.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param logAddress              The Address of the corresponding Data Frame that can be truncated (up to, and including).
     */
    void recordTruncationMarker(long operationSequenceNumber, LogAddress logAddress) {
        log.debug("{}: RecordTruncationMarker OperationSequenceNumber = {}, DataFrameAddress = {}.", this.traceObjectId, operationSequenceNumber, logAddress);
        getCurrentTransaction().recordTruncationMarker(operationSequenceNumber, logAddress);
    }

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     */
    long nextOperationSequenceNumber() {
        Preconditions.checkState(!isRecoveryMode(), "Cannot request new Operation Sequence Number in Recovery Mode.");
        return this.metadata.nextOperationSequenceNumber();
    }

    /**
     * Sets the operation sequence number in the transaction.
     */
    void setOperationSequenceNumber(long value) {
        Preconditions.checkState(this.isRecoveryMode(), "Can only set new Operation Sequence Number in Recovery Mode.");
        getCurrentTransaction().setOperationSequenceNumber(value);
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
     * For all other kinds of MetadataOperations (i.e., StreamSegmentMapOperation, TransactionMapOperation) this method only
     * does anything if the base Container Metadata is in Recovery Mode (in which case the given MetadataOperation) is
     * recorded in the pending transaction.
     *
     * @param operation The operation to pre-process.
     * @throws ContainerException              If the given operation was rejected given the current state of the container metadata.
     * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked as deleted.
     * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously sealed and
     *                                         that is incompatible with a sealed stream.
     * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously merged.
     * @throws NullPointerException            If the operation is null.
     */
    void preProcessOperation(Operation operation) throws ContainerException, StreamSegmentException {
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
    void acceptOperation(Operation operation) throws MetadataUpdateException {
        log.trace("{}: Accept {}.", this.traceObjectId, operation);
        getCurrentTransaction().acceptOperation(operation);
    }

    private UpdateTransaction getCurrentTransaction() {
        if (this.currentTransaction == null) {
            this.currentTransaction = new UpdateTransaction(this.metadata, this.traceObjectId);
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
        private final List<Long> newTruncationPoints;
        private final HashMap<Long, LogAddress> newTruncationMarkers;
        private final UpdateableContainerMetadata containerMetadata;
        private final AtomicLong newSequenceNumber;
        private final String traceObjectId;
        private boolean processedCheckpoint;

        /**
         * Creates a new instance of the UpdateTransaction class.
         *
         * @param containerMetadata The base Container Metadata.
         */
        UpdateTransaction(UpdateableContainerMetadata containerMetadata, String traceObjectId) {
            assert containerMetadata != null : "containerMetadata is null";
            this.traceObjectId = traceObjectId;
            this.streamSegmentUpdates = new HashMap<>();
            this.containerMetadata = containerMetadata;
            this.newTruncationMarkers = new HashMap<>();
            this.newTruncationPoints = new ArrayList<>();
            this.newStreamSegments = new HashMap<>();
            this.newStreamSegmentNames = new HashMap<>();
            if (containerMetadata.isRecoveryMode()) {
                this.newSequenceNumber = new AtomicLong(ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER);
            } else {
                this.newSequenceNumber = null;
            }
        }

        /**
         * Commits all pending changes to the base Container Metadata.
         */
        void commit() {
            if (this.containerMetadata.isRecoveryMode()) {
                if (this.processedCheckpoint) {
                    // If we processed a checkpoint during recovery, we need to wipe the metadata clean. We are setting
                    // a brand new one.
                    this.containerMetadata.reset();
                }

                // Reset cleaned up the Operation Sequence number. We need to reset it to whatever we have in our transaction.
                // If we have nothing, we'll just set it to 0, which is what the default value was in the metadata too.
                this.containerMetadata.setOperationSequenceNumber(this.newSequenceNumber.get());
            }

            // Commit all temporary changes to their respective sources.
            this.streamSegmentUpdates.values().forEach(TemporaryStreamSegmentMetadata::apply);

            // We must first copy the Standalone StreamSegments, and then the Transaction StreamSegments. That's because
            // the Transaction StreamSegments may refer to one of these newly created StreamSegments, and the metadata
            // will reject the operation if it can't find the parent.
            // We need this because HashMap does not necessarily preserve order when iterating via values().
            copySegmentMetadataToSource(newStreamSegments.values(), s -> s.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID);
            copySegmentMetadataToSource(newStreamSegments.values(), s -> s.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID);

            // Copy truncation markers.
            this.newTruncationMarkers.entrySet().forEach(e -> this.containerMetadata.recordTruncationMarker(e.getKey(), e.getValue()));
            this.newTruncationPoints.forEach(this.containerMetadata::setValidTruncationPoint);

            // We are done. Clear the transaction.
            rollback();
        }

        /**
         * Gets all pending changes for the given StreamSegment.
         *
         * @param streamSegmentId The Id of the Segment to query.
         * @throws MetadataUpdateException If no metadata entry exists for the given StreamSegment Id.
         */
        TemporaryStreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) throws MetadataUpdateException {
            TemporaryStreamSegmentMetadata tsm = this.streamSegmentUpdates.getOrDefault(streamSegmentId, null);
            if (tsm == null) {
                UpdateableSegmentMetadata streamSegmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
                if (streamSegmentMetadata == null) {
                    streamSegmentMetadata = this.newStreamSegments.getOrDefault(streamSegmentId, null);

                    if (streamSegmentMetadata == null) {
                        throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                                String.format("No metadata entry exists for StreamSegment Id %d.", streamSegmentId));
                    }
                }

                tsm = new TemporaryStreamSegmentMetadata(streamSegmentMetadata, this.containerMetadata.isRecoveryMode());
                this.streamSegmentUpdates.put(streamSegmentId, tsm);
            }

            return tsm;
        }

        /**
         * Records the given Truncation Marker Mapping.
         *
         * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
         * @param logAddress              The Address of the corresponding Data Frame that can be truncated (up to, and including).
         */
        void recordTruncationMarker(long operationSequenceNumber, LogAddress logAddress) {
            Exceptions.checkArgument(operationSequenceNumber >= 0, "operationSequenceNumber", "Operation Sequence Number must be a positive number.");
            Preconditions.checkNotNull(logAddress, "logAddress");
            this.newTruncationMarkers.put(operationSequenceNumber, logAddress);
        }

        /**
         * Sets the new Operation Sequence Number.
         *
         * @param value The new Operation Sequence number.
         */
        void setOperationSequenceNumber(long value) {
            Preconditions.checkState(this.newSequenceNumber != null, "Unable to set new Sequence Number");
            this.newSequenceNumber.set(value);
        }

        /**
         * Pre-processes the given Operation. See OperationMetadataUpdater.preProcessOperation for more details on behavior.
         *
         * @param operation The operation to pre-process.
         * @throws ContainerException              If the given operation was rejected given the current state of the container metadata.
         * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked as deleted.
         * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously sealed and
         *                                         that is incompatible with a sealed stream.
         * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously merged.
         * @throws NullPointerException            If the operation is null.
         */
        void preProcessOperation(Operation operation) throws ContainerException, StreamSegmentException {
            TemporaryStreamSegmentMetadata segmentMetadata = null;
            if (operation instanceof SegmentOperation) {
                segmentMetadata = getStreamSegmentMetadata(((SegmentOperation) operation).getStreamSegmentId());
                if (segmentMetadata.isDeleted()) {
                    throw new StreamSegmentNotExistsException(segmentMetadata.getName());
                }
            }

            if (operation instanceof StorageOperation) {
                if (operation instanceof StreamSegmentAppendOperation) {
                    segmentMetadata.preProcessOperation((StreamSegmentAppendOperation) operation);
                } else if (operation instanceof StreamSegmentSealOperation) {
                    segmentMetadata.preProcessOperation((StreamSegmentSealOperation) operation);
                } else if (operation instanceof MergeTransactionOperation) {
                    MergeTransactionOperation mbe = (MergeTransactionOperation) operation;
                    TemporaryStreamSegmentMetadata transactionMetadata = getStreamSegmentMetadata(mbe.getTransactionSegmentId());
                    transactionMetadata.preProcessAsTransactionSegment(mbe);
                    segmentMetadata.preProcessAsParentSegment(mbe, transactionMetadata);
                }
            } else if (operation instanceof MetadataOperation) {
                if (operation instanceof StreamSegmentMapOperation) {
                    preProcessMetadataOperation((StreamSegmentMapOperation) operation);
                } else if (operation instanceof TransactionMapOperation) {
                    preProcessMetadataOperation((TransactionMapOperation) operation);
                } else if (operation instanceof UpdateAttributesOperation) {
                    segmentMetadata.preProcessOperation((UpdateAttributesOperation) operation);
                } else if (operation instanceof MetadataCheckpointOperation) {
                    // MetadataCheckpointOperations do not require preProcess and accept; they can be handled in a single stage.
                    processMetadataOperation((MetadataCheckpointOperation) operation);
                } else if (operation instanceof StorageMetadataCheckpointOperation) {
                    // StorageMetadataCheckpointOperation do not require preProcess and accept; they can be handled in a single stage.
                    processMetadataOperation((StorageMetadataCheckpointOperation) operation);
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
        void acceptOperation(Operation operation) throws MetadataUpdateException {
            TemporaryStreamSegmentMetadata segmentMetadata = null;
            if (operation instanceof SegmentOperation) {
                segmentMetadata = getStreamSegmentMetadata(((SegmentOperation) operation).getStreamSegmentId());
                segmentMetadata.setLastUsed(operation.getSequenceNumber());
            }

            if (operation instanceof StorageOperation) {
                if (operation instanceof StreamSegmentAppendOperation) {
                    segmentMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
                } else if (operation instanceof StreamSegmentSealOperation) {
                    segmentMetadata.acceptOperation((StreamSegmentSealOperation) operation);
                } else if (operation instanceof MergeTransactionOperation) {
                    MergeTransactionOperation mto = (MergeTransactionOperation) operation;
                    TemporaryStreamSegmentMetadata transactionMetadata = getStreamSegmentMetadata(mto.getTransactionSegmentId());
                    transactionMetadata.acceptAsTransactionSegment(mto);
                    transactionMetadata.setLastUsed(operation.getSequenceNumber());
                    segmentMetadata.acceptAsParentSegment(mto, transactionMetadata);
                }
            } else if (operation instanceof MetadataOperation) {
                if (operation instanceof MetadataCheckpointOperation) {
                    // A MetadataCheckpointOperation represents a valid truncation point. Record it as such.
                    this.newTruncationPoints.add(operation.getSequenceNumber());
                } else if (operation instanceof StreamSegmentMapOperation) {
                    acceptMetadataOperation((StreamSegmentMapOperation) operation);
                } else if (operation instanceof TransactionMapOperation) {
                    acceptMetadataOperation((TransactionMapOperation) operation);
                } else if (operation instanceof UpdateAttributesOperation) {
                    segmentMetadata.acceptOperation((UpdateAttributesOperation) operation);
                }
            }
        }

        private void preProcessMetadataOperation(StreamSegmentMapOperation operation) throws ContainerException {
            // Verify StreamSegment Name is not already mapped somewhere else.
            long existingStreamSegmentId = getExistingStreamSegmentId(operation.getStreamSegmentName());
            if (existingStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                        String.format("Operation %d wants to map a StreamSegment Name that is already mapped in the metadata. Name = '%s', Existing Id = %d.",
                                operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamSegmentId));
            }

            if (!this.containerMetadata.isRecoveryMode()) {
                if (this.containerMetadata.getActiveSegmentCount() + this.newStreamSegments.size() >= this.containerMetadata.getMaximumActiveSegmentCount()) {
                    throw new TooManyActiveSegmentsException(this.containerMetadata.getContainerId(), this.containerMetadata.getMaximumActiveSegmentCount());
                }

                // Assign the SegmentId, but only in non-recovery mode.
                operation.setStreamSegmentId(generateUniqueStreamSegmentId());
            }
        }

        private void preProcessMetadataOperation(TransactionMapOperation operation) throws ContainerException {
            // Verify Parent StreamSegment Exists.
            UpdateableSegmentMetadata parentMetadata = getExistingMetadata(operation.getParentStreamSegmentId());
            if (parentMetadata == null) {
                throw new MetadataUpdateException(
                        this.containerMetadata.getContainerId(),
                        String.format(
                                "Operation %d wants to map a StreamSegment to a Parent StreamSegment Id that does not exist. Parent StreamSegmentId = %d, Transaction Name = %s.",
                                operation.getSequenceNumber(), operation.getParentStreamSegmentId(), operation.getStreamSegmentName()));
            }

            // Verify StreamSegment Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamSegmentId(operation.getStreamSegmentName());
            if (existingStreamId != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(
                        this.containerMetadata.getContainerId(),
                        String.format(
                                "Operation %d wants to map a Transaction StreamSegment Name that is already mapped in the metadata. Transaction Name = '%s', Existing Id = %d.",
                                operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamId));
            }

            if (!this.containerMetadata.isRecoveryMode()) {
                if (this.containerMetadata.getActiveSegmentCount() + this.newStreamSegments.size() >= this.containerMetadata.getMaximumActiveSegmentCount()) {
                    throw new TooManyActiveSegmentsException(this.containerMetadata.getContainerId(), this.containerMetadata.getMaximumActiveSegmentCount());
                }

                // Assign the SegmentId.
                operation.setStreamSegmentId(generateUniqueStreamSegmentId());
            }
        }

        private void processMetadataOperation(MetadataCheckpointOperation operation) throws MetadataUpdateException {
            try {
                if (this.containerMetadata.isRecoveryMode()) {
                    // In Recovery Mode, a MetadataCheckpointOperation means the entire, up-to-date state of the
                    // metadata is serialized in this operation. We need to discard whatever we have accumulated so far
                    // and rebuild the metadata from the information we have so far.
                    if (this.processedCheckpoint) {
                        // But we can (should) only process at most one MetadataCheckpoint per recovery. Any additional
                        // ones are redundant (used just for Truncation purposes) and contain the same information as
                        // if we processed every operation in order, up to them.
                        log.info("{}: Skipping recovering MetadataCheckpointOperation with SequenceNumber {} because we already have metadata changes.", this.traceObjectId, operation.getSequenceNumber());
                        return;
                    }

                    log.info("{}: Recovering MetadataCheckpointOperation with SequenceNumber {}.", this.traceObjectId, operation.getSequenceNumber());
                    rollback();
                    deserializeFrom(operation);
                    this.processedCheckpoint = true;
                } else {
                    // In non-Recovery Mode, a MetadataCheckpointOperation means we need to serialize the current state of
                    // the Metadata, both the base Container Metadata and the current Transaction.
                    serializeTo(operation);
                }
            } catch (IOException | SerializationException ex) {
                throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                        "Unable to process MetadataCheckpointOperation " + operation.toString(), ex);
            }
        }

        private void processMetadataOperation(StorageMetadataCheckpointOperation operation) throws MetadataUpdateException {
            try {
                if (this.containerMetadata.isRecoveryMode()) {
                    updateFrom(operation);
                } else {
                    serializeTo(operation);
                }
            } catch (IOException | SerializationException ex) {
                throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                        "Unable to process StorageMetadataCheckpointOperation " + operation.toString(), ex);
            }
        }

        private void acceptMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
            if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                        "StreamSegmentMapOperation does not have a StreamSegmentId assigned: " + operation.toString());
            }

            // Create StreamSegment metadata here - we need to do this as part of the transaction.
            UpdateableSegmentMetadata streamSegmentMetadata = recordNewStreamSegment(operation.getStreamSegmentName(), operation.getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID);
            streamSegmentMetadata.setStorageLength(operation.getLength());
            streamSegmentMetadata.setDurableLogLength(operation.getLength()); // DurableLogLength must be at least StorageLength.
            if (operation.isSealed()) {
                // MapOperations represent the state of the StreamSegment in Storage. If it is sealed in storage, both
                // Seal flags need to be set.
                streamSegmentMetadata.markSealed();
                streamSegmentMetadata.markSealedInStorage();
            }

            streamSegmentMetadata.updateAttributes(operation.getAttributes());
        }

        private void acceptMetadataOperation(TransactionMapOperation operation) throws MetadataUpdateException {
            if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                        "TransactionMapOperation does not have a StreamSegmentId assigned: " + operation.toString());
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            UpdateableSegmentMetadata transactionMetadata = recordNewStreamSegment(operation.getStreamSegmentName(), operation.getStreamSegmentId(), operation.getParentStreamSegmentId());
            transactionMetadata.setStorageLength(operation.getLength());
            transactionMetadata.setDurableLogLength(0);
            if (operation.isSealed()) {
                // MapOperations represent the state of the StreamSegment in Storage. If it is sealed in storage, both
                // Seal flags need to be set.
                transactionMetadata.markSealed();
                transactionMetadata.markSealedInStorage();
            }

            transactionMetadata.updateAttributes(operation.getAttributes());
        }

        private void rollback() {
            this.streamSegmentUpdates.clear();
            this.newStreamSegments.clear();
            this.newStreamSegmentNames.clear();
            this.newTruncationMarkers.clear();
            this.newTruncationPoints.clear();
            this.processedCheckpoint = false;
        }

        private long getExistingStreamSegmentId(String streamSegmentName) {
            long existingSegmentId = this.containerMetadata.getStreamSegmentId(streamSegmentName, false);
            if (existingSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                existingSegmentId = this.newStreamSegmentNames.getOrDefault(streamSegmentName, ContainerMetadata.NO_STREAM_SEGMENT_ID);
            }

            return existingSegmentId;
        }

        private long generateUniqueStreamSegmentId() {
            // The ContainerMetadata.SequenceNumber is always guaranteed to be unique (it's monotonically strict increasing).
            // It can be safely used as a new unique Segment Id. If any clashes occur, just keep searching up until we find
            // a non-used one.
            long streamSegmentId = Math.max(this.containerMetadata.getOperationSequenceNumber(), ContainerMetadata.NO_STREAM_SEGMENT_ID + 1);
            while (this.newStreamSegments.containsKey(streamSegmentId) || this.containerMetadata.getStreamSegmentMetadata(streamSegmentId) != null) {
                streamSegmentId++;
            }

            return streamSegmentId;
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
            if (parentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, this.containerMetadata.getContainerId());
            } else {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentId, this.containerMetadata.getContainerId());
            }

            this.newStreamSegments.put(metadata.getId(), metadata);
            this.newStreamSegmentNames.put(metadata.getName(), metadata.getId());

            return metadata;
        }

        private void copySegmentMetadataToSource(Collection<UpdateableSegmentMetadata> newSegments, Predicate<SegmentMetadata> filter) {
            for (SegmentMetadata newMetadata : newSegments) {
                if (!filter.test(newMetadata)) {
                    continue;
                }

                UpdateableSegmentMetadata existingMetadata;
                if (newMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                    existingMetadata = this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId(), newMetadata.getParentId());
                } else {
                    existingMetadata = this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId());
                }

                // Update real metadata with all the information from the new metadata.
                existingMetadata.copyFrom(newMetadata);
            }
        }

        /**
         * Deserializes the Metadata from the given stream.
         *
         * @param operation The MetadataCheckpointOperation to deserialize from.
         * @throws IOException            If the stream threw one.
         * @throws SerializationException If the given Stream is an invalid metadata serialization.
         * @throws IllegalStateException  If the Metadata is not in Recovery Mode.
         */
        private void deserializeFrom(MetadataCheckpointOperation operation) throws IOException, SerializationException {
            Preconditions.checkState(this.containerMetadata.isRecoveryMode(), "Cannot deserialize Metadata in non-recovery mode.");

            DataInputStream stream = new DataInputStream(new GZIPInputStream(operation.getContents().getReader()));

            // 1. Version.
            byte version = stream.readByte();
            if (version != CURRENT_SERIALIZATION_VERSION) {
                throw new SerializationException("Metadata.deserialize", String.format("Unsupported version: %d.", version));
            }

            // 2. Container id.
            int containerId = stream.readInt();
            if (this.containerMetadata.getContainerId() != containerId) {
                throw new SerializationException("Metadata.deserialize", String.format("Invalid StreamSegmentContainerId. Expected '%d', actual '%d'.", this.containerMetadata.getContainerId(), containerId));
            }

            // This is not retrieved from serialization, but rather from the operation itself.
            this.containerMetadata.setOperationSequenceNumber(operation.getSequenceNumber());

            // 3. Stream Segments (unchanged).
            int segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeSegmentMetadata(stream);
            }

            // 4. Stream Segments (updated).
            segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeSegmentMetadata(stream);
            }

            // 5. New Stream Segments.
            segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeSegmentMetadata(stream);
            }
        }

        /**
         * Applies the updates stored in the given StorageMetadataCheckpointOperation.
         *
         * @param operation The StorageMetadataCheckpointOperation to update from.
         * @throws IOException            If the stream threw one.
         * @throws SerializationException If the given Stream is an invalid metadata serialization.
         * @throws IllegalStateException  If the Metadata is not in Recovery Mode.
         */
        private void updateFrom(StorageMetadataCheckpointOperation operation) throws IOException, SerializationException, MetadataUpdateException {
            Preconditions.checkState(this.containerMetadata.isRecoveryMode(), "Cannot bulk-update Metadata in non-recovery mode.");

            DataInputStream stream = new DataInputStream(new GZIPInputStream(operation.getContents().getReader()));

            // 1. Version.
            byte version = stream.readByte();
            if (version != CURRENT_SERIALIZATION_VERSION) {
                throw new SerializationException("Metadata.updateFrom", String.format("Unsupported version: %d.", version));
            }

            // 2. Segments
            int segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeStorageSegmentMetadata(stream);
            }
        }

        private void serializeTo(MetadataCheckpointOperation operation) throws IOException {
            assert operation != null : "operation is null";
            Preconditions.checkState(!this.containerMetadata.isRecoveryMode(), "Cannot serialize Metadata in recovery mode.");

            EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
            GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
            DataOutputStream stream = new DataOutputStream(zipStream);

            // 1. Version.
            stream.writeByte(CURRENT_SERIALIZATION_VERSION);

            // 2. Container Id.
            stream.writeInt(this.containerMetadata.getContainerId());

            // Intentionally skipping over the Sequence Number. There is no need for that here; it will be set on the
            // operation anyway when it gets serialized.

            // 3. Unchanged Segment Metadata.
            Collection<Long> unchangedSegmentIds = this.containerMetadata
                    .getAllStreamSegmentIds().stream()
                    .filter(segmentId -> !this.streamSegmentUpdates.containsKey(segmentId))
                    .collect(Collectors.toList());
            stream.writeInt(unchangedSegmentIds.size());
            unchangedSegmentIds.forEach(segmentId -> serializeSegmentMetadata(this.containerMetadata.getStreamSegmentMetadata(segmentId), stream));

            // 4. New StreamSegments.
            Collection<UpdateableSegmentMetadata> newSegments = this.newStreamSegments
                    .values().stream()
                    .filter(sm -> !this.streamSegmentUpdates.containsKey(sm.getId()))
                    .collect(Collectors.toList());
            stream.writeInt(newSegments.size());
            newSegments.forEach(sm -> serializeSegmentMetadata(sm, stream));

            // 5. Changed Segment Metadata.
            stream.writeInt(this.streamSegmentUpdates.size());
            this.streamSegmentUpdates.values().forEach(sm -> serializeSegmentMetadata(sm, stream));

            zipStream.finish();
            operation.setContents(byteStream.getData());
        }

        private void serializeTo(StorageMetadataCheckpointOperation operation) throws IOException {
            assert operation != null : "operation is null";
            Preconditions.checkState(!this.containerMetadata.isRecoveryMode(), "Cannot serialize Metadata in recovery mode.");

            EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
            GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
            DataOutputStream stream = new DataOutputStream(zipStream);

            // 1. Version.
            stream.writeByte(CURRENT_SERIALIZATION_VERSION);

            // 2. Segment Metadata (there is no point in adding the new ones as those ones have not yet had
            // a chance to be updated in Storage)
            Collection<Long> segmentIds = this.containerMetadata.getAllStreamSegmentIds();
            stream.writeInt(segmentIds.size());
            segmentIds.forEach(segmentId -> serializeStorageSegmentMetadata(this.containerMetadata.getStreamSegmentMetadata(segmentId), stream));

            zipStream.finish();
            operation.setContents(byteStream.getData());
        }

        @SneakyThrows(IOException.class)
        private void serializeSegmentMetadata(SegmentMetadata sm, DataOutputStream stream) {
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
            // S8. SealedInStorage.
            stream.writeBoolean(sm.isSealedInStorage());
            // S9. Deleted.
            stream.writeBoolean(sm.isDeleted());
            // S10. LastModified.
            stream.writeLong(sm.getLastModified().getTime());
            // S11. Attributes.
            AttributeSerializer.serialize(sm.getAttributes(), stream);
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
            // S8. SealedInStorage.
            boolean isSealedInStorage = stream.readBoolean();
            if (isSealedInStorage) {
                metadata.markSealedInStorage();
            }
            // S9. Deleted.
            boolean isDeleted = stream.readBoolean();
            if (isDeleted) {
                metadata.markDeleted();
            }
            // S10. LastModified.
            ImmutableDate lastModified = new ImmutableDate(stream.readLong());
            metadata.setLastModified(lastModified);

            // S11. Attributes.
            val attributes = AttributeSerializer.deserialize(stream);
            metadata.updateAttributes(attributes);
        }

        @SneakyThrows(IOException.class)
        private void serializeStorageSegmentMetadata(SegmentMetadata sm, DataOutputStream stream) {
            // S1. StreamSegmentId.
            stream.writeLong(sm.getId());
            // S2. StorageLength.
            stream.writeLong(sm.getStorageLength());
            // S3. SealedInStorage.
            stream.writeBoolean(sm.isSealedInStorage());
            // S4. Deleted.
            stream.writeBoolean(sm.isDeleted());
        }

        private void deserializeStorageSegmentMetadata(DataInputStream stream) throws IOException, MetadataUpdateException {
            // S1. StreamSegmentId.
            long segmentId = stream.readLong();
            TemporaryStreamSegmentMetadata metadata = getStreamSegmentMetadata(segmentId);
            // S2. StorageLength.
            long storageLength = stream.readLong();
            // S3. SealedInStorage.
            boolean sealedInStorage = stream.readBoolean();
            // S4. Deleted.
            boolean deleted = stream.readBoolean();
            metadata.updateStorageState(storageLength, sealedInStorage, deleted);
        }
    }

    //endregion

    //region TemporaryStreamSegmentMetadata

    /**
     * Pending StreamSegment Metadata.
     */
    private static class TemporaryStreamSegmentMetadata implements SegmentMetadata {
        //region Members

        private final UpdateableSegmentMetadata baseMetadata;
        private final boolean isRecoveryMode;
        private final Map<UUID, Long> updatedAttributeValues;
        private long currentDurableLogLength;
        private boolean sealed;
        private boolean merged;
        private boolean deleted;
        private long lastUsed;
        private boolean isChanged;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the TemporaryStreamSegmentMetadata class.
         *
         * @param baseMetadata   The base StreamSegment Metadata.
         * @param isRecoveryMode Whether the metadata is currently in recovery model
         */
        TemporaryStreamSegmentMetadata(UpdateableSegmentMetadata baseMetadata, boolean isRecoveryMode) {
            assert baseMetadata != null : "baseMetadata is null";
            this.baseMetadata = baseMetadata;
            this.isRecoveryMode = isRecoveryMode;
            this.currentDurableLogLength = this.baseMetadata.getDurableLogLength();
            this.sealed = this.baseMetadata.isSealed();
            this.merged = this.baseMetadata.isMerged();
            this.deleted = this.baseMetadata.isDeleted();
            this.updatedAttributeValues = new HashMap<>();
            this.lastUsed = -1;
        }

        //endregion

        //region StreamProperties Implementation

        @Override
        public String getName() {
            return this.baseMetadata.getName();
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
        public ImmutableDate getLastModified() {
            return new ImmutableDate(); //TODO: implement properly.
        }

        //endregion

        //region SegmentMetadata Implementation

        @Override
        public long getId() {
            return this.baseMetadata.getId();
        }

        @Override
        public long getParentId() {
            return this.baseMetadata.getParentId();
        }

        @Override
        public int getContainerId() {
            return this.baseMetadata.getContainerId();
        }

        @Override
        public boolean isMerged() {
            return this.merged;
        }

        @Override
        public boolean isSealedInStorage() {
            return this.baseMetadata.isSealedInStorage();
        }

        @Override
        public long getStorageLength() {
            return this.baseMetadata.getStorageLength();
        }

        @Override
        public long getDurableLogLength() {
            return this.currentDurableLogLength;
        }

        @Override
        public long getLastUsed() {
            return this.baseMetadata.getLastUsed();
        }

        @Override
        public Map<UUID, Long> getAttributes() {
            // Important: This only returns the updated attributes, not the whole set of attributes. If it also returned
            // the base attributes, upon commit() they would be unnecessarily re-applied to the same segment.
            return Collections.unmodifiableMap(this.updatedAttributeValues);
        }

        private long getAttributeValue(UUID attributeId, long defaultValue) {
            if (this.updatedAttributeValues.containsKey(attributeId)) {
                return this.updatedAttributeValues.get(attributeId);
            } else {
                return this.baseMetadata.getAttributes().getOrDefault(attributeId, defaultValue);
            }
        }

        //endregion

        //region Pre-Processing

        /**
         * Pre-processes a StreamSegmentAppendOperation.
         * After this method returns, the given operation will have its StreamSegmentOffset property set to the current
         * StreamSegmentLength, and all AttributeUpdates will be set to the current values.
         *
         * @param operation The operation to pre-process.
         * @throws StreamSegmentSealedException If the StreamSegment is sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws BadOffsetException           If the operation has an assigned offset, but it doesn't match the current
         *                                      Segment DurableLogOffset.
         * @throws IllegalArgumentException     If the operation is for a different StreamSegment.
         * @throws BadAttributeUpdateException  If at least one of the AttributeUpdates is invalid given the current attribute
         *                                      values of the segment.
         */
        void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
                BadOffsetException, BadAttributeUpdateException {
            ensureSegmentId(operation);
            if (this.merged) {
                // We do not allow any operation after merging (since after merging the StreamSegment disappears).
                throw new StreamSegmentMergedException(this.baseMetadata.getName());
            }

            if (this.sealed) {
                throw new StreamSegmentSealedException(this.baseMetadata.getName());
            }

            if (!this.isRecoveryMode) {
                // Offset check (if append-with-offset).
                long operationOffset = operation.getStreamSegmentOffset();
                if (operationOffset >= 0) {
                    // If the Operation already has an offset assigned, verify that it matches the current end offset of the Segment.
                    if (operationOffset != this.currentDurableLogLength) {
                        throw new BadOffsetException(this.baseMetadata.getName(), this.currentDurableLogLength, operationOffset);
                    }
                } else {
                    // No pre-assigned offset. Put the Append at the end of the Segment.
                    operation.setStreamSegmentOffset(this.currentDurableLogLength);
                }

                // Attribute validation.
                preProcessAttributes(operation.getAttributeUpdates());
            }
        }

        /**
         * Pre-processes a UpdateAttributesOperation.
         * After this method returns, the given operation will have its AttributeUpdates set to the current values of
         * those attributes.
         *
         * @param operation The operation to pre-process.
         * @throws StreamSegmentSealedException If the StreamSegment is sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws IllegalArgumentException     If the operation is for a different StreamSegment.
         * @throws BadAttributeUpdateException  If at least one of the AttributeUpdates is invalid given the current attribute
         *                                      values of the segment.
         */
        void preProcessOperation(UpdateAttributesOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
                BadAttributeUpdateException {
            ensureSegmentId(operation);
            if (this.merged) {
                // We do not allow any operation after merging (since after merging the StreamSegment disappears).
                throw new StreamSegmentMergedException(this.baseMetadata.getName());
            }

            if (this.sealed) {
                throw new StreamSegmentSealedException(this.baseMetadata.getName());
            }

            if (!this.isRecoveryMode) {
                preProcessAttributes(operation.getAttributeUpdates());
            }
        }

        /**
         * Pre-processes a StreamSegmentSealOperation.
         * After this method returns, the operation will have its StreamSegmentLength property set to the current length of the StreamSegment.
         *
         * @param operation The Operation.
         * @throws StreamSegmentSealedException If the StreamSegment is already sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws IllegalArgumentException     If the operation is for a different StreamSegment.
         */
        void preProcessOperation(StreamSegmentSealOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException {
            ensureSegmentId(operation);
            if (this.merged) {
                // We do not allow any operation after merging (since after merging the Stream disappears).
                throw new StreamSegmentMergedException(this.baseMetadata.getName());
            }

            if (this.sealed) {
                // We do not allow re-sealing an already sealed stream.
                throw new StreamSegmentSealedException(this.baseMetadata.getName());
            }

            if (!this.isRecoveryMode) {
                // Assign entry StreamSegment Length.
                operation.setStreamSegmentOffset(this.currentDurableLogLength);
            }
        }

        /**
         * Pre-processes the given MergeTransactionOperation as a Parent StreamSegment.
         * After this method returns, the operation will have its TargetStreamSegmentOffset set to the length of the Parent StreamSegment.
         *
         * @param operation           The operation to pre-process.
         * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
         * @throws StreamSegmentSealedException If the parent stream is already sealed.
         * @throws MetadataUpdateException      If the operation cannot be processed because of the current state of the metadata.
         * @throws IllegalArgumentException     If the operation is for a different StreamSegment.
         */
        void preProcessAsParentSegment(MergeTransactionOperation operation, TemporaryStreamSegmentMetadata transactionMetadata) throws StreamSegmentSealedException, MetadataUpdateException {
            ensureSegmentId(operation);

            if (this.sealed) {
                // We do not allow merging into sealed Segments.
                throw new StreamSegmentSealedException(this.baseMetadata.getName());
            }

            if (this.baseMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "Cannot merge a StreamSegment into a Transaction StreamSegment: " + operation.toString());
            }

            // Check that the Transaction has been properly sealed and has its length set.
            if (!transactionMetadata.isSealed()) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "Transaction StreamSegment to be merged needs to be sealed: " + operation.toString());
            }

            long transLength = operation.getLength();
            if (transLength < 0) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "MergeTransactionOperation does not have its Transaction StreamSegment Length set: " + operation.toString());
            }

            if (!this.isRecoveryMode) {
                // Assign entry StreamSegment offset and update StreamSegment offset afterwards.
                operation.setStreamSegmentOffset(this.currentDurableLogLength);
            }
        }

        /**
         * Pre-processes the given operation as a Transaction StreamSegment.
         *
         * @param operation The operation
         * @throws IllegalArgumentException     If the operation is for a different stream segment.
         * @throws MetadataUpdateException      If the StreamSegment is not sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is already merged.
         */
        void preProcessAsTransactionSegment(MergeTransactionOperation operation) throws MetadataUpdateException, StreamSegmentMergedException {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(), "operation", "Invalid Operation Transaction StreamSegment Id.");

            if (this.merged) {
                throw new StreamSegmentMergedException(this.baseMetadata.getName());
            }

            if (!this.sealed) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "Transaction StreamSegment to be merged needs to be sealed: " + operation.toString());
            }

            if (!this.isRecoveryMode) {
                operation.setLength(this.currentDurableLogLength);
            }
        }

        /**
         * Pre-processes a collection of attributes.
         * After this method returns, all AttributeUpdates in the given collection will have the actual (and updated) value
         * of that attribute in the Segment.
         *
         * @param attributeUpdates The Updates to process (if any).
         * @throws BadAttributeUpdateException If any of the given AttributeUpdates is invalid given the current state of
         *                                     the segment.
         */
        void preProcessAttributes(Collection<AttributeUpdate> attributeUpdates) throws BadAttributeUpdateException {
            if (attributeUpdates == null) {
                return;
            }

            for (AttributeUpdate u : attributeUpdates) {
                AttributeUpdateType updateType = u.getUpdateType();
                long previousValue = getAttributeValue(u.getAttributeId(), SegmentMetadata.NULL_ATTRIBUTE_VALUE);

                // Perform validation, and set the AttributeUpdate.value to the updated value, if necessary.
                switch (updateType) {
                    case ReplaceIfGreater:
                        // Verify value against existing value, if any.
                        if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE && u.getValue() <= previousValue) {
                            throw new BadAttributeUpdateException(this.baseMetadata.getName(), u,
                                    String.format("Expected greater than '%s'.", previousValue));
                        }

                        break;
                    case None:
                        // Attribute cannot be updated once set.
                        if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                            throw new BadAttributeUpdateException(this.baseMetadata.getName(), u,
                                    String.format("Attribute value already exists and cannot be updated (%s).", previousValue));
                        }

                        break;
                    case Accumulate:
                        if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                            u.setValue(previousValue + u.getValue());
                        }

                        break;
                    case Replace:
                    default:
                        log.error("Unexpected update type: {}", updateType);
                        break;
                }
            }
        }

        //endregion

        //region AcceptOperation

        /**
         * Accepts a StreamSegmentAppendOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation StreamSegmentOffset is different from the current StreamSegment Length.
         * @throws IllegalArgumentException If the operation is for a different StreamSegment.
         */
        void acceptOperation(StreamSegmentAppendOperation operation) throws MetadataUpdateException {
            ensureSegmentId(operation);
            if (operation.getStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(
                        this.baseMetadata.getContainerId(),
                        String.format("StreamSegmentAppendOperation offset mismatch. Expected %d, actual %d.",
                                this.currentDurableLogLength, operation.getStreamSegmentOffset()));
            }

            this.currentDurableLogLength += operation.getData().length;
            acceptAttributes(operation.getAttributeUpdates());
            this.isChanged = true;
        }

        /**
         * Accepts an UpdateAttributesOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws IllegalArgumentException If the operation is for a different StreamSegment.
         */
        void acceptOperation(UpdateAttributesOperation operation) {
            ensureSegmentId(operation);
            acceptAttributes(operation.getAttributeUpdates());
            this.isChanged = true;
        }

        /**
         * Accepts a StreamSegmentSealOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation hasn't been pre-processed.
         * @throws IllegalArgumentException If the operation is for a different StreamSegment.
         */
        void acceptOperation(StreamSegmentSealOperation operation) throws MetadataUpdateException {
            ensureSegmentId(operation);
            if (operation.getStreamSegmentOffset() < 0) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "StreamSegmentSealOperation cannot be accepted if it hasn't been pre-processed: " + operation.toString());
            }

            this.sealed = true;

            // Clear all dynamic attributes.
            this.updatedAttributeValues.keySet().removeIf(Attributes::isDynamic);
            for (UUID attributeId : this.baseMetadata.getAttributes().keySet()) {
                if (Attributes.isDynamic(attributeId)) {
                    this.updatedAttributeValues.put(attributeId, SegmentMetadata.NULL_ATTRIBUTE_VALUE);
                }
            }

            this.isChanged = true;
        }

        /**
         * Accepts the given MergeTransactionOperation as a Parent StreamSegment.
         *
         * @param operation           The operation to accept.
         * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
         * @throws MetadataUpdateException  If the operation cannot be processed because of the current state of the metadata.
         * @throws IllegalArgumentException If the operation is for a different StreamSegment.
         */
        void acceptAsParentSegment(MergeTransactionOperation operation, TemporaryStreamSegmentMetadata transactionMetadata) throws MetadataUpdateException {
            ensureSegmentId(operation);

            if (operation.getStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        String.format("MergeTransactionOperation target offset mismatch. Expected %d, actual %d.",
                                this.currentDurableLogLength, operation.getStreamSegmentOffset()));
            }

            long transLength = operation.getLength();
            if (transLength < 0 || transLength != transactionMetadata.currentDurableLogLength) {
                throw new MetadataUpdateException(this.baseMetadata.getContainerId(),
                        "MergeTransactionOperation does not seem to have been pre-processed: " + operation.toString());
            }

            this.currentDurableLogLength += transLength;
            this.isChanged = true;
        }

        /**
         * Accepts the given operation as a Transaction Stream Segment.
         *
         * @param operation The operation
         * @throws IllegalArgumentException If the operation is for a different StreamSegment.
         */
        void acceptAsTransactionSegment(MergeTransactionOperation operation) {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(), "operation", "Invalid Operation Transaction StreamSegment Id.");

            this.sealed = true;
            this.merged = true;
            this.isChanged = true;
        }

        /**
         * Accepts a collection of AttributeUpdates in the metadata.
         *
         * @param attributeUpdates The Attribute updates to accept.
         */
        void acceptAttributes(Collection<AttributeUpdate> attributeUpdates) {
            if (attributeUpdates == null) {
                return;
            }

            for (AttributeUpdate au : attributeUpdates) {
                this.updatedAttributeValues.put(au.getAttributeId(), au.getValue());
            }
        }

        /**
         * Sets the last used value to the given one.
         *
         * @param value The value to set.
         */
        void setLastUsed(long value) {
            this.lastUsed = value;
        }

        //endregion

        //region Operations

        /**
         * Updates the base metadata directly with the given state of the segment in storage. Note that, as opposed from
         * the rest of the methods in this class, this does not first update the transaction and then apply it to the
         * base segment, instead it modifies it directly.
         *
         * This method is only meant to be used during recovery mode when we need to restore the state of a segment.
         * During normal operations, these values are set asynchronously by the Writer.
         *
         * @param storageLength The value to set as StorageLength.
         * @param storageSealed The value to set as SealedInStorage.
         * @param deleted       The value to set as Deleted.
         */
        void updateStorageState(long storageLength, boolean storageSealed, boolean deleted) {
            this.baseMetadata.setStorageLength(storageLength);
            if (storageSealed) {
                this.baseMetadata.markSealedInStorage();
            }

            if (deleted) {
                this.baseMetadata.markDeleted();
            }
        }

        /**
         * Applies all the outstanding changes to the base StreamSegmentMetadata object.
         */
        public void apply() {
            if (!this.isChanged) {
                // No changes made.
                return;
            }

            // Apply to base metadata.
            this.baseMetadata.setLastUsed(this.lastUsed);
            this.baseMetadata.updateAttributes(this.updatedAttributeValues);
            this.baseMetadata.setDurableLogLength(this.currentDurableLogLength);
            if (this.isSealed()) {
                this.baseMetadata.markSealed();
                if (this.isSealedInStorage()) {
                    this.baseMetadata.isSealedInStorage();
                }
            }

            if (this.isMerged()) {
                this.baseMetadata.markMerged();
            }
        }

        private void ensureSegmentId(SegmentOperation operation) {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getStreamSegmentId(), "operation", "Invalid Log Operation StreamSegment Id.");
        }

        //endregion
    }

    //endregion
}
