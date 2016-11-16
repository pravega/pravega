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

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.io.EnhancedByteArrayOutputStream;
import com.emc.pravega.common.util.CollectionHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.BadEventNumberException;
import com.emc.pravega.service.contracts.BadOffsetException;
import com.emc.pravega.service.contracts.StreamSegmentException;
import com.emc.pravega.service.contracts.StreamSegmentMergedException;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.containers.StreamSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.MergeTransactionOperation;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.MetadataOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentMapOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.logs.operations.TransactionMapOperation;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static com.emc.pravega.common.util.CollectionHelpers.forEach;

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
    public long getStreamSegmentId(String streamSegmentName) {
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
     * @param logAddress              The Address of the corresponding Data Frame that can be truncated (up to, and
     *                                including).
     */
    void recordTruncationMarker(long operationSequenceNumber, LogAddress logAddress) {
        log.debug("{}: RecordTruncationMarker OperationSequenceNumber = {}, DataFrameAddress = {}.", this.traceObjectId,
                operationSequenceNumber, logAddress);
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
     * For all other kinds of MetadataOperations (i.e., StreamSegmentMapOperation, TransactionMapOperation) this
     * method only
     * does anything if the base Container Metadata is in Recovery Mode (in which case the given MetadataOperation) is
     * recorded in the pending transaction.
     *
     * @param operation The operation to pre-process.
     * @throws MetadataUpdateException         If the given operation was rejected given the current state of the
     *                                         metadata.
     * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked as
     *                                         deleted.
     * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously
     *                                         sealed and
     *                                         that is incompatible with a sealed stream.
     * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously
     *                                         merged.
     * @throws NullPointerException            If the operation is null.
     */
    void preProcessOperation(Operation operation) throws MetadataUpdateException, StreamSegmentException {
        log.trace("{}: PreProcess {}.", this.traceObjectId, operation);
        getCurrentTransaction().preProcessOperation(operation);
    }

    /**
     * Phase 2/2 of processing an Operation. The Operation's effects are reflected in the pending transaction.
     * <p/>
     * This method only has an effect on StorageOperations. It does nothing for MetadataOperations, regardless of
     * whether
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
     * A Metadata Update Transaction. Keeps all pending changes, until they are ready to be committed to the base
     * Container Metadata.
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

                // Reset cleaned up the Operation Sequence number. We need to reset it to whatever we have in our
                // transaction.
                // If we have nothing, we'll just set it to 0, which is what the default value was in the metadata too.
                this.containerMetadata.setOperationSequenceNumber(this.newSequenceNumber.get());
            }

            // Commit all temporary changes to their respective sources.
            this.streamSegmentUpdates.values().forEach(TemporaryStreamSegmentMetadata::apply);

            // We must first copy the Standalone StreamSegments, and then the Transaction StreamSegments. That's because
            // the Transaction StreamSegments may refer to one of these newly created StreamSegments, and the metadata
            // will reject the operation if it can't find the parent.
            // We need this because HashMap does not necessarily preserve order when iterating via values().
            copySegmentMetadataToSource(newStreamSegments.values(),
                    s -> s.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID);
            copySegmentMetadataToSource(newStreamSegments.values(),
                    s -> s.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID);

            // Copy truncation markers.
            this.newTruncationMarkers.entrySet().forEach(
                    e -> this.containerMetadata.recordTruncationMarker(e.getKey(), e.getValue()));
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
                UpdateableSegmentMetadata streamSegmentMetadata = this.containerMetadata.getStreamSegmentMetadata(
                        streamSegmentId);
                if (streamSegmentMetadata == null) {
                    streamSegmentMetadata = this.newStreamSegments.getOrDefault(streamSegmentId, null);

                    if (streamSegmentMetadata == null) {
                        throw new MetadataUpdateException(
                                String.format("No metadata entry exists for StreamSegment " + "Id %d.",
                                        streamSegmentId));
                    }
                }

                tsm = new TemporaryStreamSegmentMetadata(streamSegmentMetadata,
                        this.containerMetadata.isRecoveryMode());

                this.streamSegmentUpdates.put(streamSegmentId, tsm);
            }

            return tsm;
        }

        /**
         * Records the given Truncation Marker Mapping.
         *
         * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation
         *                                argument.
         * @param logAddress              The Address of the corresponding Data Frame that can be truncated (up to,
         *                                and including).
         */
        void recordTruncationMarker(long operationSequenceNumber, LogAddress logAddress) {
            Exceptions.checkArgument(operationSequenceNumber >= 0, "operationSequenceNumber",
                    "Operation Sequence " + "Number must be a positive number.");
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
         * Pre-processes the given Operation. See OperationMetadataUpdater.preProcessOperation for more details on
         * behavior.
         *
         * @param operation The operation to pre-process.
         * @throws MetadataUpdateException         If the given operation was rejected given the current state of the
         *                                         metadata.
         * @throws StreamSegmentNotExistsException If the given operation was for a StreamSegment that was is marked
         *                                         as deleted.
         * @throws StreamSegmentSealedException    If the given operation was for a StreamSegment that was previously
         *                                         sealed and
         *                                         that is incompatible with a sealed stream.
         * @throws StreamSegmentMergedException    If the given operation was for a StreamSegment that was previously
         *                                         merged.
         * @throws NullPointerException            If the operation is null.
         */
        void preProcessOperation(Operation operation) throws MetadataUpdateException, StreamSegmentException {
            if (operation instanceof StorageOperation) {
                TemporaryStreamSegmentMetadata streamMetadata = getStreamSegmentMetadata(
                        ((StorageOperation) operation).getStreamSegmentId());
                if (streamMetadata.isDeleted()) {
                    throw new StreamSegmentNotExistsException(streamMetadata.getName());
                }

                if (operation instanceof StreamSegmentAppendOperation) {
                    streamMetadata.preProcessOperation((StreamSegmentAppendOperation) operation);
                } else if (operation instanceof StreamSegmentSealOperation) {
                    streamMetadata.preProcessOperation((StreamSegmentSealOperation) operation);
                } else if (operation instanceof MergeTransactionOperation) {
                    MergeTransactionOperation mbe = (MergeTransactionOperation) operation;
                    TemporaryStreamSegmentMetadata transactionMetadata = getStreamSegmentMetadata(
                            mbe.getTransactionSegmentId());
                    transactionMetadata.preProcessAsTransactionSegment(mbe);
                    streamMetadata.preProcessAsParentSegment(mbe, transactionMetadata);
                }
            } else if (operation instanceof MetadataOperation) {
                if (operation instanceof StreamSegmentMapOperation) {
                    preProcessMetadataOperation((StreamSegmentMapOperation) operation);
                } else if (operation instanceof TransactionMapOperation) {
                    preProcessMetadataOperation((TransactionMapOperation) operation);
                } else if (operation instanceof MetadataCheckpointOperation) {
                    // MetadataCheckpointOperations do not require preProcess and accept; they can be handled in a
                    // single stage.
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
        void acceptOperation(Operation operation) throws MetadataUpdateException {
            if (operation instanceof StorageOperation) {
                TemporaryStreamSegmentMetadata segmentMetadata = getStreamSegmentMetadata(
                        ((StorageOperation) operation).getStreamSegmentId());
                if (operation instanceof StreamSegmentAppendOperation) {
                    segmentMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
                } else if (operation instanceof StreamSegmentSealOperation) {
                    segmentMetadata.acceptOperation((StreamSegmentSealOperation) operation);
                } else if (operation instanceof MergeTransactionOperation) {
                    MergeTransactionOperation mto = (MergeTransactionOperation) operation;
                    TemporaryStreamSegmentMetadata transactionMetadata = getStreamSegmentMetadata(
                            mto.getTransactionSegmentId());
                    transactionMetadata.acceptAsTransactionSegment(mto);
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
                }
            }
        }

        private void preProcessMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
            // Verify StreamSegment Name is not already mapped somewhere else.
            long existingStreamSegmentId = getExistingStreamSegmentId(operation.getStreamSegmentName());
            if (existingStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(String.format(
                        "Operation %d wants to map a StreamSegment Name that " + "is already mapped in the metadata. " +
                                "Name = '%s', Existing Id = %d.",
                        operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamSegmentId));
            }

            if (!this.containerMetadata.isRecoveryMode()) {
                // Assign the SegmentId, but only in non-recovery mode.
                operation.setStreamSegmentId(generateUniqueStreamSegmentId());
            }
        }

        private void preProcessMetadataOperation(TransactionMapOperation operation) throws MetadataUpdateException {
            // Verify Parent StreamSegment Exists.
            UpdateableSegmentMetadata parentMetadata = getExistingMetadata(operation.getParentStreamSegmentId());
            if (parentMetadata == null) {
                throw new MetadataUpdateException(String.format(
                        "Operation %d wants to map a StreamSegment to a Parent StreamSegment Id that does not exist. " +
                                "" + "Parent StreamSegmentId = %d, Transaction Name = %s.",
                        operation.getSequenceNumber(), operation.getParentStreamSegmentId(),
                        operation.getStreamSegmentName()));
            }

            // Verify StreamSegment Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamSegmentId(operation.getStreamSegmentName());
            if (existingStreamId != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(String.format(
                        "Operation %d wants to map a Transaction StreamSegment Name that is already mapped in the " +
                                "metadata. Transaction Name = '%s', Existing Id = %d.",
                        operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamId));
            }

            // Assign the SegmentId.
            if (!this.containerMetadata.isRecoveryMode()) {
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
                        log.info(
                                "{}: Skipping recovering MetadataCheckpointOperation with SequenceNumber {} because "
                                        + "we already have metadata changes.",
                                this.traceObjectId, operation.getSequenceNumber());
                        return;
                    }

                    log.info("{}: Recovering MetadataCheckpointOperation with SequenceNumber {}.", this.traceObjectId,
                            operation.getSequenceNumber());
                    rollback();
                    deserializeFrom(operation);
                    this.processedCheckpoint = true;
                } else {
                    // In non-Recovery Mode, a MetadataCheckpointOperation means we need to serialize the current
                    // state of
                    // the Metadata, both the base Container Metadata and the current Transaction.
                    serializeTo(operation);
                }
            } catch (IOException | SerializationException ex) {
                throw new MetadataUpdateException("Unable to process MetadataCheckpointOperation " + operation, ex);
            }
        }

        private void acceptMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
            if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(
                        "StreamSegmentMapOperation does not have a StreamSegmentId " + "assigned: " + operation
                                .toString());
            }

            // Create StreamSegment metadata here - we need to do this as part of the transaction.
            UpdateableSegmentMetadata streamSegmentMetadata = recordNewStreamSegment(operation.getStreamSegmentName(),
                    operation.getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID);
            streamSegmentMetadata.setStorageLength(operation.getLength());
            streamSegmentMetadata.setDurableLogLength(operation.getLength()); // DurableLogLength must be at least
            // StorageLength.
            if (operation.isSealed()) {
                // MapOperations represent the state of the StreamSegment in Storage. If it is sealed in storage, both
                // Seal flags need to be set.
                streamSegmentMetadata.markSealed();
                streamSegmentMetadata.markSealedInStorage();
            }
        }

        private void acceptMetadataOperation(TransactionMapOperation operation) throws MetadataUpdateException {
            if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException(
                        "TransactionMapOperation does not have a StreamSegmentId assigned: " + "" + operation
                                .toString());
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            UpdateableSegmentMetadata transactionMetadata = recordNewStreamSegment(operation.getStreamSegmentName(),
                    operation.getStreamSegmentId(), operation.getParentStreamSegmentId());
            transactionMetadata.setStorageLength(operation.getLength());
            transactionMetadata.setDurableLogLength(0);
            if (operation.isSealed()) {
                // MapOperations represent the state of the StreamSegment in Storage. If it is sealed in storage, both
                // Seal flags need to be set.
                transactionMetadata.markSealed();
                transactionMetadata.markSealedInStorage();
            }
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
            long existingStreamId = this.containerMetadata.getStreamSegmentId(streamSegmentName);
            if (existingStreamId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                existingStreamId = this.newStreamSegmentNames.getOrDefault(streamSegmentName,
                        ContainerMetadata.NO_STREAM_SEGMENT_ID);
            }

            return existingStreamId;
        }

        private long generateUniqueStreamSegmentId() {
            // The ContainerMetadata.SequenceNumber is always guaranteed to be unique (it's monotonically strict
            // increasing).
            // It can be safely used as a new unique Segment Id. If any clashes occur, just keep searching up until
            // we find
            // a non-used one.
            long streamSegmentId = Math.max(this.containerMetadata.getOperationSequenceNumber(),
                    ContainerMetadata.NO_STREAM_SEGMENT_ID + 1);
            while (this.newStreamSegments.containsKey(
                    streamSegmentId) || this.containerMetadata.getStreamSegmentMetadata(streamSegmentId) != null) {
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

        private UpdateableSegmentMetadata recordNewStreamSegment(String streamSegmentName, long streamSegmentId, long
                parentId) {
            UpdateableSegmentMetadata metadata;
            if (parentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId,
                        this.containerMetadata.getContainerId());
            } else {
                metadata = new StreamSegmentMetadata(streamSegmentName, streamSegmentId, parentId,
                        this.containerMetadata.getContainerId());
            }

            this.newStreamSegments.put(metadata.getId(), metadata);
            this.newStreamSegmentNames.put(metadata.getName(), metadata.getId());

            return metadata;
        }

        private void copySegmentMetadataToSource(Collection<UpdateableSegmentMetadata> newStreams,
                                                 Predicate<SegmentMetadata> filter) {
            for (SegmentMetadata newMetadata : newStreams) {
                if (!filter.test(newMetadata)) {
                    continue;
                }

                UpdateableSegmentMetadata existingMetadata;
                if (newMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                    existingMetadata = this.containerMetadata.mapStreamSegmentId(newMetadata.getName(),
                            newMetadata.getId(), newMetadata.getParentId());
                } else {
                    existingMetadata = this.containerMetadata.mapStreamSegmentId(newMetadata.getName(),
                            newMetadata.getId());
                }

                // Update real metadata with all the information from the new metadata.
                existingMetadata.copyFrom(newMetadata);
            }
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
            Preconditions.checkState(this.containerMetadata.isRecoveryMode(),
                    "Cannot deserialize Metadata in " + "recovery mode.");

            DataInputStream stream = new DataInputStream(operation.getContents().getReader());

            // 1. Version.
            byte version = stream.readByte();
            if (version != CURRENT_SERIALIZATION_VERSION) {
                throw new SerializationException("Metadata.deserialize",
                        String.format("Unsupported version: %d.", version));
            }

            // 2. Container id.
            int containerId = stream.readInt();
            if (this.containerMetadata.getContainerId() != containerId) {
                throw new SerializationException("Metadata.deserialize",
                        String.format("Invalid " + "StreamSegmentContainerId. Expected '%d', actual '%d'.",
                                this.containerMetadata.getContainerId(), containerId));
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

            // 54. New Stream Segments.
            segmentCount = stream.readInt();
            for (int i = 0; i < segmentCount; i++) {
                deserializeSegmentMetadata(stream);
            }
        }

        private void serializeTo(MetadataCheckpointOperation operation) throws IOException {
            assert operation != null : "operation is null";
            Preconditions.checkState(!this.containerMetadata.isRecoveryMode(),
                    "Cannot serialize Metadata in recovery" + " mode.");

            EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
            DataOutputStream stream = new DataOutputStream(byteStream);

            // 1. Version.
            stream.writeByte(CURRENT_SERIALIZATION_VERSION);

            // 2. Container Id.
            stream.writeInt(this.containerMetadata.getContainerId());

            // Intentionally skipping over the Sequence Number. There is no need for that here; it will be set on the
            // operation anyway when it gets serialized.

            // 3. Unchanged Segment Metadata.
            Collection<Long> unchangedSegmentIds = CollectionHelpers.filter(
                    this.containerMetadata.getAllStreamSegmentIds(),
                    segmentId -> !this.streamSegmentUpdates.containsKey(segmentId));
            stream.writeInt(unchangedSegmentIds.size());
            CollectionHelpers.forEach(unchangedSegmentIds,
                    segmentId -> serializeSegmentMetadata(this.containerMetadata.getStreamSegmentMetadata(segmentId),
                            stream));

            // 4. New StreamSegments.
            Collection<UpdateableSegmentMetadata> newSegments = CollectionHelpers.filter(
                    this.newStreamSegments.values(), sm -> !this.streamSegmentUpdates.containsKey(sm.getId()));
            stream.writeInt(newSegments.size());
            forEach(newSegments, sm -> serializeSegmentMetadata(sm, stream));

            // 5. Changed Segment Metadata.
            stream.writeInt(this.streamSegmentUpdates.size());
            CollectionHelpers.forEach(this.streamSegmentUpdates.values(), sm -> serializeSegmentMetadata(sm, stream));

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
            // S8. SealedInStorage.
            stream.writeBoolean(sm.isSealedInStorage());
            // S9. Deleted.
            stream.writeBoolean(sm.isDeleted());
            // S10. LastModified.
            stream.writeLong(sm.getLastModified().getTime());

            // TODO: determine if we want to snapshot the client ids and their offsets too. This might be a long
            // list, especially if we don't clean it up.
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

        private final UpdateableSegmentMetadata baseMetadata;
        private final boolean isRecoveryMode;
        private final AbstractMap<UUID, AppendContext> lastCommittedAppends;
        private long currentDurableLogLength;
        private boolean sealed;
        private boolean merged;
        private boolean deleted;
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
            this.lastCommittedAppends = new HashMap<>();
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
        public Date getLastModified() {
            return new Date(); //TODO: implement properly.
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
        public AppendContext getLastAppendContext(UUID clientId) {
            AppendContext result = this.lastCommittedAppends.getOrDefault(clientId, null);
            return result != null ? result : this.baseMetadata.getLastAppendContext(clientId);
        }

        @Override
        public Collection<UUID> getKnownClientIds() {
            return this.lastCommittedAppends.keySet();
        }

        //endregion

        //region Pre-Processing

        /**
         * Pre-processes a StreamSegmentAppendOperation.
         * After this method returns, the given operation will have its StreamSegmentOffset property set to the
         * current StreamSegmentLength.
         *
         * @param operation The operation to pre-process.
         * @throws StreamSegmentSealedException If the StreamSegment is sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws BadOffsetException           If the operation has an assigned offset, but it doesn't match the
         *                                      current
         *                                      Segment DurableLogOffset.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException,
                StreamSegmentMergedException, BadOffsetException, BadEventNumberException {
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
                    // If the Operation already has an offset assigned, verify that it matches the current end offset
                    // of the Segment.
                    if (operationOffset != this.currentDurableLogLength) {
                        throw new BadOffsetException(this.baseMetadata.getName(), this.currentDurableLogLength,
                                operationOffset);
                    }
                } else {
                    // No pre-assigned offset. Put the Append at the end of the Segment.
                    operation.setStreamSegmentOffset(this.currentDurableLogLength);
                }

                // Context Event-Number check (must be monotonically increasing).
                AppendContext currentContext = operation.getAppendContext();
                if (currentContext != null) {
                    AppendContext lastContext = getLastAppendContext(operation.getAppendContext().getClientId());
                    if (lastContext != null && currentContext.getEventNumber() <= lastContext.getEventNumber()) {
                        throw new BadEventNumberException(this.baseMetadata.getName(), lastContext.getEventNumber(),
                                currentContext.getEventNumber());
                    }
                }
            }
        }

        /**
         * Pre-processes a StreamSegmentSealOperation.
         * After this method returns, the operation will have its StreamSegmentLength property set to the current
         * length of the StreamSegment.
         *
         * @param operation The Operation.
         * @throws StreamSegmentSealedException If the StreamSegment is already sealed.
         * @throws StreamSegmentMergedException If the StreamSegment is merged into another.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        void preProcessOperation(StreamSegmentSealOperation operation) throws StreamSegmentSealedException,
                StreamSegmentMergedException {
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
         * After this method returns, the operation will have its TargetStreamSegmentOffset set to the length of the
         * Parent StreamSegment.
         *
         * @param operation           The operation to pre-process.
         * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
         * @throws StreamSegmentSealedException If the parent stream is already sealed.
         * @throws MetadataUpdateException      If the operation cannot be processed because of the current state of
         *                                      the metadata.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        void preProcessAsParentSegment(MergeTransactionOperation operation, TemporaryStreamSegmentMetadata
                transactionMetadata) throws StreamSegmentSealedException, MetadataUpdateException {
            ensureSegmentId(operation);

            if (this.sealed) {
                // We do not allow merging into sealed Segments.
                throw new StreamSegmentSealedException(this.baseMetadata.getName());
            }

            if (this.baseMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                throw new MetadataUpdateException("Cannot merge a StreamSegment into a Transaction StreamSegment.");
            }

            // Check that the Transaction has been properly sealed and has its length set.
            if (!transactionMetadata.isSealed()) {
                throw new MetadataUpdateException("Transaction StreamSegment to be merged needs to be sealed.");
            }

            long transLength = operation.getLength();
            if (transLength < 0) {
                throw new MetadataUpdateException(
                        "MergeTransactionOperation does not have its Transaction " + "StreamSegment Length set.");
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
        void preProcessAsTransactionSegment(MergeTransactionOperation operation) throws MetadataUpdateException,
                StreamSegmentMergedException {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(), "operation",
                    "Invalid Operation Transaction StreamSegment Id.");

            if (this.merged) {
                throw new StreamSegmentMergedException(this.baseMetadata.getName());
            }

            if (!this.sealed) {
                throw new MetadataUpdateException("Transaction StreamSegment to be merged needs to be sealed.");
            }

            if (!this.isRecoveryMode) {
                operation.setLength(this.currentDurableLogLength);
            }
        }

        //endregion

        //region AcceptOperation

        /**
         * Accepts a StreamSegmentAppendOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation StreamSegmentOffset is different from the current
         *                                  StreamSegment Length.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        void acceptOperation(StreamSegmentAppendOperation operation) throws MetadataUpdateException {
            ensureSegmentId(operation);
            if (operation.getStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(
                        String.format("StreamSegmentAppendOperation offset mismatch. " + "Expected %d, actual %d.",
                                this.currentDurableLogLength, operation.getStreamSegmentOffset()));
            }

            this.currentDurableLogLength += operation.getData().length;
            this.lastCommittedAppends.put(operation.getAppendContext().getClientId(), operation.getAppendContext());
            this.isChanged = true;
        }

        /**
         * Accepts a StreamSegmentSealOperation in the metadata.
         *
         * @param operation The operation to accept.
         * @throws MetadataUpdateException  If the operation hasn't been pre-processed.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        void acceptOperation(StreamSegmentSealOperation operation) throws MetadataUpdateException {
            ensureSegmentId(operation);
            if (operation.getStreamSegmentOffset() < 0) {
                throw new MetadataUpdateException(
                        "StreamSegmentSealOperation cannot be accepted if it hasn't been " + "pre-processed.");
            }

            this.sealed = true;
            this.isChanged = true;
        }

        /**
         * Accepts the given MergeTransactionOperation as a Parent StreamSegment.
         *
         * @param operation           The operation to accept.
         * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
         * @throws MetadataUpdateException  If the operation cannot be processed because of the current state of the
         *                                  metadata.
         * @throws IllegalArgumentException If the operation is for a different stream.
         */
        void acceptAsParentSegment(MergeTransactionOperation operation, TemporaryStreamSegmentMetadata
                transactionMetadata) throws MetadataUpdateException {
            ensureSegmentId(operation);

            if (operation.getStreamSegmentOffset() != this.currentDurableLogLength) {
                throw new MetadataUpdateException(
                        String.format("MergeTransactionOperation target offset mismatch. " + "Expected %d, actual %d.",
                                this.currentDurableLogLength, operation.getStreamSegmentOffset()));
            }

            long transLength = operation.getLength();
            if (transLength < 0 || transLength != transactionMetadata.currentDurableLogLength) {
                throw new MetadataUpdateException(
                        "MergeTransactionOperation does not seem to have been pre-processed" + ".");
            }

            this.currentDurableLogLength += transLength;
            this.isChanged = true;
        }

        /**
         * Accepts the given operation as a Transaction Stream Segment.
         *
         * @param operation The operation
         * @throws IllegalArgumentException If the operation is for a different stream segment.
         */
        void acceptAsTransactionSegment(MergeTransactionOperation operation) {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(), "operation",
                    "Invalid Operation Transaction StreamSegment Id.");

            this.sealed = true;
            this.merged = true;
            this.isChanged = true;
        }

        //endregion

        //region Operations

        /**
         * Applies all the outstanding changes to the base StreamSegmentMetadata object.
         */
        public void apply() {
            if (!this.isChanged) {
                // No changes made.
                return;
            }

            // Apply to base metadata.
            this.lastCommittedAppends.values().forEach(this.baseMetadata::recordAppendContext);
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

        private void ensureSegmentId(StorageOperation operation) {
            Exceptions.checkArgument(this.baseMetadata.getId() == operation.getStreamSegmentId(), "operation",
                    "Invalid Log Operation StreamSegment Id.");
        }

        //endregion
    }

    //endregion
}
