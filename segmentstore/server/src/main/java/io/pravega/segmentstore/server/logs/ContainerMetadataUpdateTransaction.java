/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.AttributeSerializer;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.SegmentOperation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapping;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.TransactionMapOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * An update transaction that can apply changes to a ContainerMetadata.
 */
@Slf4j
@NotThreadSafe
class ContainerMetadataUpdateTransaction implements ContainerMetadata {
    // region Members

    private static final byte CURRENT_SERIALIZATION_VERSION = 0;
    /**
     * Pointer to the real (live) ContainerMetadata. Used when needing access to live information (such as Storage Info).
     */
    private final ContainerMetadata realMetadata;
    private final HashMap<Long, SegmentMetadataUpdateTransaction> segmentUpdates;
    private final HashMap<Long, UpdateableSegmentMetadata> newSegments;
    private final HashMap<String, Long> newSegmentNames;
    private final List<Long> newTruncationPoints;
    @Getter
    private final int containerId;
    @Getter
    private final boolean recoveryMode;
    @Getter
    private int maximumActiveSegmentCount;
    private int baseNewSegmentCount;
    /**
     * The base ContainerMetadata on top of which this UpdateTransaction is based. This isn't necessarily the same as
     * realMetadata (above) and this should be used when relying on information that is updated via transactions.
     */
    private ContainerMetadata baseMetadata;
    private long newSequenceNumber;
    @Getter
    private final long transactionId;
    private final String traceObjectId;
    private boolean processedCheckpoint;
    @Getter
    private boolean sealed; // This refers to the UpdateTransaction, and not to the individual Segment's status.

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerMetadataUpdateTransaction class.
     *
     * @param baseMetadata The base Container Metadata.
     * @param transactionId Id of the ContainerMetadataUpdateTransaction.
     */
    ContainerMetadataUpdateTransaction(ContainerMetadata baseMetadata, ContainerMetadata realMetadata, long transactionId) {
        this.baseMetadata = Preconditions.checkNotNull(baseMetadata, "baseMetadata");
        this.realMetadata = Preconditions.checkNotNull(realMetadata, "realMetadata");
        this.transactionId = transactionId;
        this.containerId = this.baseMetadata.getContainerId();
        this.recoveryMode = this.baseMetadata.isRecoveryMode();
        this.maximumActiveSegmentCount = this.baseMetadata.getMaximumActiveSegmentCount();
        this.baseNewSegmentCount = getNewSegmentCount(baseMetadata);

        this.traceObjectId = String.format("MetadataUpdate[%d-%d]", this.containerId, transactionId);
        this.segmentUpdates = new HashMap<>();
        this.newTruncationPoints = new ArrayList<>();
        this.newSegments = new HashMap<>();
        this.newSegmentNames = new HashMap<>();
        this.sealed = false;
        resetNewSequenceNumber();
    }

    //endregion

    //region ContainerMetadata Implementation

    @Override
    public long getContainerEpoch() {
        throw new UnsupportedOperationException("getContainerEpoch() is not supported in " + getClass().getName());
    }

    @Override
    public long getOperationSequenceNumber() {
        Preconditions.checkState(!isRecoveryMode(), "GetOperationSequenceNumber cannot be invoked in recovery mode.");
        return this.realMetadata.getOperationSequenceNumber();
    }

    @Override
    public long getStreamSegmentId(String segmentName, boolean updateLastUsed) {
        long existingSegmentId = this.newSegmentNames.getOrDefault(segmentName, ContainerMetadata.NO_STREAM_SEGMENT_ID);
        if (existingSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            existingSegmentId = this.baseMetadata.getStreamSegmentId(segmentName, false);
        }

        return existingSegmentId;
    }

    @Override
    public SegmentMetadata getStreamSegmentMetadata(long segmentId) {
        SegmentMetadata sm = this.segmentUpdates.getOrDefault(segmentId, null);
        if (sm == null) {
            sm = this.newSegments.getOrDefault(segmentId, null);
            if (sm == null) {
                sm = this.baseMetadata.getStreamSegmentMetadata(segmentId);
            }
        }

        return sm;
    }

    @Override
    public Collection<Long> getAllStreamSegmentIds() {
        Collection<Long> baseSegmentIds = this.baseMetadata.getAllStreamSegmentIds();
        ArrayList<Long> result = new ArrayList<>(baseSegmentIds.size() + this.newSegments.size());
        result.addAll(baseSegmentIds);
        result.addAll(this.newSegments.keySet());
        return result;
    }

    @Override
    public int getActiveSegmentCount() {
        return this.realMetadata.getActiveSegmentCount() + getNewSegmentCount();
    }

    /**
     * Gets the total number of new segments from this UpdateTransaction and all base UpdateTransactions.
     */
    private int getNewSegmentCount() {
        return this.baseNewSegmentCount + this.newSegments.size();
    }

    /**
     * Gets the total number of new segments from the given metadata.
     *
     * @param baseMetadata The metadata to query.
     * @return The total number of new segments, if the argument is a ContainerMetadataUpdateTransaction, or 0 otherwise.
     */
    private int getNewSegmentCount(ContainerMetadata baseMetadata) {
        if (baseMetadata instanceof ContainerMetadataUpdateTransaction) {
            return ((ContainerMetadataUpdateTransaction) baseMetadata).getNewSegmentCount();
        }

        return 0;
    }

    //endregion

    //region Operations

    /**
     * Seals this UpdateTransaction and disallows any further changes on it. After this, the only operations that can
     * work are rebase() and commit().
     */
    void seal() {
        this.sealed = true;
    }

    /**
     * Rebases this UpdateTransaction to the given ContainerMetadata.
     *
     * @param baseMetadata The ContainerMetadata to rebase onto.
     */
    void rebase(ContainerMetadata baseMetadata) {
        Preconditions.checkArgument(baseMetadata.getContainerId() == this.containerId, "ContainerId mismatch");
        Preconditions.checkArgument(baseMetadata.isRecoveryMode() == this.isRecoveryMode(), "isRecoveryMode mismatch");
        this.baseMetadata = baseMetadata;
        this.maximumActiveSegmentCount = baseMetadata.getMaximumActiveSegmentCount();
        this.baseNewSegmentCount = getNewSegmentCount(baseMetadata);
        resetNewSequenceNumber();
    }

    /**
     * Commits all pending changes to the given target Container Metadata.
     * @param target The UpdateableContainerMetadata to commit to.
     */
    void commit(UpdateableContainerMetadata target) {
        Preconditions.checkArgument(target.getContainerId() == this.containerId, "ContainerId mismatch");
        Preconditions.checkArgument(target.isRecoveryMode() == this.isRecoveryMode(), "isRecoveryMode mismatch");

        if (target.isRecoveryMode()) {
            if (this.processedCheckpoint) {
                // If we processed a checkpoint during recovery, we need to wipe the metadata clean. We are setting
                // a brand new one.
                target.reset();
            }

            // RecoverableMetadata.reset() cleaned up the Operation Sequence number. We need to set it back to whatever
            // we have in our UpdateTransaction. If we have nothing, we'll just set it to the default.
            assert this.newSequenceNumber >= ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER
                    : "Invalid Sequence Number " + this.newSequenceNumber;
            target.setOperationSequenceNumber(this.newSequenceNumber);
        }

        // Commit all segment-related transactional changes to their respective sources.
        this.segmentUpdates.values().forEach(txn -> {
            UpdateableSegmentMetadata targetSegmentMetadata = target.getStreamSegmentMetadata(txn.getId());
            if (targetSegmentMetadata == null) {
                targetSegmentMetadata = this.newSegments.get(txn.getId());
            }

            txn.apply(targetSegmentMetadata);
        });

        // We must first copy the Standalone Segments, and then the Transaction Segments. That's because
        // the Transaction Segments may refer to one of these newly created Segments, and the metadata
        // will reject the operation if it can't find the parent.
        // We need this because HashMap does not necessarily preserve order when iterating via values().
        copySegmentMetadata(this.newSegments.values(), s -> !s.isTransaction(), target);
        copySegmentMetadata(this.newSegments.values(), SegmentMetadata::isTransaction, target);

        // Copy truncation points.
        this.newTruncationPoints.forEach(target::setValidTruncationPoint);

        // We are done. Clear the transaction.
        clear();
    }

    /**
     * Clears this UpdateTransaction of all changes.
     */
    @VisibleForTesting
    void clear() {
        this.segmentUpdates.clear();
        this.newSegments.clear();
        this.newSegmentNames.clear();
        this.newTruncationPoints.clear();
        this.processedCheckpoint = false;
        resetNewSequenceNumber();
    }

    private void resetNewSequenceNumber() {
        if (this.baseMetadata.isRecoveryMode()) {
            this.newSequenceNumber = ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER;
        } else {
            this.newSequenceNumber = Long.MIN_VALUE;
        }
    }

    //endregion

    //region Log Operation Processing

    /**
     * Sets the new Operation Sequence Number.
     *
     * @param value The new Operation Sequence number.
     */
    void setOperationSequenceNumber(long value) {
        checkNotSealed();
        Preconditions.checkState(this.recoveryMode, "Cannot set Sequence Number because ContainerMetadata is not in recovery mode.");
        this.newSequenceNumber = value;
    }

    /**
     * Pre-processes the given Operation. See OperationMetadataUpdater.preProcessOperation for more details on behavior.
     *
     * @param operation The operation to pre-process.
     * @throws ContainerException     If the given operation was rejected given the current state of the container metadata.
     * @throws StreamSegmentException If the given operation was incompatible with the current state of the Segment.
     *                                For example: StreamSegmentNotExistsException, StreamSegmentSealedException or
     *                                StreamSegmentMergedException.
     */
    void preProcessOperation(Operation operation) throws ContainerException, StreamSegmentException {
        checkNotSealed();
        SegmentMetadataUpdateTransaction segmentMetadata = null;
        if (operation instanceof SegmentOperation) {
            segmentMetadata = getSegmentUpdateTransaction(((SegmentOperation) operation).getStreamSegmentId());
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
                SegmentMetadataUpdateTransaction transactionMetadata = getSegmentUpdateTransaction(mbe.getTransactionSegmentId());
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
        checkNotSealed();
        SegmentMetadataUpdateTransaction segmentMetadata = null;
        if (operation instanceof SegmentOperation) {
            segmentMetadata = getSegmentUpdateTransaction(((SegmentOperation) operation).getStreamSegmentId());
            segmentMetadata.setLastUsed(operation.getSequenceNumber());
        }

        if (operation instanceof StorageOperation) {
            if (operation instanceof StreamSegmentAppendOperation) {
                segmentMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
            } else if (operation instanceof StreamSegmentSealOperation) {
                segmentMetadata.acceptOperation((StreamSegmentSealOperation) operation);
            } else if (operation instanceof MergeTransactionOperation) {
                MergeTransactionOperation mto = (MergeTransactionOperation) operation;
                SegmentMetadataUpdateTransaction transactionMetadata = getSegmentUpdateTransaction(mto.getTransactionSegmentId());
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
        // Verify that the segment is not already mapped. If it is mapped, then it needs to have the exact same
        // segment id as the one the operation is trying to set.
        checkExistingMapping(operation);
        assignUniqueSegmentId(operation);
    }

    private void preProcessMetadataOperation(TransactionMapOperation operation) throws ContainerException {
        // Verify Parent Segment Exists.
        SegmentMetadata parentMetadata = getExistingMetadata(operation.getParentStreamSegmentId());
        if (parentMetadata == null) {
            throw new MetadataUpdateException(this.containerId,
                    String.format("Operation %d wants to map a Segment to a Parent Segment Id that does " +
                                    "not exist. Parent SegmentId = %d, Transaction Name = %s.",
                            operation.getSequenceNumber(), operation.getParentStreamSegmentId(), operation.getStreamSegmentName()));
        }

        // Verify that the segment is not already mapped. If it is mapped, then it needs to have the exact same
        // segment id as the one the operation is trying to set.
        checkExistingMapping(operation);
        assignUniqueSegmentId(operation);
    }

    private void processMetadataOperation(MetadataCheckpointOperation operation) throws MetadataUpdateException {
        try {
            if (this.recoveryMode) {
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
                clear();
                deserializeFrom(operation);
                this.processedCheckpoint = true;
            } else {
                // In non-Recovery Mode, a MetadataCheckpointOperation means we need to serialize the current state of
                // the Metadata, both the base Container Metadata and the current Transaction.
                serializeTo(operation);
            }
        } catch (IOException | SerializationException ex) {
            throw new MetadataUpdateException(this.containerId, "Unable to process MetadataCheckpointOperation " + operation, ex);
        }
    }

    private void processMetadataOperation(StorageMetadataCheckpointOperation operation) throws MetadataUpdateException {
        try {
            if (this.recoveryMode) {
                updateFrom(operation);
            } else {
                serializeTo(operation);
            }
        } catch (IOException | SerializationException ex) {
            throw new MetadataUpdateException(this.containerId, "Unable to process StorageMetadataCheckpointOperation " + operation, ex);
        }
    }

    private void acceptMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            throw new MetadataUpdateException(this.containerId,
                    "StreamSegmentMapOperation does not have a SegmentId assigned: " + operation);
        }

        // Create or reuse an existing Segment Metadata.
        UpdateableSegmentMetadata segmentMetadata = getOrCreateSegmentUpdateTransaction(operation.getStreamSegmentName(),
                operation.getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID);
        updateMetadata(operation, segmentMetadata);
    }

    private void acceptMetadataOperation(TransactionMapOperation operation) throws MetadataUpdateException {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            throw new MetadataUpdateException(this.containerId,
                    "TransactionMapOperation does not have a SegmentId assigned: " + operation);
        }

        // Create or reuse an existing Transaction Metadata.
        UpdateableSegmentMetadata transactionMetadata = getOrCreateSegmentUpdateTransaction(operation.getStreamSegmentName(),
                operation.getStreamSegmentId(), operation.getParentStreamSegmentId());
        updateMetadata(operation, transactionMetadata);
    }

    private void updateMetadata(StreamSegmentMapping mapping, UpdateableSegmentMetadata metadata) {
        metadata.setStorageLength(mapping.getLength());

        // DurableLogLength must be at least StorageLength.
        metadata.setDurableLogLength(Math.max(mapping.getLength(), metadata.getDurableLogLength()));
        if (mapping.isSealed()) {
            // MapOperations represent the state of the Segment in Storage. If it is sealed in storage, both
            // Seal flags need to be set.
            metadata.markSealed();
            metadata.markSealedInStorage();
        }

        metadata.updateAttributes(mapping.getAttributes());
    }

    //endregion

    //region Helpers

    private void checkExistingMapping(StreamSegmentMapping operation) throws MetadataUpdateException {
        long existingSegmentId = getStreamSegmentId(operation.getStreamSegmentName(), false);
        if (existingSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID
                && existingSegmentId != operation.getStreamSegmentId()) {
            throw new MetadataUpdateException(this.containerId,
                    String.format("Operation '%s' wants to map a Segment that is already mapped in the metadata. Existing Id = %d.",
                            operation, existingSegmentId));
        }
    }

    private void assignUniqueSegmentId(StreamSegmentMapping mapping) throws TooManyActiveSegmentsException {
        if (!this.recoveryMode) {
            if (getActiveSegmentCount() >= this.maximumActiveSegmentCount) {
                throw new TooManyActiveSegmentsException(this.containerId, this.maximumActiveSegmentCount);
            }

            // Assign the SegmentId, but only in non-recovery mode and only if not already assigned.
            if (mapping.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                mapping.setStreamSegmentId(generateUniqueSegmentId());
            }
        }
    }

    private long generateUniqueSegmentId() {
        // The ContainerMetadata.SequenceNumber is always guaranteed to be unique (it's monotonically strict increasing).
        // It can be safely used as a new unique Segment Id. If any clashes occur, just keep searching up until we find
        // a non-used one.
        long segmentId = this.realMetadata.getOperationSequenceNumber();
        while (this.newSegments.containsKey(segmentId) || this.baseMetadata.getStreamSegmentMetadata(segmentId) != null) {
            segmentId++;
        }

        assert segmentId >= ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER : "Invalid generated SegmentId";
        return segmentId;
    }

    /**
     * Gets an UpdateableSegmentMetadata for the given segment id, if it is already registered in the base metadata
     * or added as a new segment in this transaction.
     */
    private SegmentMetadata getExistingMetadata(long segmentId) {
        SegmentMetadata sm = this.baseMetadata.getStreamSegmentMetadata(segmentId);
        if (sm == null) {
            sm = this.newSegments.getOrDefault(segmentId, null);
        }

        return sm;
    }

    /**
     * Gets all pending changes for the given Segment.
     *
     * @param segmentId The Id of the Segment to query.
     * @throws MetadataUpdateException If no metadata entry exists for the given Segment Id.
     */
    private SegmentMetadataUpdateTransaction getSegmentUpdateTransaction(long segmentId) throws MetadataUpdateException {
        SegmentMetadataUpdateTransaction tsm = tryGetSegmentUpdateTransaction(segmentId);
        if (tsm == null) {
            throw new MetadataUpdateException(this.containerId, String.format("No metadata entry exists for Segment Id %d.", segmentId));
        }

        return tsm;
    }

    /**
     * Gets an UpdateableSegmentMetadata for the given Segment. If already registered, it returns that instance,
     * otherwise it creates and records a new Segment metadata.
     */
    private SegmentMetadataUpdateTransaction getOrCreateSegmentUpdateTransaction(String segmentName, long segmentId, long parentId) {
        SegmentMetadataUpdateTransaction sm = tryGetSegmentUpdateTransaction(segmentId);
        if (sm == null) {
            SegmentMetadata baseSegmentMetadata = createSegmentMetadata(segmentName, segmentId, parentId);
            sm = new SegmentMetadataUpdateTransaction(baseSegmentMetadata, this.recoveryMode);
            this.segmentUpdates.put(segmentId, sm);
        }

        return sm;
    }

    /**
     * Attempts to get a SegmentMetadataUpdateTransaction for an existing or new Segment.
     *
     * @param segmentId The Id of the Segment to retrieve.
     * @return An instance of SegmentMetadataUpdateTransaction, or null if no such segment exists.
     */
    private SegmentMetadataUpdateTransaction tryGetSegmentUpdateTransaction(long segmentId) {
        SegmentMetadataUpdateTransaction sm = this.segmentUpdates.getOrDefault(segmentId, null);
        if (sm == null) {
            SegmentMetadata baseSegmentMetadata = this.baseMetadata.getStreamSegmentMetadata(segmentId);
            if (baseSegmentMetadata == null) {
                baseSegmentMetadata = this.newSegments.getOrDefault(segmentId, null);
            }

            if (baseSegmentMetadata != null) {
                sm = new SegmentMetadataUpdateTransaction(baseSegmentMetadata, this.recoveryMode);
                this.segmentUpdates.put(segmentId, sm);
            }
        }

        return sm;
    }

    /**
     * Creates a new UpdateableSegmentMetadata for the given Segment and registers it.
     */
    private UpdateableSegmentMetadata createSegmentMetadata(String segmentName, long segmentId, long parentId) {
        UpdateableSegmentMetadata metadata;
        if (parentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            metadata = new StreamSegmentMetadata(segmentName, segmentId, this.containerId);
        } else {
            metadata = new StreamSegmentMetadata(segmentName, segmentId, parentId, this.containerId);
        }

        this.newSegments.put(metadata.getId(), metadata);
        this.newSegmentNames.put(metadata.getName(), metadata.getId());

        return metadata;
    }

    private void copySegmentMetadata(Collection<UpdateableSegmentMetadata> newSegments, Predicate<SegmentMetadata> filter,
                                     UpdateableContainerMetadata target) {
        for (SegmentMetadata newMetadata : newSegments) {
            if (!filter.test(newMetadata)) {
                continue;
            }

            UpdateableSegmentMetadata existingMetadata;
            if (newMetadata.isTransaction()) {
                existingMetadata = target.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId(), newMetadata.getParentId());
            } else {
                existingMetadata = target.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId());
            }

            // Update real metadata with all the information from the new metadata.
            existingMetadata.copyFrom(newMetadata);
        }
    }

    private void checkNotSealed() {
        Preconditions.checkState(!this.sealed, "%s has been sealed and can no longer accept changes.",
                this.traceObjectId);
    }

    //endregion

    //region Serialization

    /**
     * Deserializes the Metadata from the given stream.
     *
     * @param operation The MetadataCheckpointOperation to deserialize from.
     * @throws IOException            If the stream threw one.
     * @throws SerializationException If the given Stream is an invalid metadata serialization.
     * @throws IllegalStateException  If the Metadata is not in Recovery Mode.
     */
    private void deserializeFrom(MetadataCheckpointOperation operation) throws IOException, SerializationException {
        Preconditions.checkState(this.recoveryMode, "Cannot deserialize Metadata in non-recovery mode.");

        DataInputStream stream = new DataInputStream(new GZIPInputStream(operation.getContents().getReader()));

        // 1. Version.
        byte version = stream.readByte();
        if (version != CURRENT_SERIALIZATION_VERSION) {
            throw new SerializationException("Metadata.deserialize", String.format("Unsupported version: %d.", version));
        }

        // 2. Container id.
        int containerId = stream.readInt();
        if (this.containerId != containerId) {
            throw new SerializationException("Metadata.deserialize",
                    String.format("Invalid ContainerId. Expected '%d', actual '%d'.", this.containerId, containerId));
        }

        // This is not retrieved from serialization, but rather from the operation itself.
        setOperationSequenceNumber(operation.getSequenceNumber());

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
        Preconditions.checkState(this.recoveryMode, "Cannot bulk-update Metadata in non-recovery mode.");

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
        Preconditions.checkState(!this.recoveryMode, "Cannot serialize Metadata in recovery mode.");

        EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
        GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
        DataOutputStream stream = new DataOutputStream(zipStream);

        // 1. Version.
        stream.writeByte(CURRENT_SERIALIZATION_VERSION);

        // 2. Container Id.
        stream.writeInt(this.containerId);

        // Intentionally skipping over the Sequence Number. There is no need for that here; it will be set on the
        // operation anyway when it gets serialized.

        // 3. Unchanged Segment Metadata.
        Collection<Long> unchangedSegmentIds = this.baseMetadata
                .getAllStreamSegmentIds().stream()
                .filter(segmentId -> !this.segmentUpdates.containsKey(segmentId))
                .collect(Collectors.toList());
        stream.writeInt(unchangedSegmentIds.size());
        unchangedSegmentIds.forEach(segmentId -> serializeSegmentMetadata(this.baseMetadata.getStreamSegmentMetadata(segmentId), stream));

        // 4. New Segments.
        Collection<UpdateableSegmentMetadata> newSegments = this.newSegments
                .values().stream()
                .filter(sm -> !this.segmentUpdates.containsKey(sm.getId()))
                .collect(Collectors.toList());
        stream.writeInt(newSegments.size());
        newSegments.forEach(sm -> serializeSegmentMetadata(sm, stream));

        // 5. Changed Segment Metadata.
        stream.writeInt(this.segmentUpdates.size());
        this.segmentUpdates.values().forEach(sm -> serializeSegmentMetadata(sm, stream));

        zipStream.finish();
        operation.setContents(byteStream.getData());
    }

    private void serializeTo(StorageMetadataCheckpointOperation operation) throws IOException {
        assert operation != null : "operation is null";
        Preconditions.checkState(!this.recoveryMode, "Cannot serialize Metadata in recovery mode.");

        EnhancedByteArrayOutputStream byteStream = new EnhancedByteArrayOutputStream();
        GZIPOutputStream zipStream = new GZIPOutputStream(byteStream);
        DataOutputStream stream = new DataOutputStream(zipStream);

        // 1. Version.
        stream.writeByte(CURRENT_SERIALIZATION_VERSION);

        // 2. Segment Metadata. We want to snapshot the actual metadata, and not this transaction. Hence we use realMetadata here.
        Collection<Long> segmentIds = this.realMetadata.getAllStreamSegmentIds();
        stream.writeInt(segmentIds.size());
        segmentIds.forEach(segmentId -> serializeStorageSegmentMetadata(this.realMetadata.getStreamSegmentMetadata(segmentId), stream));

        zipStream.finish();
        operation.setContents(byteStream.getData());
    }

    @SneakyThrows(IOException.class)
    private void serializeSegmentMetadata(SegmentMetadata sm, DataOutputStream stream) {
        // S1. SegmentId.
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
        // S1. SegmentId.
        long segmentId = stream.readLong();
        // S2. ParentId.
        long parentId = stream.readLong();
        // S3. Name.
        String name = stream.readUTF();

        UpdateableSegmentMetadata metadata = getOrCreateSegmentUpdateTransaction(name, segmentId, parentId);

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
        // S1. SegmentId.
        stream.writeLong(sm.getId());
        // S2. StorageLength.
        stream.writeLong(sm.getStorageLength());
        // S3. SealedInStorage.
        stream.writeBoolean(sm.isSealedInStorage());
        // S4. Deleted.
        stream.writeBoolean(sm.isDeleted());
    }

    private void deserializeStorageSegmentMetadata(DataInputStream stream) throws IOException, MetadataUpdateException {
        // S1. SegmentId.
        long segmentId = stream.readLong();
        SegmentMetadataUpdateTransaction metadata = getSegmentUpdateTransaction(segmentId);
        // S2. StorageLength.
        long storageLength = stream.readLong();
        // S3. SealedInStorage.
        boolean sealedInStorage = stream.readBoolean();
        // S4. Deleted.
        boolean deleted = stream.readBoolean();
        metadata.updateStorageState(storageLength, sealedInStorage, deleted);
    }

    //endregion
}
