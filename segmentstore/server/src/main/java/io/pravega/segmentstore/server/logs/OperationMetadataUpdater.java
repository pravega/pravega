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

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.LogAddress;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Transaction-based Metadata Updater for Log Operations.
 */
@Slf4j
@NotThreadSafe
class OperationMetadataUpdater implements ContainerMetadata {
    //region Members

    static long MIN_CHECKPOINT = Long.MIN_VALUE;
    static long MAX_CHECKPOINT = Long.MAX_VALUE;

    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private ContainerMetadataUpdateTransaction currentTransaction;

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
        ContainerMetadataUpdateTransaction transaction = this.currentTransaction;
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
        ContainerMetadataUpdateTransaction transaction = this.currentTransaction;
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
     * Creates a checkpoint and returns its id.
     *
     * @return The new checkpoint id.
     */
    long createCheckpoint() {
        return 0;
    }

    /**
     * Commits all outstanding changes to the base Container Metadata, across all checkpoints.
     *
     * @return True if anything was committed, false otherwise.
     */
    boolean commit() {
        return commit(MAX_CHECKPOINT);
    }

    /**
     * Commits all outstanding changes to the base Container Metadata, up to and including the one for the given checkpoint.
     * @param upToCheckpointId  The Id of the checkpoint up to which to commit.
     *
     * @return True if anything was committed, false otherwise.
     */
    boolean commit(long upToCheckpointId) {
        // TODO: implement checkpoints.
        log.trace("{}: Commit (CheckPoint = {}, Anything = {}).", this.traceObjectId, upToCheckpointId, this.currentTransaction != null);
        if (this.currentTransaction == null) {
            return false;
        }

        this.currentTransaction.commit();
        this.currentTransaction = null;
        return true;
    }

    /**
     * Discards any outstanding changes, across all checkpoints.
     */
    void rollback() {
        rollback(MIN_CHECKPOINT);
    }

    /**
     * Discards any outstanding changes, starting at the given checkpoint forward.
     *
     * @param fromCheckpointId The Id of the checkpoint from which to rollback.
     */
    void rollback(long fromCheckpointId) {
        // TODO: implement checkpoints.
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

    private ContainerMetadataUpdateTransaction getCurrentTransaction() {
        // TODO: remove - new transactions are made only with checkpoints.
        if (this.currentTransaction == null) {
            this.currentTransaction = new ContainerMetadataUpdateTransaction(this.metadata, this.traceObjectId);
        }

        return this.currentTransaction;
    }

    //endregion
}
