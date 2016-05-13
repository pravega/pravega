package com.emc.logservice.logs;

import com.emc.logservice.logs.operations.*;
import com.emc.logservice.*;

import java.util.HashMap;

/**
 * Transaction-based Metadata Updater for Log Operations.
 */
public class OperationMetadataUpdater implements StreamSegmentMetadataSource {
    //region Members

    private final StreamSegmentContainerMetadata metadata;
    private UpdateTransaction currentTransaction;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationMetadataUpdater class.
     *
     * @param metadata The Container Metadata to update.
     * @throws NullPointerException If the metadata is null.
     */
    public OperationMetadataUpdater(StreamSegmentContainerMetadata metadata) {
        if (metadata == null) {
            throw new NullPointerException("metadata");
        }

        this.metadata = metadata;
        this.currentTransaction = null;
    }

    //endregion

    //region Transactions

    /**
     * Commits all outstanding changes to the base Container Metadata.
     *
     * @return True if anything was committed, false otherwise.
     */
    public boolean commit() {
        if (this.currentTransaction == null) {
            return false;
        }

        this.currentTransaction.commit();
        rollback();
        return true;
    }

    /**
     * Discards any outstanding changes.
     */
    public void rollback() {
        this.currentTransaction = null;
    }

    /**
     * Commits a Truncation Marker directly to the base Metadata.
     *
     * @param truncationMarker The Truncation Marker to commit.
     * @throws NullPointerException If the truncationMarker is null.
     */
    public void commitTruncationMarker(TruncationMarker truncationMarker) {
        this.metadata.recordTruncationMarker(truncationMarker);
    }

    //endregion

    //region OperationMetadataUpdater

    @Override
    public ReadOnlyStreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) {
        UpdateTransaction transaction = this.currentTransaction;
        if (transaction == null) {
            return null;
        }

        try {
            return transaction.getStreamSegmentMetadata(streamSegmentId);
        }
        catch (MetadataUpdateException ex) {
            return null;
        }
    }

    //endregion

    //region Processing

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return
     */
    public long getNewOperationSequenceNumber() {
        return this.metadata.getNewOperationSequenceNumber();
    }

    /**
     * Processes the given Metadata Operation and records it in the pending transaction.
     * This method only works if the base Container Metadata is in Recovery Mode.
     *
     * @param operation The operation to process.
     * @throws MetadataUpdateException If the given operation was rejected given the current state of the Metadata.
     * @throws IllegalStateException   If the base Container Metadata is not in Recovery Mode.
     * @throws NullPointerException    If the operation is null.
     */
    public void processMetadataOperation(MetadataOperation operation) throws MetadataUpdateException {
        if (!this.metadata.isRecoveryMode()) {
            throw new IllegalStateException("Cannot process MetadataOperation in non-recovery mode.");
        }

        getCurrentTransaction().processMetadataOperation(operation);
    }

    /**
     * Phase 1/2 of processing a Storage Operation. The Operation is validated against the base Container Metadata and
     * the pending transaction and it is updated accordingly (if needed).
     *
     * @param operation The operation to pre-process.
     * @throws MetadataUpdateException      If the given operation was rejected given the current state of the metadata.
     * @throws StreamSegmentSealedException If the given operation was for a StreamSegment that was previously sealed and
     *                                      that is incompatible with a sealed stream
     * @throws NullPointerException         If the operation is null.
     */
    public void preProcessOperation(StorageOperation operation) throws MetadataUpdateException, StreamSegmentSealedException {
        TemporaryStreamSegmentMetadata streamMetadata = getCurrentTransaction().getStreamSegmentMetadata(operation.getStreamSegmentId());
        if (operation instanceof StreamSegmentAppendOperation) {
            streamMetadata.preProcessOperation((StreamSegmentAppendOperation) operation);
        }
        else if (operation instanceof StreamSegmentSealOperation) {
            streamMetadata.preProcessOperation((StreamSegmentSealOperation) operation);
        }
        else if (operation instanceof MergeBatchOperation) {
            MergeBatchOperation mbe = (MergeBatchOperation) operation;
            TemporaryStreamSegmentMetadata batchStreamMetadata = getCurrentTransaction().getStreamSegmentMetadata(mbe.getBatchStreamSegmentId());
            batchStreamMetadata.preProcessAsBatchStreamSegment(mbe);
            streamMetadata.preProcessAsParentStreamSegment(mbe, batchStreamMetadata);
        }
    }

    /**
     * Phase 2/2 of processing a Storage Operation. The Operation's effects are reflected in the pending transaction.
     *
     * @param operation The operation to accept.
     * @throws MetadataUpdateException If the given operation was rejected given the current state of the metadata.
     * @throws NullPointerException    If the operation is null.
     */
    public void acceptOperation(StorageOperation operation) throws MetadataUpdateException {
        TemporaryStreamSegmentMetadata streamMetadata = getCurrentTransaction().getStreamSegmentMetadata(operation.getStreamSegmentId());
        if (operation instanceof StreamSegmentAppendOperation) {
            streamMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
        }
        else if (operation instanceof StreamSegmentSealOperation) {
            streamMetadata.acceptOperation((StreamSegmentSealOperation) operation);
        }
        else if (operation instanceof MergeBatchOperation) {
            MergeBatchOperation mbe = (MergeBatchOperation) operation;
            TemporaryStreamSegmentMetadata batchStreamMetadata = this.currentTransaction.getStreamSegmentMetadata(mbe.getBatchStreamSegmentId());
            batchStreamMetadata.acceptAsBatchStreamSegment(mbe);
            streamMetadata.acceptAsParentStreamSegment(mbe, batchStreamMetadata);
        }
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
    private class UpdateTransaction {
        private final HashMap<Long, TemporaryStreamSegmentMetadata> streamUpdates;
        private final HashMap<Long, StreamSegmentMetadata> newStreams;
        private final HashMap<String, Long> newStreamsNames;
        private final StreamSegmentContainerMetadata containerMetadata;

        /**
         * Creates a new instance of the UpdateTransaction class.
         *
         * @param containerMetadata The base Container Metadata.
         */
        public UpdateTransaction(StreamSegmentContainerMetadata containerMetadata) {
            this.streamUpdates = new HashMap<>();
            this.containerMetadata = containerMetadata;
            if (containerMetadata.isRecoveryMode()) {
                this.newStreams = new HashMap<>();
                this.newStreamsNames = new HashMap<>();
            }
            else {
                this.newStreams = null;
                this.newStreamsNames = null;
            }
        }

        /**
         * Commits all pending changes to the base Container Metadata.
         */
        public void commit() {
            // Commit all temporary changes to their respective sources.
            this.streamUpdates.values().forEach(TemporaryStreamSegmentMetadata::apply);

            // If we are in recovery mode, add new stream metadata to the container metadata.
            if (this.containerMetadata.isRecoveryMode()) {
                for (StreamSegmentMetadata newMetadata : this.newStreams.values()) {
                    //TODO: should we check (again?) if the container metadata has knowledge of this stream?
                    if (newMetadata.getParentId() != StreamSegmentContainerMetadata.NoStreamSegmentId) {
                        this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId(), newMetadata.getParentId());
                    }
                    else {
                        this.containerMetadata.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId());
                    }

                    StreamSegmentMetadata existingMetadata = this.containerMetadata.getStreamSegmentMetadata(newMetadata.getId());
                    existingMetadata.setStorageLength(newMetadata.getStorageLength());
                    existingMetadata.setDurableLogLength(newMetadata.getDurableLogLength());
                    if (newMetadata.isSealed()) {
                        existingMetadata.markSealed();
                    }

                    if (newMetadata.isDeleted()) {
                        existingMetadata.markDeleted();
                    }
                }
            }
        }

        /**
         * Gets all pending changes for the given StreamSegment.
         *
         * @param streamSegmentId
         * @return The result
         * @throws MetadataUpdateException If no metadata entry exists for the given StreamSegment Id.
         */
        public TemporaryStreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId) throws MetadataUpdateException {
            TemporaryStreamSegmentMetadata tsm = this.streamUpdates.getOrDefault(streamSegmentId, null);
            if (tsm == null) {
                StreamSegmentMetadata streamSegmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
                if (streamSegmentMetadata == null) {
                    if (this.newStreams != null) {
                        streamSegmentMetadata = this.newStreams.getOrDefault(streamSegmentId, null);
                        if (streamSegmentMetadata == null) {
                            throw new MetadataUpdateException(String.format("No metadata entry exists for StreamSegment Id %d.", streamSegmentId));
                        }
                    }
                }

                tsm = new TemporaryStreamSegmentMetadata(streamSegmentMetadata, this.containerMetadata.isRecoveryMode());
                this.streamUpdates.put(streamSegmentId, tsm);
            }

            return tsm;
        }

        /**
         * Processes the given Metadata Operation and records it in the transaction.
         *
         * @param operation The operation to process.
         * @throws MetadataUpdateException If the given operation was rejected given the current state of the Metadata.
         */
        public void processMetadataOperation(MetadataOperation operation) throws MetadataUpdateException {
            if (operation instanceof StreamSegmentMapOperation) {
                processMetadataOperation((StreamSegmentMapOperation) operation);
            }
            else if (operation instanceof BatchMapOperation) {
                processMetadataOperation((BatchMapOperation) operation);
            }
            else if (operation instanceof MetadataPersistedOperation) {
                processMetadataOperation((MetadataPersistedOperation) operation);
            }
        }

        private void processMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
            // Verify Stream does not exist.
            StreamSegmentMetadata sm = getExistingMetadata(operation.getStreamSegmentId());
            if (sm != null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Stream Id that is already mapped in the metadata. Entry: %d->'%s', Metadata: %d->'%s'.", operation.getSequenceNumber(), operation.getStreamSegmentId(), operation.getStreamSegmentName(), sm.getId(), sm.getName()));
            }

            // Verify Stream Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamId(operation.getStreamSegmentName());
            if (existingStreamId != StreamSegmentContainerMetadata.NoStreamSegmentId) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Stream Name that is already mapped in the metadata. Stream Name = '%s', Existing Id = %d, New Id = %d.", operation.getSequenceNumber(), operation.getStreamSegmentName(), existingStreamId, operation.getStreamSegmentId()));
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            sm = new StreamSegmentMetadata(operation.getStreamSegmentName(), operation.getStreamSegmentId());
            sm.setStorageLength(operation.getStreamSegmentLength());
            sm.setDurableLogLength(0);
            this.newStreams.put(sm.getId(), sm);
            this.newStreamsNames.put(sm.getName(), sm.getId());
        }

        private void processMetadataOperation(BatchMapOperation entry) throws MetadataUpdateException {
            // Verify Parent Stream Exists.
            StreamSegmentMetadata parentMetadata = getExistingMetadata(entry.getParentStreamSegmentId());
            if (parentMetadata == null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Stream to a Parent Stream Id that does not exist. Parent Stream Id = %d, Batch Stream Id = %d, Batch Stream Name = %s.", entry.getSequenceNumber(), entry.getParentStreamSegmentId(), entry.getBatchStreamSegmentId(), entry.getBatchStreamSegmentName()));
            }

            // Verify Batch Stream does not exist.
            StreamSegmentMetadata batchStreamSegmentMetadata = getExistingMetadata(entry.getBatchStreamSegmentId());
            if (batchStreamSegmentMetadata != null) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Batch Stream Id that is already mapped in the metadata. Entry: %d->'%s', Metadata: %d->'%s'.", entry.getSequenceNumber(), entry.getBatchStreamSegmentId(), entry.getBatchStreamSegmentName(), batchStreamSegmentMetadata.getId(), batchStreamSegmentMetadata.getName()));
            }

            // Verify Stream Name is not already mapped somewhere else.
            long existingStreamId = getExistingStreamId(entry.getBatchStreamSegmentName());
            if (existingStreamId != StreamSegmentContainerMetadata.NoStreamSegmentId) {
                throw new MetadataUpdateException(String.format("Operation %d wants to map a Batch Stream Name that is already mapped in the metadata. Stream Name = '%s', Existing Id = %d, New Id = %d.", entry.getSequenceNumber(), entry.getBatchStreamSegmentName(), existingStreamId, entry.getBatchStreamSegmentId()));
            }

            // Create stream metadata here - we need to do this as part of the transaction.
            batchStreamSegmentMetadata = new StreamSegmentMetadata(entry.getBatchStreamSegmentName(), entry.getBatchStreamSegmentId(), entry.getParentStreamSegmentId());
            this.newStreams.put(batchStreamSegmentMetadata.getId(), batchStreamSegmentMetadata);
            this.newStreamsNames.put(batchStreamSegmentMetadata.getName(), batchStreamSegmentMetadata.getId());
        }

        private void processMetadataOperation(MetadataPersistedOperation entry) {
            // TODO: verify metadata integrity. Check that whatever we have in this transaction matches the current state of the metadata.
            // Everything up to here has been persisted in some other media. Therefore whatever we currently have in
            // our transaction is irrelevant (probably even obsolete). Discard it.
            rollback();
        }

        private long getExistingStreamId(String streamName) {
            long existingStreamId = this.containerMetadata.getStreamSegmentId(streamName);
            if (existingStreamId == StreamSegmentContainerMetadata.NoStreamSegmentId) {
                existingStreamId = this.newStreamsNames.getOrDefault(streamName, StreamSegmentContainerMetadata.NoStreamSegmentId);
            }

            return existingStreamId;
        }

        private StreamSegmentMetadata getExistingMetadata(long streamId) {
            StreamSegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamId);
            if (sm == null) {
                sm = this.newStreams.getOrDefault(streamId, null);
            }

            return sm;
        }
    }

    //endregion

    //region TemporaryStreamSegmentMetadata

    /**
     * Pending StreamSegment Metadata.
     */
    private class TemporaryStreamSegmentMetadata implements ReadOnlyStreamSegmentMetadata {
        //region Members
        private final StreamSegmentMetadata streamSegmentMetadata;
        private final boolean isRecoveryMode;
        private long currentDurableLogLength;
        private boolean sealed;
        private int changeCount;

        //endregion

        //region Constructor

        /**
         * Creates a new instance of the TemporaryStreamSegmentMetadata class.
         *
         * @param streamSegmentMetadata The base StreamSegment Metadata.
         * @param isRecoveryMode        Whether the metadata is currently in recovery model
         */
        public TemporaryStreamSegmentMetadata(StreamSegmentMetadata streamSegmentMetadata, boolean isRecoveryMode) {
            this.streamSegmentMetadata = streamSegmentMetadata;
            this.isRecoveryMode = isRecoveryMode;
            this.currentDurableLogLength = this.streamSegmentMetadata.getDurableLogLength();
            this.sealed = this.streamSegmentMetadata.isSealed();
            this.changeCount = 0;
        }

        //endregion

        //region ReadOnlyStreamSegmentMetadata Implementation

        @Override
        public String getName() {
            return this.streamSegmentMetadata.getName();
        }

        @Override
        public long getId() {
            return this.streamSegmentMetadata.getId();
        }

        @Override
        public long getParentId() {
            return this.streamSegmentMetadata.getParentId();
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
        public boolean isSealed() {
            return this.sealed;
        }

        @Override
        public boolean isDeleted() {
            return false;
        }

        //endregion

        //region StreamSegmentAppendOperation

        /**
         * Pre-processes a StreamSegmentAppendOperation.
         * After this method returns, the given operation will have its StreamSegmentOffset property set to the current StreamSegmentLength.
         *
         * @param operation The operation to pre-process.
         * @throws StreamSegmentSealedException If the StreamSegment is sealed.
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        public void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException {
            ensureStreamId(operation);
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
         * @throws IllegalArgumentException     If the operation is for a different stream.
         */
        public void preProcessOperation(StreamSegmentSealOperation operation) throws StreamSegmentSealedException {
            ensureStreamId(operation);
            if (this.sealed) {
                // We do not allow re-sealing an already sealed stream.
                throw new StreamSegmentSealedException(this.streamSegmentMetadata.getName());
            }

            if (!isRecoveryMode) {
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

            if (this.streamSegmentMetadata.getParentId() != StreamSegmentContainerMetadata.NoStreamSegmentId) {
                throw new MetadataUpdateException("Cannot merge a StreamSegment into a Batch StreamSegment.");
            }

            // Check that the batch has been properly sealed and has its length set.
            if (!batchStreamMetadata.isSealed()) {
                throw new MetadataUpdateException("Batch Stream to be merged needs to be sealed.");
            }

            long batchLength = operation.getBatchStreamSegmentLength();
            if (batchLength < 0) {
                throw new MetadataUpdateException("MergeBatchOperation does not have its Batch Stream Length set.");
            }

            if (!isRecoveryMode) {
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
         * @throws IllegalArgumentException If the operation is for a different stream segment.
         * @throws MetadataUpdateException  If the StreamSegment is already merged.
         */
        public void preProcessAsBatchStreamSegment(MergeBatchOperation operation) throws MetadataUpdateException {
            if (this.streamSegmentMetadata.getId() != operation.getBatchStreamSegmentId()) {
                throw new IllegalArgumentException("Invalid Log Operation Batch Stream Segment Id.");
            }

            if (this.sealed) {
                throw new MetadataUpdateException("Batch StreamSegment is already merged.");
            }

            if (!isRecoveryMode) {
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
            if (this.streamSegmentMetadata.getId() != operation.getBatchStreamSegmentId()) {
                throw new IllegalArgumentException("Invalid Log Operation Batch Stream Segment Id.");
            }

            this.sealed = true;
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
            this.streamSegmentMetadata.setDurableLogLength(this.currentDurableLogLength);
            if (this.isSealed()) {
                this.streamSegmentMetadata.markSealed();
            }
        }

        private void ensureStreamId(StorageOperation operation) {
            if (this.streamSegmentMetadata.getId() != operation.getStreamSegmentId()) {
                throw new IllegalArgumentException("Invalid Log Operation Stream Id.");
            }
        }

        //endregion
    }

    //endregion
}
