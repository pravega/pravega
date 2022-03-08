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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.ContainerException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.TooManyActiveSegmentsException;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.containers.StreamSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.CheckpointOperationBase;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Data;
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

    private static final MetadataCheckpointSerializer METADATA_CHECKPOINT_SERIALIZER = new MetadataCheckpointSerializer();
    private static final MetadataCheckpointIncrementalDeserializer METADATA_CHECKPOINT_INCREMENTAL_DESERIALIZER = new MetadataCheckpointIncrementalDeserializer();
    private static final StorageCheckpointSerializer STORAGE_CHECKPOINT_SERIALIZER = new StorageCheckpointSerializer();
    /**
     * Pointer to the real (live) ContainerMetadata. Used when needing access to live information (such as Storage Info).
     */
    private final ContainerMetadata realMetadata;
    private final HashMap<Long, SegmentMetadataUpdateTransaction> segmentUpdates;
    private final HashMap<Long, StreamSegmentMetadata> newSegments;
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

        // Copy all Segment Metadata
        copySegmentMetadata(this.newSegments.values(), target);

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

    private boolean isNewSegment(long segmentId) {
        return this.newSegments.containsKey(segmentId);
    }

    private void removeNewSegment(long segmentId) {
        assert isRecoveryMode();
        val sm = this.newSegments.remove(segmentId);
        if (sm != null) {
            sm.markInactive();
            this.newSegmentNames.remove(sm.getName());
            val ut = this.segmentUpdates.remove(segmentId);
            if (ut != null) {
                ut.setActive(false);
            }
        }
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
        if (operation instanceof SegmentOperation) {
            val segmentMetadata = getSegmentUpdateTransaction(((SegmentOperation) operation).getStreamSegmentId());
            if (segmentMetadata.isDeleted()) {
                throw new StreamSegmentNotExistsException(segmentMetadata.getName());
            }
            if (operation instanceof StreamSegmentAppendOperation) {
                segmentMetadata.preProcessOperation((StreamSegmentAppendOperation) operation);
            } else if (operation instanceof StreamSegmentSealOperation) {
                segmentMetadata.preProcessOperation((StreamSegmentSealOperation) operation);
            } else if (operation instanceof MergeSegmentOperation) {
                MergeSegmentOperation mbe = (MergeSegmentOperation) operation;
                SegmentMetadataUpdateTransaction sourceMetadata = getSegmentUpdateTransaction(mbe.getSourceSegmentId());
                sourceMetadata.preProcessAsSourceSegment(mbe);
                segmentMetadata.preProcessAsTargetSegment(mbe, sourceMetadata);
            } else if (operation instanceof UpdateAttributesOperation) {
                segmentMetadata.preProcessOperation((UpdateAttributesOperation) operation);
            } else if (operation instanceof StreamSegmentTruncateOperation) {
                segmentMetadata.preProcessOperation((StreamSegmentTruncateOperation) operation);
            } else if (operation instanceof DeleteSegmentOperation) {
                segmentMetadata.preProcessOperation((DeleteSegmentOperation) operation);
            }
        }

        if (operation instanceof MetadataCheckpointOperation) {
            // MetadataCheckpointOperations do not require preProcess and accept; they can be handled in a single stage.
            processMetadataOperation((MetadataCheckpointOperation) operation);
        } else if (operation instanceof StorageMetadataCheckpointOperation) {
            // StorageMetadataCheckpointOperation do not require preProcess and accept; they can be handled in a single stage.
            processMetadataOperation((StorageMetadataCheckpointOperation) operation);
        } else if (operation instanceof StreamSegmentMapOperation) {
            preProcessMetadataOperation((StreamSegmentMapOperation) operation);
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
        if (operation instanceof SegmentOperation) {
            val segmentMetadata = getSegmentUpdateTransaction(((SegmentOperation) operation).getStreamSegmentId());
            segmentMetadata.setLastUsed(operation.getSequenceNumber());
            if (operation instanceof StreamSegmentAppendOperation) {
                segmentMetadata.acceptOperation((StreamSegmentAppendOperation) operation);
            } else if (operation instanceof StreamSegmentSealOperation) {
                segmentMetadata.acceptOperation((StreamSegmentSealOperation) operation);
            } else if (operation instanceof MergeSegmentOperation) {
                MergeSegmentOperation mto = (MergeSegmentOperation) operation;
                SegmentMetadataUpdateTransaction sourceMetadata = getSegmentUpdateTransaction(mto.getSourceSegmentId());
                sourceMetadata.acceptAsSourceSegment(mto);
                sourceMetadata.setLastUsed(operation.getSequenceNumber());
                segmentMetadata.acceptAsTargetSegment(mto, sourceMetadata);
            } else if (operation instanceof UpdateAttributesOperation) {
                segmentMetadata.acceptOperation((UpdateAttributesOperation) operation);
            } else if (operation instanceof StreamSegmentTruncateOperation) {
                segmentMetadata.acceptOperation((StreamSegmentTruncateOperation) operation);
            } else if (operation instanceof DeleteSegmentOperation) {
                segmentMetadata.acceptOperation((DeleteSegmentOperation) operation);
            }
        }

        if (operation instanceof CheckpointOperationBase) {
            if (operation instanceof MetadataCheckpointOperation) {
                // A MetadataCheckpointOperation represents a valid truncation point. Record it as such.
                this.newTruncationPoints.add(operation.getSequenceNumber());
            }

            // Checkpoint operation has been serialized and we no longer need its contents. Clear it and release any
            // memory it used.
            ((CheckpointOperationBase) operation).clearContents();
        } else if (operation instanceof StreamSegmentMapOperation) {
            acceptMetadataOperation((StreamSegmentMapOperation) operation);
        }
    }

    private void preProcessMetadataOperation(StreamSegmentMapOperation operation) throws ContainerException {
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
                    // A MetadataCheckpoint should be processed fully only if is the first checkpoint encountered.
                    // Any subsequent MetadataCheckpoints should be partially processed and applied.
                    // But we can (should) only process at most one MetadataCheckpoint per recovery. Any additional
                    // ones are redundant (used just for Truncation purposes) and contain the same information as
                    // if we processed every operation in order, up to them.
                    log.info("MetadataUpdate[{}]: Recovering MetadataCheckpointOperation({}) and applying incrementally.",
                            this.containerId, operation.getSequenceNumber());
                    METADATA_CHECKPOINT_INCREMENTAL_DESERIALIZER.deserialize(operation.getContents(), this);
                } else {
                    log.info("MetadataUpdate[{}]: Recovering MetadataCheckpointOperation({}).",
                            this.containerId, operation.getSequenceNumber());
                    clear();

                    METADATA_CHECKPOINT_SERIALIZER.deserialize(operation.getContents(), this);
                    this.processedCheckpoint = true;
                }

                // This is not retrieved from serialization, but rather from the operation itself.
                setOperationSequenceNumber(operation.getSequenceNumber());
            } else {
                // In non-Recovery Mode, a MetadataCheckpointOperation means we need to serialize the current state of
                // the Metadata, both the base Container Metadata and the current Transaction.
                operation.setContents(METADATA_CHECKPOINT_SERIALIZER.serialize(this));
            }
        } catch (IOException ex) {
            throw new MetadataUpdateException(this.containerId, "Unable to process MetadataCheckpointOperation " + operation, ex);
        }
    }

    private void processMetadataOperation(StorageMetadataCheckpointOperation operation) throws MetadataUpdateException {
        try {
            if (this.recoveryMode) {
                STORAGE_CHECKPOINT_SERIALIZER.deserialize(operation.getContents(), this);
            } else {
                operation.setContents(STORAGE_CHECKPOINT_SERIALIZER.serialize(this));
            }
        } catch (IOException ex) {
            throw new MetadataUpdateException(this.containerId, "Unable to process StorageMetadataCheckpointOperation " + operation, ex);
        }
    }

    private void acceptMetadataOperation(StreamSegmentMapOperation operation) throws MetadataUpdateException {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            throw new MetadataUpdateException(this.containerId,
                    "StreamSegmentMapOperation does not have a SegmentId assigned: " + operation);
        }

        // Create or reuse an existing Segment Metadata.
        UpdateableSegmentMetadata segmentMetadata = getOrCreateSegmentUpdateTransaction(
                operation.getStreamSegmentName(), operation.getStreamSegmentId());
        updateMetadata(operation, segmentMetadata);
    }

    private void updateMetadata(StreamSegmentMapOperation mapping, UpdateableSegmentMetadata metadata) {
        metadata.setStorageLength(mapping.getLength());

        // Length must be at least StorageLength.
        metadata.setLength(Math.max(mapping.getLength(), metadata.getLength()));

        // StartOffset must be a value in the interval [0, Length] (closed at both ends).
        if (metadata.getLength() > 0) {
            metadata.setStartOffset(Math.min(mapping.getStartOffset(), metadata.getLength()));
        }

        if (mapping.isSealed()) {
            metadata.markSealed();
        }

        // Pin this to memory if needed.
        if (mapping.isPinned()) {
            metadata.markPinned();
        }

        metadata.updateAttributes(mapping.getAttributes());
        metadata.refreshDerivedProperties();
    }

    //endregion

    //region Helpers

    private void checkExistingMapping(StreamSegmentMapOperation operation) throws MetadataUpdateException {
        long existingSegmentId = getStreamSegmentId(operation.getStreamSegmentName(), false);
        if (existingSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID
                && existingSegmentId != operation.getStreamSegmentId()) {
            throw new MetadataUpdateException(this.containerId,
                    String.format("Operation '%s' wants to map a Segment that is already mapped in the metadata. Existing Id = %d.",
                            operation, existingSegmentId));
        }
    }

    private void assignUniqueSegmentId(StreamSegmentMapOperation mapping) throws TooManyActiveSegmentsException {
        if (!this.recoveryMode) {
            if (getActiveSegmentCount() >= this.maximumActiveSegmentCount && !mapping.isPinned()) {
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
    private SegmentMetadataUpdateTransaction getOrCreateSegmentUpdateTransaction(String segmentName, long segmentId) {
        SegmentMetadataUpdateTransaction sm = tryGetSegmentUpdateTransaction(segmentId);
        if (sm == null) {
            SegmentMetadata baseSegmentMetadata = createSegmentMetadata(segmentName, segmentId);
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
    private UpdateableSegmentMetadata createSegmentMetadata(String segmentName, long segmentId) {
        StreamSegmentMetadata metadata = new StreamSegmentMetadata(segmentName, segmentId, this.containerId);
        this.newSegments.put(metadata.getId(), metadata);
        this.newSegmentNames.put(metadata.getName(), metadata.getId());
        return metadata;
    }

    private void copySegmentMetadata(Collection<StreamSegmentMetadata> newSegments, UpdateableContainerMetadata target) {
        for (SegmentMetadata newMetadata : newSegments) {
            // Update real metadata with all the information from the new metadata.
            UpdateableSegmentMetadata existingMetadata = target.mapStreamSegmentId(newMetadata.getName(), newMetadata.getId());
            existingMetadata.copyFrom(newMetadata);
        }
    }

    private void checkNotSealed() {
        Preconditions.checkState(!this.sealed, "MetadataUpdate[%s-%s}] has been sealed and can no longer accept changes.",
                this.containerId, this.transactionId);
    }

    //endregion

    //region StorageCheckpointSerializer

    private static class StorageCheckpointSerializer extends VersionedSerializer.Direct<ContainerMetadataUpdateTransaction> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(ContainerMetadataUpdateTransaction t, RevisionDataOutput output) throws IOException {
            val toSerialize = t.realMetadata.getAllStreamSegmentIds().stream()
                    .map(t.realMetadata::getStreamSegmentMetadata)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            output.writeCollection(toSerialize, this::writeSegmentMetadata00);
        }

        private void read00(RevisionDataInput input, ContainerMetadataUpdateTransaction t) throws IOException {
            input.readCollection(s -> readSegmentMetadata00(s, t));
        }

        private void writeSegmentMetadata00(RevisionDataOutput output, SegmentMetadata sm) throws IOException {
            output.writeLong(sm.getId());
            output.writeLong(sm.getStorageLength());
            output.writeBoolean(sm.isSealedInStorage());
            output.writeBoolean(sm.isDeleted());
            output.writeBoolean(sm.isDeletedInStorage());
        }

        @SneakyThrows(MetadataUpdateException.class)
        private SegmentMetadata readSegmentMetadata00(RevisionDataInput input, ContainerMetadataUpdateTransaction t) throws IOException {
            long segmentId = input.readLong();
            SegmentMetadataUpdateTransaction metadata = t.getSegmentUpdateTransaction(segmentId);
            long storageLength = input.readLong();
            boolean sealedInStorage = input.readBoolean();
            boolean deleted = input.readBoolean();
            boolean deletedInStorage = input.readBoolean();
            metadata.updateStorageState(storageLength, sealedInStorage, deleted, deletedInStorage);
            return metadata;
        }
    }

    //endregion

    //region MetadataCheckpointSerializer

    private static class MetadataCheckpointSerializer extends VersionedSerializer.Direct<ContainerMetadataUpdateTransaction> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        protected void write00(ContainerMetadataUpdateTransaction t, RevisionDataOutput output) throws IOException {
            // Intentionally skipping over the Sequence Number. There is no need for that here; it will be set on the
            // operation anyway when it gets serialized.
            output.writeCompactInt(t.containerId);

            val toSerialize = new ArrayList<SegmentMetadata>();

            // Unchanged segments.
            t.baseMetadata.getAllStreamSegmentIds().stream()
                          .filter(segmentId -> !t.segmentUpdates.containsKey(segmentId))
                          .forEach(segmentId -> toSerialize.add(t.baseMetadata.getStreamSegmentMetadata(segmentId)));

            // New Segments.
            t.newSegments.values().stream()
                         .filter(sm -> !t.segmentUpdates.containsKey(sm.getId()))
                         .forEach(toSerialize::add);

            // 5. Changed Segment Metadata.
            toSerialize.addAll(t.segmentUpdates.values());
            output.writeCollection(toSerialize, this::writeSegmentMetadata00);
        }

        private void read00(RevisionDataInput input, ContainerMetadataUpdateTransaction t) throws IOException {
            int containerId = input.readCompactInt();
            if (t.containerId != containerId) {
                throw new SerializationException(String.format("Invalid ContainerId. Expected '%d', actual '%d'.", t.containerId, containerId));
            }

            Collection<UpdateableSegmentMetadata> checkpointMetadata = input.readCollection(s -> readSegmentMetadata00(s, t));
            postRead(checkpointMetadata, t);
        }

        private void writeSegmentMetadata00(RevisionDataOutput output, SegmentMetadata sm) throws IOException {
            output.writeLong(sm.getId());
            output.writeUTF(sm.getName());
            output.writeLong(sm.getLength());
            output.writeLong(sm.getStorageLength());
            output.writeBoolean(sm.isMerged());
            output.writeBoolean(sm.isSealed());
            output.writeBoolean(sm.isSealedInStorage());
            output.writeBoolean(sm.isDeleted());
            output.writeBoolean(sm.isDeletedInStorage());
            output.writeLong(sm.getLastModified().getTime());
            output.writeLong(sm.getStartOffset());

            // We only serialize Core Attributes. Extended Attributes can be retrieved from the AttributeIndex.
            output.writeMap(Attributes.getCoreNonNullAttributes(sm.getAttributes()), this::writeAttributeId00, RevisionDataOutput::writeLong);
        }

        private UpdateableSegmentMetadata readSegmentMetadata00(RevisionDataInput input, ContainerMetadataUpdateTransaction t) throws IOException {
            long segmentId = input.readLong();
            String name = input.readUTF();

            UpdateableSegmentMetadata metadata = getSegmentMetadata(name, segmentId, t);

            metadata.setLength(input.readLong());
            metadata.setStorageLength(input.readLong());
            boolean isMerged = input.readBoolean();
            if (isMerged) {
                metadata.markMerged();
            }

            boolean isSealed = input.readBoolean();
            if (isSealed) {
                metadata.markSealed();
            }

            boolean isSealedInStorage = input.readBoolean();
            if (isSealedInStorage) {
                metadata.markSealedInStorage();
            }

            boolean isDeleted = input.readBoolean();
            if (isDeleted) {
                metadata.markDeleted();
            }

            boolean isDeletedInStorage = input.readBoolean();
            if (isDeletedInStorage) {
                metadata.markDeletedInStorage();
            }

            ImmutableDate lastModified = new ImmutableDate(input.readLong());
            metadata.setLastModified(lastModified);
            metadata.setStartOffset(input.readLong());

            val attributes = input.readMap(this::readAttributeId00, RevisionDataInput::readLong);
            metadata.updateAttributes(attributes);
            return metadata;
        }

        private void writeAttributeId00(RevisionDataOutput out, AttributeId attributeId) throws IOException {
            assert attributeId.isUUID();
            out.writeLong(attributeId.getBitGroup(0));
            out.writeLong(attributeId.getBitGroup(1));
        }

        private AttributeId readAttributeId00(RevisionDataInput in) throws IOException {
            return AttributeId.uuid(in.readLong(), in.readLong());
        }

        protected UpdateableSegmentMetadata getSegmentMetadata(String name, long segmentId, ContainerMetadataUpdateTransaction t) {
            return t.getOrCreateSegmentUpdateTransaction(name, segmentId);
        }

        protected void postRead(Collection<UpdateableSegmentMetadata> checkpointMetadata, ContainerMetadataUpdateTransaction t) {
            // This method intentionally left blank. Will be overridden in derived classes.
        }
    }

    //endregion

    //region MetadataCheckpointPartialDeserializer

    private static class MetadataCheckpointIncrementalDeserializer extends MetadataCheckpointSerializer {
        @Override
        protected void write00(ContainerMetadataUpdateTransaction t, RevisionDataOutput output) {
            throw new UnsupportedOperationException("MetadataCheckpointPartialDeserializer may not be used for serialization.");
        }

        @Override
        protected UpdateableSegmentMetadata getSegmentMetadata(String name, long segmentId, ContainerMetadataUpdateTransaction t) {
            return new PartialSegmentMetadata(name, segmentId);
        }

        @Override
        protected void postRead(Collection<UpdateableSegmentMetadata> checkpointMetadata, ContainerMetadataUpdateTransaction t) {
            Preconditions.checkState(t.isRecoveryMode(), "MetadataCheckpointPartialDeserializer can only be used in recovery mode.");

            // Index checkpointed metadata by segment id.
            val byId = checkpointMetadata.stream().collect(Collectors.toMap(SegmentMetadata::getId, m -> m));

            // Update any segment that we encountered. This includes unregistering a missing segment (eviction) or updating
            // its storage state as necessary.
            for (val segmentId : t.getAllStreamSegmentIds()) {
                val m = byId.getOrDefault(segmentId, null);
                if (m == null) {
                    val existingMetadata = t.getStreamSegmentMetadata(segmentId);
                    if (t.isNewSegment(segmentId) && existingMetadata != null && canUnregister(existingMetadata)) {
                        // This segment existed in our Update Transaction/Base Metadata, however this checkpoint no longer has it.
                        // This means that the segment has been evicted at one point between the last checkpoint and this one,
                        // so it should be safe to remove it from our list (if possible).
                        log.debug("MetadataUpdate[{}]: Un-mapping Segment Id {} because it is no longer present in a MetadataCheckpoint.", t.containerId, segmentId);
                        t.removeNewSegment(segmentId);
                    }
                } else {
                    // Update segment's state with latest info.
                    val segmentUpdate = t.getOrCreateSegmentUpdateTransaction(m.getName(), m.getId());
                    if (m.isSealedInStorage()) {
                        segmentUpdate.markSealed();
                    }

                    if (m.isDeletedInStorage()) {
                        segmentUpdate.markDeleted();
                    }

                    segmentUpdate.updateStorageState(m.getStorageLength(), m.isSealedInStorage(), m.isDeleted(), m.isDeletedInStorage());
                }
            }
        }

        private boolean canUnregister(SegmentMetadata existingMetadata) {
            return existingMetadata.isDeleted()
                    || existingMetadata.getStorageLength() >= existingMetadata.getLength();
        }

        @Data
        private static class PartialSegmentMetadata implements UpdateableSegmentMetadata {
            private final String name;
            private final long id;
            private long storageLength;
            private long startOffset;
            private long length;
            private boolean sealed;
            private boolean sealedInStorage;
            private boolean deleted;
            private boolean deletedInStorage;
            private boolean merged;

            @Override
            public void markSealed() {
                this.sealed = true;
            }

            @Override
            public void markSealedInStorage() {
                this.sealedInStorage = true;
            }

            @Override
            public void markDeleted() {
                this.deleted = true;
            }

            @Override
            public void markDeletedInStorage() {
                this.deletedInStorage = true;
            }

            @Override
            public void markMerged() {
                this.merged = true;
            }

            // region Unimplemented methods

            @Override
            public boolean isActive() {
                return true;
            }

            @Override
            public void updateAttributes(Map<AttributeId, Long> attributeValues) {
                // Not relevant here.
            }

            @Override
            public void setLastModified(ImmutableDate date) {
                // Not relevant here.
            }

            @Override
            public void copyFrom(SegmentMetadata other) {
                throw new UnsupportedOperationException("copyFrom not supported on " + PartialSegmentMetadata.class.getSimpleName());
            }

            @Override
            public void refreshDerivedProperties() {
                // Not relevant here.
            }

            @Override
            public int getContainerId() {
                return -1;
            }


            @Override
            public long getLastUsed() {
                return 0;
            }

            @Override
            public void setLastUsed(long value) {
                // Not relevant here.
            }

            @Override
            public SegmentProperties getSnapshot() {
                return this;
            }

            @Override
            public void markPinned() {
                // Not relevant here.
            }

            @Override
            public boolean isPinned() {
                return false;
            }

            @Override
            public ImmutableDate getLastModified() {
                return null;
            }

            @Override
            public Map<AttributeId, Long> getAttributes() {
                return Collections.emptyMap();
            }

            @Override
            public Map<AttributeId, Long> getAttributes(BiPredicate<AttributeId, Long> filter) {
                return Collections.emptyMap();
            }

            @Override
            public SegmentType getType() {
                return null;
            }

            @Override
            public int getAttributeIdLength() {
                return 0;
            }

            //endregion
        }
    }

    //endregion
}
