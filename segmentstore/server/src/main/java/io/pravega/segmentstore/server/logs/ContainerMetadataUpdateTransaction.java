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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
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
import io.pravega.segmentstore.storage.LogAddress;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * An update transaction that can apply changes to a ContainerMetadata.
 */
@Slf4j
@NotThreadSafe
class ContainerMetadataUpdateTransaction {
    // region Members

    private static final byte CURRENT_SERIALIZATION_VERSION = 0;
    private final HashMap<Long, SegmentMetadataUpdateTransaction> streamSegmentUpdates;
    private final HashMap<Long, UpdateableSegmentMetadata> newStreamSegments;
    private final HashMap<String, Long> newStreamSegmentNames;
    private final List<Long> newTruncationPoints;
    private final HashMap<Long, LogAddress> newTruncationMarkers;
    private ContainerMetadata containerMetadata;
    private final AtomicLong newSequenceNumber;
    private final String traceObjectId;
    private boolean processedCheckpoint;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerMetadataUpdateTransaction class.
     *
     * @param containerMetadata The base Container Metadata.
     */
    ContainerMetadataUpdateTransaction(UpdateableContainerMetadata containerMetadata, String traceObjectId) {
        this.containerMetadata = Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        this.traceObjectId = traceObjectId;
        this.streamSegmentUpdates = new HashMap<>();
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

    //endregion

    //region Operations

    /**
     * Commits all pending changes to the given target Container Metadata.
     * @param target The UpdateableContainerMetadata to commit to.
     */
    void commit(UpdateableContainerMetadata target) {
        Preconditions.checkArgument(target.getContainerId() == this.containerMetadata.getContainerId(),
                "ContainerId mismatch");

        if (target.isRecoveryMode()) {
            if (this.processedCheckpoint) {
                // If we processed a checkpoint during recovery, we need to wipe the metadata clean. We are setting
                // a brand new one.
                target.reset();
            }

            // Reset cleaned up the Operation Sequence number. We need to reset it to whatever we have in our transaction.
            // If we have nothing, we'll just set it to 0, which is what the default value was in the metadata too.
            target.setOperationSequenceNumber(this.newSequenceNumber.get());
        }

        // Commit all segment-related transactional changes to their respective sources.
        this.streamSegmentUpdates.values().forEach(txn -> {
            UpdateableSegmentMetadata targetSegmentMetadata = target.getStreamSegmentMetadata(txn.getId());
            txn.apply(targetSegmentMetadata);
        });

        // We must first copy the Standalone StreamSegments, and then the Transaction StreamSegments. That's because
        // the Transaction StreamSegments may refer to one of these newly created StreamSegments, and the metadata
        // will reject the operation if it can't find the parent.
        // We need this because HashMap does not necessarily preserve order when iterating via values().
        copySegmentMetadata(this.newStreamSegments.values(), s -> !s.isTransaction(), target);
        copySegmentMetadata(this.newStreamSegments.values(), SegmentMetadata::isTransaction, target);

        // Copy truncation markers.
        this.newTruncationMarkers.forEach(target::recordTruncationMarker);
        this.newTruncationPoints.forEach(target::setValidTruncationPoint);

        // We are done. Clear the transaction.
        clear();
    }

    /**
     * Records the given Truncation Marker Mapping.
     *
     * @param operationSequenceNumber The Sequence Number of the Operation that can be used as a truncation argument.
     * @param logAddress              The Address of the corresponding Data Frame that can be truncated (up to, and including).
     */
    void recordTruncationMarker(long operationSequenceNumber, LogAddress logAddress) {
        Exceptions.checkArgument(operationSequenceNumber >= 0, "operationSequenceNumber",
                "Operation Sequence Number must be a positive number.");
        Preconditions.checkNotNull(logAddress, "logAddress");
        this.newTruncationMarkers.put(operationSequenceNumber, logAddress);
    }

    private void clear() {
        this.streamSegmentUpdates.clear();
        this.newStreamSegments.clear();
        this.newStreamSegmentNames.clear();
        this.newTruncationMarkers.clear();
        this.newTruncationPoints.clear();
        this.processedCheckpoint = false;
    }

    //endregion

    //region Log Operation Processing

    /**
     * Sets the new Operation Sequence Number.
     *
     * @param value The new Operation Sequence number.
     */
    void setOperationSequenceNumber(long value) {
        Preconditions.checkState(this.newSequenceNumber != null,
                "Unable to set new Sequence Number. Most likely ContainerMetadata is not in recovery mode.");
        this.newSequenceNumber.set(value);
    }

    /**
     * Pre-processes the given Operation. See OperationMetadataUpdater.preProcessOperation for more details on behavior.
     *
     * @param operation The operation to pre-process.
     * @throws ContainerException     If the given operation was rejected given the current state of the container metadata.
     * @throws StreamSegmentException If the given operation was incompatible with the current state of the StreamSegment.
     *                                For example: StreamSegmentNotExistsException, StreamSegmentSealedException or
     *                                StreamSegmentMergedException.
     */
    void preProcessOperation(Operation operation) throws ContainerException, StreamSegmentException {
        SegmentMetadataUpdateTransaction segmentMetadata = null;
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
                SegmentMetadataUpdateTransaction transactionMetadata = getStreamSegmentMetadata(mbe.getTransactionSegmentId());
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
        SegmentMetadataUpdateTransaction segmentMetadata = null;
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
                SegmentMetadataUpdateTransaction transactionMetadata = getStreamSegmentMetadata(mto.getTransactionSegmentId());
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
        assignUniqueStreamSegmentId(operation);
    }

    private void preProcessMetadataOperation(TransactionMapOperation operation) throws ContainerException {
        // Verify Parent StreamSegment Exists.
        SegmentMetadata parentMetadata = getExistingMetadata(operation.getParentStreamSegmentId());
        if (parentMetadata == null) {
            throw new MetadataUpdateException(
                    this.containerMetadata.getContainerId(),
                    String.format(
                            "Operation %d wants to map a StreamSegment to a Parent StreamSegment Id that does " +
                                    "not exist. Parent StreamSegmentId = %d, Transaction Name = %s.",
                            operation.getSequenceNumber(), operation.getParentStreamSegmentId(), operation.getStreamSegmentName()));
        }

        // Verify that the segment is not already mapped. If it is mapped, then it needs to have the exact same
        // segment id as the one the operation is trying to set.
        checkExistingMapping(operation);
        assignUniqueStreamSegmentId(operation);
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
                clear();
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

        // Create or reuse an existing Segment Metadata.
        UpdateableSegmentMetadata segmentMetadata = getOrCreateSegmentMetadata(operation.getStreamSegmentName(),
                operation.getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID);
        updateMetadata(operation, segmentMetadata);
    }

    private void acceptMetadataOperation(TransactionMapOperation operation) throws MetadataUpdateException {
        if (operation.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                    "TransactionMapOperation does not have a StreamSegmentId assigned: " + operation.toString());
        }

        // Create or reuse an existing Transaction Metadata.
        UpdateableSegmentMetadata transactionMetadata = getOrCreateSegmentMetadata(operation.getStreamSegmentName(),
                operation.getStreamSegmentId(), operation.getParentStreamSegmentId());
        updateMetadata(operation, transactionMetadata);
    }

    private void updateMetadata(StreamSegmentMapping mapping, UpdateableSegmentMetadata metadata) {
        metadata.setStorageLength(mapping.getLength());

        // DurableLogLength must be at least StorageLength.
        metadata.setDurableLogLength(Math.max(mapping.getLength(), metadata.getDurableLogLength()));
        if (mapping.isSealed()) {
            // MapOperations represent the state of the StreamSegment in Storage. If it is sealed in storage, both
            // Seal flags need to be set.
            metadata.markSealed();
            metadata.markSealedInStorage();
        }

        metadata.updateAttributes(mapping.getAttributes());
    }

    //endregion

    //region Helpers

    /**
     * Gets all pending changes for the given StreamSegment.
     *
     * @param streamSegmentId The Id of the Segment to query.
     * @throws MetadataUpdateException If no metadata entry exists for the given StreamSegment Id.
     */
    SegmentMetadataUpdateTransaction getStreamSegmentMetadata(long streamSegmentId) throws MetadataUpdateException {
        SegmentMetadataUpdateTransaction tsm = this.streamSegmentUpdates.getOrDefault(streamSegmentId, null);
        if (tsm == null) {
            SegmentMetadata streamSegmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            if (streamSegmentMetadata == null) {
                streamSegmentMetadata = this.newStreamSegments.getOrDefault(streamSegmentId, null);

                if (streamSegmentMetadata == null) {
                    throw new MetadataUpdateException(this.containerMetadata.getContainerId(),
                            String.format("No metadata entry exists for StreamSegment Id %d.", streamSegmentId));
                }
            }

            tsm = new SegmentMetadataUpdateTransaction(streamSegmentMetadata, this.containerMetadata.isRecoveryMode());
            this.streamSegmentUpdates.put(streamSegmentId, tsm);
        }

        return tsm;
    }

    /**
     * Gets the Id of a segment associated with a Segment Name.
     *
     * @param streamSegmentName The name of the Segment to lookup the Id for.
     * @return The requested Id, or NO_STREAM_SEGMENT if not found.
     */
    long getExistingStreamSegmentId(String streamSegmentName) {
        long existingSegmentId = this.containerMetadata.getStreamSegmentId(streamSegmentName, false);
        if (existingSegmentId == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            existingSegmentId = this.newStreamSegmentNames.getOrDefault(streamSegmentName, ContainerMetadata.NO_STREAM_SEGMENT_ID);
        }

        return existingSegmentId;
    }

    private void checkExistingMapping(StreamSegmentMapping operation) throws MetadataUpdateException {
        long existingStreamSegmentId = getExistingStreamSegmentId(operation.getStreamSegmentName());
        if (existingStreamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID
                && existingStreamSegmentId != operation.getStreamSegmentId()) {
            throw new MetadataUpdateException(
                    this.containerMetadata.getContainerId(),
                    String.format("Operation '%s' wants to map a Segment that is already mapped in the metadata. Existing Id = %d.",
                            operation, existingStreamSegmentId));
        }
    }

    private void assignUniqueStreamSegmentId(StreamSegmentMapping mapping) throws TooManyActiveSegmentsException {
        if (!this.containerMetadata.isRecoveryMode()) {
            int maxCount = this.containerMetadata.getMaximumActiveSegmentCount();
            if (this.containerMetadata.getActiveSegmentCount() + this.newStreamSegments.size() >= maxCount) {
                throw new TooManyActiveSegmentsException(this.containerMetadata.getContainerId(), maxCount);
            }

            // Assign the SegmentId, but only in non-recovery mode and only if not already assigned.
            if (mapping.getStreamSegmentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                mapping.setStreamSegmentId(generateUniqueStreamSegmentId());
            }
        }
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

    /**
     * Gets an UpdateableSegmentMetadata for the given segment id, if it is already registered in the base metadata
     * or added as a new segment in this transaction.
     */
    private SegmentMetadata getExistingMetadata(long streamSegmentId) {
        SegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
        if (sm == null) {
            sm = this.newStreamSegments.getOrDefault(streamSegmentId, null);
        }

        return sm;
    }

    /**
     * Gets an UpdateableSegmentMetadata for the given Segment. If already registered, it returns that instance,
     * otherwise it creates and records a new Segment metadata.
     */
    private UpdateableSegmentMetadata getOrCreateSegmentMetadata(String streamSegmentName, long streamSegmentId, long parentId) {
        UpdateableSegmentMetadata sm = this.streamSegmentUpdates.get(streamSegmentId);
        if (sm == null) {
            sm = this.newStreamSegments.getOrDefault(streamSegmentId, null);
        }

        if (sm == null) {
            sm = createSegmentMetadata(streamSegmentName, streamSegmentId, parentId);
        }

        return sm;
    }

    /**
     * Creates a new UpdateableSegmentMetadata for the given Segment and registers it.
     */
    private UpdateableSegmentMetadata createSegmentMetadata(String streamSegmentName, long streamSegmentId, long parentId) {
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

        UpdateableSegmentMetadata metadata = getOrCreateSegmentMetadata(name, segmentId, parentId);

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
        SegmentMetadataUpdateTransaction metadata = getStreamSegmentMetadata(segmentId);
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
