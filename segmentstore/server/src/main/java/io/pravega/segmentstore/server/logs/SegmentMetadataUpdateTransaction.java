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
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.SegmentOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An update transaction that can apply changes to a SegmentMetadata.
 */
@NotThreadSafe
class SegmentMetadataUpdateTransaction implements SegmentMetadata {
    //region Members

    private final boolean isRecoveryMode;
    private final Map<UUID, Long> updatedAttributeValues;
    private SegmentMetadata baseMetadata;
    private long currentDurableLogLength;
    private long currentStorageLength;
    private boolean sealed;
    private boolean sealedInStorage;
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
    SegmentMetadataUpdateTransaction(SegmentMetadata baseMetadata, boolean isRecoveryMode) {
        this.baseMetadata = Preconditions.checkNotNull(baseMetadata, "baseMetadata");
        this.isRecoveryMode = isRecoveryMode;
        this.currentDurableLogLength = this.baseMetadata.getDurableLogLength();
        this.currentStorageLength = -1;
        this.sealed = this.baseMetadata.isSealed();
        this.sealedInStorage = this.baseMetadata.isSealedInStorage();
        this.merged = this.baseMetadata.isMerged();
        this.deleted = this.baseMetadata.isDeleted();
        this.updatedAttributeValues = new HashMap<>();
        this.lastUsed = -1;
    }

    //endregion

    //region SegmentProperties Implementation

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
    public ImmutableDate getLastModified() {
        return new ImmutableDate();
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
        return this.sealedInStorage;
    }

    @Override
    public long getStorageLength() {
        return this.currentStorageLength < 0 ? this.baseMetadata.getStorageLength() : this.currentStorageLength;
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
        HashMap<UUID, Long> result = new HashMap<>(this.baseMetadata.getAttributes());
        result.putAll(this.updatedAttributeValues);
        return result; // TODO: this used to return updatedAttributeValues only...
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
    void preProcessAsParentSegment(MergeTransactionOperation operation, SegmentMetadataUpdateTransaction transactionMetadata)
            throws StreamSegmentSealedException, MetadataUpdateException {
        ensureSegmentId(operation);

        if (this.sealed) {
            // We do not allow merging into sealed Segments.
            throw new StreamSegmentSealedException(this.baseMetadata.getName());
        }

        if (this.baseMetadata.isTransaction()) {
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
        Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(),
                "operation", "Invalid Operation Transaction StreamSegment Id.");

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
            boolean hasValue = previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE;

            // Perform validation, and set the AttributeUpdate.value to the updated value, if necessary.
            switch (updateType) {
                case ReplaceIfGreater:
                    // Verify value against existing value, if any.
                    if (hasValue && u.getValue() <= previousValue) {
                        throw new BadAttributeUpdateException(this.baseMetadata.getName(), u,
                                String.format("Expected greater than '%s'.", previousValue));
                    }

                    break;
                case ReplaceIfEquals:
                    // Verify value against existing value, if any.
                    if (!hasValue || u.getComparisonValue() != previousValue) {
                        throw new BadAttributeUpdateException(this.baseMetadata.getName(), u,
                                String.format("Expected existing value to be '%s', actual '%s'.",
                                        u.getComparisonValue(), hasValue ? previousValue : "(not set)"));
                    }

                    break;
                case None:
                    // Verify value is not already set.
                    if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                        throw new BadAttributeUpdateException(this.baseMetadata.getName(), u,
                                String.format("Attribute value already set (%s).", previousValue));
                    }

                    break;
                case Accumulate:
                    if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                        u.setValue(previousValue + u.getValue());
                    }

                    break;
                case Replace:
                    break;
                default:
                    throw new BadAttributeUpdateException(this.baseMetadata.getName(), u, "Unexpected update type: " + updateType);
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
                    "StreamSegmentSealOperation cannot be accepted if it hasn't been pre-processed: " + operation);
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
    void acceptAsParentSegment(MergeTransactionOperation operation, SegmentMetadataUpdateTransaction transactionMetadata) throws MetadataUpdateException {
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
        Exceptions.checkArgument(this.baseMetadata.getId() == operation.getTransactionSegmentId(),
                "operation", "Invalid Operation Transaction StreamSegment Id.");

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
        this.currentStorageLength = storageLength;
        this.sealedInStorage = storageSealed;
        this.deleted = deleted;
        this.isChanged = true;
    }

    /**
     * Changes the base of this UpdateTransaction to a new SegmentMetadata.
     *
     * @param newBaseMetadata The new SegmentMetadata base.
     */
    void rebase(SegmentMetadata newBaseMetadata) {
        Preconditions.checkArgument(this.baseMetadata.getContainerId() == newBaseMetadata.getContainerId(), "ContainerId mismatch.");
        Preconditions.checkArgument(this.baseMetadata.getId() == newBaseMetadata.getId(), "SegmentId mismatch.");
        Preconditions.checkArgument(this.baseMetadata.getName().equals(newBaseMetadata.getName()), "SegmentName mismatch.");
        Preconditions.checkArgument(this.baseMetadata.getDurableLogLength() == newBaseMetadata.getDurableLogLength(), "DurableLogLength mismatch.");
        Preconditions.checkArgument(this.baseMetadata.isSealed() == newBaseMetadata.isSealed(), "IsSealed mismatch.");
        Preconditions.checkArgument(this.baseMetadata.isMerged() == newBaseMetadata.isMerged(), "IsMerged mismatch.");
        this.baseMetadata = newBaseMetadata;
    }

    /**
     * Applies all the outstanding changes to the base StreamSegmentMetadata object.
     */
    void apply(UpdateableSegmentMetadata target) {
        if (!this.isChanged) {
            // No changes made.
            return;
        }

        Preconditions.checkArgument(target.getId() == this.baseMetadata.getId(),
                "Target Segment Id mismatch. Expected %s, given %s.", this.baseMetadata.getId(), target.getId());
        Preconditions.checkArgument(target.getName().equals(this.baseMetadata.getName()),
                "Target Segment Name mismatch. Expected %s, given %s.", this.baseMetadata.getName(), target.getName());

        // Apply to base metadata.
        target.setLastUsed(this.lastUsed);
        target.updateAttributes(this.updatedAttributeValues);
        target.setDurableLogLength(this.currentDurableLogLength);
        if (this.currentStorageLength >= 0) {
            // Only update this if it really was set. Otherwise we might revert back to an old value if the Writer
            // has already made progress on it.
            target.setStorageLength(this.currentStorageLength);
        }

        if (this.sealed) {
            target.markSealed();
            if (this.sealedInStorage) {
                target.markSealedInStorage();
            }
        }

        if (this.merged) {
            target.markMerged();
        }
    }

    private void ensureSegmentId(SegmentOperation operation) {
        Exceptions.checkArgument(this.baseMetadata.getId() == operation.getStreamSegmentId(),
                "operation", "Invalid Log Operation StreamSegment Id.");
    }

    //endregion
}
