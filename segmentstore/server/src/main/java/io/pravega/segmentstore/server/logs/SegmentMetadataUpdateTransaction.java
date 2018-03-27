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
import io.pravega.segmentstore.contracts.StreamSegmentNotSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.TooManyAttributesException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.SegmentOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * An update transaction that can apply changes to a SegmentMetadata.
 */
@NotThreadSafe
class SegmentMetadataUpdateTransaction implements UpdateableSegmentMetadata {
    //region Members

    private final boolean recoveryMode;
    private final Map<UUID, Long> attributeValues;
    @Getter
    private final long id;
    @Getter
    private final long parentId;
    @Getter
    private final String name;
    @Getter
    private final int containerId;
    @Getter
    private long startOffset;
    @Getter
    private long length;
    private final long baseStorageLength;
    private long storageLength;
    @Getter
    private boolean sealed;
    @Getter
    private boolean sealedInStorage;
    @Getter
    private boolean merged;
    @Getter
    private boolean deleted;
    @Getter
    private long lastUsed;
    private boolean isChanged;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentMetadataUpdateTransaction class.
     *
     * @param baseMetadata   The base SegmentMetadata.
     * @param recoveryMode Whether the metadata is currently in recovery mode.
     */
    SegmentMetadataUpdateTransaction(SegmentMetadata baseMetadata, boolean recoveryMode) {
        this.recoveryMode = recoveryMode;
        this.id = baseMetadata.getId();
        this.parentId = baseMetadata.getParentId();
        this.name = baseMetadata.getName();
        this.containerId = baseMetadata.getContainerId();
        this.startOffset = baseMetadata.getStartOffset();
        this.length = baseMetadata.getLength();
        this.baseStorageLength = baseMetadata.getStorageLength();
        this.storageLength = -1;
        this.sealed = baseMetadata.isSealed();
        this.sealedInStorage = baseMetadata.isSealedInStorage();
        this.merged = baseMetadata.isMerged();
        this.deleted = baseMetadata.isDeleted();
        this.attributeValues = new HashMap<>(baseMetadata.getAttributes());
        this.lastUsed = baseMetadata.getLastUsed();
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public ImmutableDate getLastModified() {
        return new ImmutableDate();
    }

    //endregion

    //region SegmentMetadata Implementation


    @Override
    public long getStorageLength() {
        return this.storageLength < 0 ? this.baseStorageLength : this.storageLength;
    }

    @Override
    public boolean isActive() {
        throw new UnsupportedOperationException("isActive() is not supported on " + getClass().getName());
    }

    @Override
    public Map<UUID, Long> getAttributes() {
        return Collections.unmodifiableMap(this.attributeValues);
    }

    //endregion

    //region UpdateableSegmentMetadata implementation

    @Override
    public void setStorageLength(long value) {
        this.storageLength = value;
        this.isChanged = true;
    }

    @Override
    public void setStartOffset(long value) {
        this.startOffset = value;
        this.isChanged = true;
    }

    @Override
    public void setLength(long value) {
        this.length = value;
        this.isChanged = true;
    }

    @Override
    public void markSealed() {
        this.sealed = true;
        this.isChanged = true;
    }

    @Override
    public void markSealedInStorage() {
        this.sealedInStorage = true;
        this.sealed = true;
        this.isChanged = true;
    }

    @Override
    public void markDeleted() {
        this.deleted = true;
        this.isChanged = true;
    }

    @Override
    public void markMerged() {
        this.merged = true;
        this.isChanged = true;
    }

    @Override
    public void updateAttributes(Map<UUID, Long> attributeValues) {
        this.attributeValues.clear();
        this.attributeValues.putAll(attributeValues);
        this.isChanged = true;
    }

    @Override
    public void setLastModified(ImmutableDate date) {
        // Nothing to do.
    }

    @Override
    public void setLastUsed(long value) {
        this.lastUsed = value;
        this.isChanged = true;
    }

    @Override
    public void copyFrom(SegmentMetadata other) {
        throw new UnsupportedOperationException("copyFrom is not supported on " + this.getClass().getName());
    }

    //endregion

    //region Pre-Processing

    /**
     * Pre-processes a StreamSegmentAppendOperation.
     * After this method returns, the given operation will have its SegmentOffset property set to the current
     * SegmentLength, and all AttributeUpdates will be set to the current values.
     *
     * @param operation The operation to pre-process.
     * @throws StreamSegmentSealedException If the Segment is sealed.
     * @throws StreamSegmentMergedException If the Segment is merged into another.
     * @throws BadOffsetException           If the operation has an assigned offset, but it doesn't match the current
     *                                      Segment DurableLogOffset.
     * @throws IllegalArgumentException     If the operation is for a different Segment.
     * @throws BadAttributeUpdateException  If at least one of the AttributeUpdates is invalid given the current attribute
     *                                      values of the segment.
     * @throws TooManyAttributesException  If, as a result of applying the given updates, the Segment would exceed the
     *                                     maximum allowed number of Attributes.
     */
    void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
            BadOffsetException, BadAttributeUpdateException, TooManyAttributesException {
        ensureSegmentId(operation);
        if (this.merged) {
            // We do not allow any operation after merging (since after merging the Segment disappears).
            throw new StreamSegmentMergedException(this.name);
        }

        if (this.sealed) {
            throw new StreamSegmentSealedException(this.name);
        }

        if (!this.recoveryMode) {
            // Offset check (if append-with-offset).
            long operationOffset = operation.getStreamSegmentOffset();
            if (operationOffset >= 0) {
                // If the Operation already has an offset assigned, verify that it matches the current end offset of the Segment.
                if (operationOffset != this.length) {
                    throw new BadOffsetException(this.name, this.length, operationOffset);
                }
            } else {
                // No pre-assigned offset. Put the Append at the end of the Segment.
                operation.setStreamSegmentOffset(this.length);
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
     * @throws StreamSegmentSealedException If the Segment is sealed.
     * @throws StreamSegmentMergedException If the Segment is merged into another.
     * @throws IllegalArgumentException     If the operation is for a different Segment.
     * @throws BadAttributeUpdateException  If at least one of the AttributeUpdates is invalid given the current attribute
     *                                      values of the segment.
     * @throws TooManyAttributesException  If, as a result of applying the given updates, the Segment would exceed the
     *                                     maximum allowed number of Attributes.
     */
    void preProcessOperation(UpdateAttributesOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
            BadAttributeUpdateException, TooManyAttributesException {
        ensureSegmentId(operation);
        if (this.merged) {
            // We do not allow any operation after merging (since after merging the Segment disappears).
            throw new StreamSegmentMergedException(this.name);
        }

        if (this.sealed) {
            throw new StreamSegmentSealedException(this.name);
        }

        if (!this.recoveryMode) {
            preProcessAttributes(operation.getAttributeUpdates());
        }
    }

    /**
     * Pre-processes a StreamSegmentSealOperation.
     * After this method returns, the operation will have its SegmentLength property set to the current length of the Segment.
     *
     * @param operation The Operation.
     * @throws StreamSegmentSealedException If the Segment is already sealed.
     * @throws StreamSegmentMergedException If the Segment is merged into another.
     * @throws IllegalArgumentException     If the operation is for a different Segment.
     */
    void preProcessOperation(StreamSegmentSealOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException {
        ensureSegmentId(operation);
        if (this.merged) {
            // We do not allow any operation after merging (since after merging the Stream disappears).
            throw new StreamSegmentMergedException(this.name);
        }

        if (this.sealed) {
            // We do not allow re-sealing an already sealed stream.
            throw new StreamSegmentSealedException(this.name);
        }

        if (!this.recoveryMode) {
            // Assign entry StreamSegment Length.
            operation.setStreamSegmentOffset(this.length);
        }
    }

    /**
     * Pre-processes a StreamSegmentTruncateOperation.
     *
     * @param operation The Operation.
     * @throws BadOffsetException              If the operation's Offset is not between the current StartOffset and current
     *                                         EndOffset (SegmentLength - 1).
     * @throws MetadataUpdateException         If the operation cannot be processed because of the current state of the
     *                                         StreamSegment or Container (ex: Segment is a Transaction).
     */
    void preProcessOperation(StreamSegmentTruncateOperation operation) throws BadOffsetException, MetadataUpdateException {
        ensureSegmentId(operation);
        if (isTransaction()) {
            throw new MetadataUpdateException(this.containerId, "Cannot truncate a Transaction Segment: " + operation);
        }

        if (operation.getStreamSegmentOffset() < this.startOffset || operation.getStreamSegmentOffset() > this.length) {
            String msg = String.format("Truncation Offset must be at least %d and at most %d, given %d.",
                                       this.startOffset, this.length, operation.getStreamSegmentOffset());
            throw new BadOffsetException(this.name, this.startOffset, operation.getStreamSegmentOffset(), msg);
        }
    }

    /**
     * Pre-processes the given MergeTransactionOperation as a Parent Segment.
     * After this method returns, the operation will have its TargetSegmentOffset set to the length of the Parent Segment.
     *
     * @param operation           The operation to pre-process.
     * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
     * @throws StreamSegmentSealedException    If the parent stream is already sealed.
     * @throws StreamSegmentNotSealedException If the transaction segment is not sealed.
     * @throws MetadataUpdateException If the operation cannot be processed because of the current state of the metadata.
     * @throws IllegalArgumentException        If the operation is for a different Segment.
     */
    void preProcessAsParentSegment(MergeTransactionOperation operation, SegmentMetadataUpdateTransaction transactionMetadata)
            throws StreamSegmentSealedException, StreamSegmentNotSealedException, MetadataUpdateException {
        ensureSegmentId(operation);

        if (this.sealed) {
            // We do not allow merging into sealed Segments.
            throw new StreamSegmentSealedException(this.name);
        }

        if (isTransaction()) {
            throw new MetadataUpdateException(this.containerId,
                    "Cannot merge a StreamSegment into a Transaction Segment: " + operation.toString());
        }

        // Check that the Transaction has been properly sealed and has its length set.
        if (!transactionMetadata.isSealed()) {
            throw new StreamSegmentNotSealedException(this.name);
        }

        long transLength = operation.getLength();
        if (transLength < 0) {
            throw new MetadataUpdateException(this.containerId,
                    "MergeTransactionOperation does not have its Transaction Segment Length set: " + operation.toString());
        }

        if (!this.recoveryMode) {
            // Assign entry Segment offset and update Segment offset afterwards.
            operation.setStreamSegmentOffset(this.length);
        }
    }

    /**
     * Pre-processes the given operation as a Transaction Segment.
     *
     * @param operation The operation
     * @throws IllegalArgumentException        If the operation is for a different stream segment.
     * @throws StreamSegmentNotSealedException If the Segment is not sealed.
     * @throws StreamSegmentMergedException    If the Segment is already merged.
     */
    void preProcessAsTransactionSegment(MergeTransactionOperation operation) throws StreamSegmentNotSealedException, StreamSegmentMergedException {
        Exceptions.checkArgument(this.id == operation.getTransactionSegmentId(),
                "operation", "Invalid Operation Transaction Segment Id.");

        if (this.merged) {
            throw new StreamSegmentMergedException(this.name);
        }

        if (!this.sealed) {
            throw new StreamSegmentNotSealedException(this.name);
        }

        if (!this.recoveryMode) {
            operation.setLength(this.length);
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
     * @throws TooManyAttributesException  If, as a result of applying the given updates, the Segment would exceed the
     *                                     maximum allowed number of Attributes.
     */
    private void preProcessAttributes(Collection<AttributeUpdate> attributeUpdates) throws BadAttributeUpdateException, TooManyAttributesException {
        if (attributeUpdates == null) {
            return;
        }

        int newAttributeCount = this.attributeValues.size();
        for (AttributeUpdate u : attributeUpdates) {
            AttributeUpdateType updateType = u.getUpdateType();
            long previousValue = this.attributeValues.getOrDefault(u.getAttributeId(), SegmentMetadata.NULL_ATTRIBUTE_VALUE);

            // Perform validation, and set the AttributeUpdate.value to the updated value, if necessary.
            switch (updateType) {
                case ReplaceIfGreater:
                    // Verify value against existing value, if any.
                    boolean hasValue = previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE;
                    if (hasValue && u.getValue() <= previousValue) {
                        throw new BadAttributeUpdateException(this.name, u,
                                String.format("Expected greater than '%s'.", previousValue));
                    }

                    break;
                case ReplaceIfEquals:
                    // Verify value against existing value, if any.
                    if (u.getComparisonValue() != previousValue) {
                        throw new BadAttributeUpdateException(this.name, u,
                                String.format("Expected existing value to be '%s', actual '%s'.",
                                        u.getComparisonValue(), previousValue));
                    }

                    break;
                case None:
                    // Verify value is not already set.
                    if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                        throw new BadAttributeUpdateException(this.name, u,
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
                    throw new BadAttributeUpdateException(this.name, u, "Unexpected update type: " + updateType);
            }

            if (previousValue == SegmentMetadata.NULL_ATTRIBUTE_VALUE && u.getValue() != SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                // This attribute did not exist and is about to be added.
                newAttributeCount++;
            } else if (previousValue != SegmentMetadata.NULL_ATTRIBUTE_VALUE && u.getValue() == SegmentMetadata.NULL_ATTRIBUTE_VALUE) {
                // This attribute existed and is about to be removed.
                newAttributeCount--;
            }
        }

        if (newAttributeCount > SegmentMetadata.MAXIMUM_ATTRIBUTE_COUNT && newAttributeCount > this.attributeValues.size()) {
            // We only want to prevent exceeding the max attribute count if the number of attributes increased. Should
            // we ever want to decrease this limit in the future, we need to make sure that we can still remove/replace
            // attributes of existing segments, but not increase their count.
            throw new TooManyAttributesException(this.name, SegmentMetadata.MAXIMUM_ATTRIBUTE_COUNT);
        }
    }

    //endregion

    //region AcceptOperation

    /**
     * Accepts a StreamSegmentAppendOperation in the metadata.
     *
     * @param operation The operation to accept.
     * @throws MetadataUpdateException  If the operation SegmentOffset is different from the current Segment Length.
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptOperation(StreamSegmentAppendOperation operation) throws MetadataUpdateException {
        ensureSegmentId(operation);
        if (operation.getStreamSegmentOffset() != this.length) {
            throw new MetadataUpdateException(this.containerId,
                    String.format("SegmentAppendOperation offset mismatch. Expected %d, actual %d.",
                            this.length, operation.getStreamSegmentOffset()));
        }

        this.length += operation.getData().length;
        acceptAttributes(operation.getAttributeUpdates());
        this.isChanged = true;
    }

    /**
     * Accepts an UpdateAttributesOperation in the metadata.
     *
     * @param operation The operation to accept.
     * @throws IllegalArgumentException If the operation is for a different Segment.
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
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptOperation(StreamSegmentSealOperation operation) throws MetadataUpdateException {
        ensureSegmentId(operation);
        if (operation.getStreamSegmentOffset() < 0) {
            throw new MetadataUpdateException(containerId,
                    "StreamSegmentSealOperation cannot be accepted if it hasn't been pre-processed: " + operation);
        }

        this.sealed = true;

        // Clear all dynamic attributes.
        this.attributeValues.entrySet().forEach(e -> {
            if (Attributes.isDynamic(e.getKey())) {
                e.setValue(SegmentMetadata.NULL_ATTRIBUTE_VALUE);
            }
        });

        this.isChanged = true;
    }

    /**
     * Accepts a StreamSegmentTruncateOperation in the metadata.
     *
     * @param operation The operation to accept.
     */
    void acceptOperation(StreamSegmentTruncateOperation operation) {
        ensureSegmentId(operation);
        this.startOffset = operation.getStreamSegmentOffset();
        this.isChanged = true;
    }

    /**
     * Accepts the given MergeTransactionOperation as a Parent Segment.
     *
     * @param operation           The operation to accept.
     * @param transactionMetadata The metadata for the Transaction Stream Segment to merge.
     * @throws MetadataUpdateException  If the operation cannot be processed because of the current state of the metadata.
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptAsParentSegment(MergeTransactionOperation operation, SegmentMetadataUpdateTransaction transactionMetadata) throws MetadataUpdateException {
        ensureSegmentId(operation);

        if (operation.getStreamSegmentOffset() != this.length) {
            throw new MetadataUpdateException(containerId,
                    String.format("MergeTransactionOperation target offset mismatch. Expected %d, actual %d.",
                            this.length, operation.getStreamSegmentOffset()));
        }

        long transLength = operation.getLength();
        if (transLength < 0 || transLength != transactionMetadata.length) {
            throw new MetadataUpdateException(containerId,
                    "MergeTransactionOperation does not seem to have been pre-processed: " + operation.toString());
        }

        this.length += transLength;
        this.isChanged = true;
    }

    /**
     * Accepts the given operation as a Transaction Stream Segment.
     *
     * @param operation The operation
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptAsTransactionSegment(MergeTransactionOperation operation) {
        Exceptions.checkArgument(this.id == operation.getTransactionSegmentId(),
                "operation", "Invalid Operation Transaction Segment Id.");

        this.sealed = true;
        this.merged = true;
        this.isChanged = true;
    }

    /**
     * Accepts a collection of AttributeUpdates in the metadata.
     *
     * @param attributeUpdates The Attribute updates to accept.
     */
    private void acceptAttributes(Collection<AttributeUpdate> attributeUpdates) {
        if (attributeUpdates == null) {
            return;
        }

        for (AttributeUpdate au : attributeUpdates) {
            this.attributeValues.put(au.getAttributeId(), au.getValue());
        }
    }

    //endregion

    //region Operations

    /**
     * Updates the transaction with the given state of the segment in storage.
     *
     * This method is only meant to be used during recovery mode when we need to restore the state of a segment.
     * During normal operations, these values are set asynchronously by the Writer.
     *
     * @param storageLength The value to set as StorageLength.
     * @param storageSealed The value to set as SealedInStorage.
     * @param deleted       The value to set as Deleted.
     */
    void updateStorageState(long storageLength, boolean storageSealed, boolean deleted) {
        this.storageLength = storageLength;
        this.sealedInStorage = storageSealed;
        this.deleted = deleted;
        this.isChanged = true;
    }

    /**
     * Applies all the outstanding changes to the base SegmentMetadata object.
     */
    void apply(UpdateableSegmentMetadata target) {
        if (!this.isChanged) {
            // No changes made.
            return;
        }

        Preconditions.checkArgument(target.getId() == this.id,
                "Target Segment Id mismatch. Expected %s, given %s.", this.id, target.getId());
        Preconditions.checkArgument(target.getName().equals(this.name),
                "Target Segment Name mismatch. Expected %s, given %s.", name, target.getName());

        // Apply to base metadata.
        target.setLastUsed(this.lastUsed);
        target.updateAttributes(this.attributeValues);
        target.setLength(this.length);

        // Update StartOffset after (potentially) updating the length, since he Start Offset must be less than or equal to Length.
        target.setStartOffset(this.startOffset);
        if (this.storageLength >= 0) {
            // Only update this if it really was set. Otherwise we might revert back to an old value if the Writer
            // has already made progress on it.
            target.setStorageLength(this.storageLength);
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

        if (this.deleted) {
            target.markDeleted();
        }
    }

    private void ensureSegmentId(SegmentOperation operation) {
        Exceptions.checkArgument(this.id == operation.getStreamSegmentId(),
                "operation", "Invalid Log Operation Segment Id.");
    }

    //endregion
}
