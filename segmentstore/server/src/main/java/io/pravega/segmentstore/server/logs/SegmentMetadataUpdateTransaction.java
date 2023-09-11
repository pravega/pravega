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
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.DynamicAttributeUpdate;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiPredicate;
import javax.annotation.concurrent.NotThreadSafe;

import lombok.Getter;
import lombok.Setter;

/**
 * An update transaction that can apply changes to a SegmentMetadata.
 */
@NotThreadSafe
class SegmentMetadataUpdateTransaction implements UpdateableSegmentMetadata {
    //region Members

    /**
     * The number of Extended Attributes allowed to be associated with a {@link SegmentType#TRANSIENT_SEGMENT}.
     */
    @VisibleForTesting
    public final static int TRANSIENT_ATTRIBUTE_LIMIT = 16;

    private final boolean recoveryMode;
    private final Map<AttributeId, Long> baseAttributeValues;
    private final Map<AttributeId, Long> attributeUpdates;
    @Getter
    private final long id;
    @Getter
    private final String name;
    @Getter
    private final int containerId;
    @Getter
    private final SegmentType type;
    @Getter
    private final int attributeIdLength;
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
    private boolean deletedInStorage;
    @Getter
    private boolean pinned;
    @Getter
    private boolean deleted;
    @Getter
    private long lastUsed;
    private boolean isChanged;
    @Getter
    @Setter
    private boolean active;

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
        this.name = baseMetadata.getName();
        this.containerId = baseMetadata.getContainerId();
        this.type = baseMetadata.getType();
        this.attributeIdLength = baseMetadata.getAttributeIdLength();
        this.startOffset = baseMetadata.getStartOffset();
        this.length = baseMetadata.getLength();
        this.baseStorageLength = baseMetadata.getStorageLength();
        this.storageLength = -1;
        this.sealed = baseMetadata.isSealed();
        this.sealedInStorage = baseMetadata.isSealedInStorage();
        this.merged = baseMetadata.isMerged();
        this.deletedInStorage = baseMetadata.isDeletedInStorage();
        this.deleted = baseMetadata.isDeleted();
        this.baseAttributeValues = baseMetadata.getAttributes();
        this.attributeUpdates = new HashMap<>();
        this.lastUsed = baseMetadata.getLastUsed();
        this.active = true;
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
    public SegmentProperties getSnapshot() {
        throw new UnsupportedOperationException("getSnapshot() is not supported on " + getClass().getName());
    }

    @Override
    public Map<AttributeId, Long> getAttributes(BiPredicate<AttributeId, Long> filter) {
        throw new UnsupportedOperationException("getAttributes(BiPredicate) is not supported on " + getClass().getName());
    }

    @Override
    public Map<AttributeId, Long> getAttributes() {
        HashMap<AttributeId, Long> result = new HashMap<>(this.baseAttributeValues);
        result.putAll(this.attributeUpdates);
        return result;
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
    public void markDeletedInStorage() {
        this.deletedInStorage = true;
        this.deleted = true;
        this.isChanged = true;
    }

    @Override
    public void markMerged() {
        this.merged = true;
        this.isChanged = true;
    }

    @Override
    public void markPinned() {
        this.pinned = true;
        this.isChanged = true;
    }

    @Override
    public void updateAttributes(Map<AttributeId, Long> attributeValues) {
        this.attributeUpdates.clear();
        this.attributeUpdates.putAll(attributeValues);
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
    public void refreshDerivedProperties() {
        // Nothing to do here. This method only applies to StreamSegmentMetadata.
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
     * @throws MetadataUpdateException      If the operation cannot not be processed because a Metadata Update precondition
     *                                      check failed.
     */
    void preProcessOperation(StreamSegmentAppendOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
            BadOffsetException, BadAttributeUpdateException, MetadataUpdateException {
        ensureSegmentId(operation);
        if (this.merged) {
            // We do not allow any operation after merging (since after merging the Segment disappears).
            throw new StreamSegmentMergedException(this.name);
        }

        if (this.sealed) {
            throw new StreamSegmentSealedException(this.name);
        }

        if (!this.recoveryMode) {
            // Attribute validation must occur first. If an append is invalid due to both bad attributes and offsets,
            // the attribute validation takes precedence.
            preProcessAttributes(operation.getAttributeUpdates());

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
     * @throws MetadataUpdateException      If the operation cannot not be processed because a Metadata Update precondition
     *                                      check failed.
     */
    void preProcessOperation(UpdateAttributesOperation operation) throws StreamSegmentSealedException, StreamSegmentMergedException,
            BadAttributeUpdateException, MetadataUpdateException {
        ensureSegmentId(operation);
        if (this.merged) {
            // We do not allow any operation after merging (since after merging the Segment disappears).
            throw new StreamSegmentMergedException(this.name);
        }

        if (this.sealed && !(operation.isInternal() && operation.hasOnlyCoreAttributes())) {
            // No operation may be accepted after a Segment has been sealed, except if it was internally generated and the
            // only thing that operation does is update exclusively Core Attributes.
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
     */
    void preProcessOperation(StreamSegmentTruncateOperation operation) throws BadOffsetException {
        ensureSegmentId(operation);
        if (operation.getStreamSegmentOffset() < this.startOffset || operation.getStreamSegmentOffset() > this.length) {
            String msg = String.format("Truncation Offset must be at least %d and at most %d, given %d.",
                                       this.startOffset, this.length, operation.getStreamSegmentOffset());
            throw new BadOffsetException(this.name, this.startOffset, operation.getStreamSegmentOffset(), msg);
        }
    }

    /**
     * Pre-processes a DeleteSegmentOperation.
     *
     * @param operation The Operation.
     */
    void preProcessOperation(DeleteSegmentOperation operation) {
        ensureSegmentId(operation);
    }

    /**
     * Pre-processes the given MergeSegmentOperation as a Target Segment (where it will be merged into).
     * After this method returns, the operation will have its TargetSegmentOffset set to the length of the Target Segment.
     *
     * @param operation      The operation to pre-process.
     * @param sourceMetadata The metadata for the Stream Segment to merge.
     * @throws StreamSegmentSealedException    If the target Segment is already sealed.
     * @throws StreamSegmentNotSealedException If the source Segment is not sealed.
     * @throws MetadataUpdateException         If the operation cannot be processed because of the current state of the metadata.
     * @throws IllegalArgumentException        If the operation is for a different Segment.
     * @throws BadAttributeUpdateException     If any of the given AttributeUpdates is invalid given the current state of the segment.
     */
    void preProcessAsTargetSegment(MergeSegmentOperation operation, SegmentMetadataUpdateTransaction sourceMetadata)
            throws StreamSegmentSealedException, StreamSegmentNotSealedException, MetadataUpdateException, BadAttributeUpdateException {
        ensureSegmentId(operation);

        if (this.sealed) {
            // We do not allow merging into sealed Segments.
            throw new StreamSegmentSealedException(this.name);
        }

        // Check that the Source has been properly sealed and has its length set.
        if (!sourceMetadata.isSealed()) {
            throw new StreamSegmentNotSealedException(this.name);
        }

        long transLength = operation.getLength();
        if (transLength < 0) {
            throw new MetadataUpdateException(this.containerId,
                    "MergeSegmentOperation does not have its Source Segment Length set: " + operation);
        }

        if (!this.recoveryMode) {
            // Update attributes first on the target Segment, if any.
            preProcessAttributes(operation.getAttributeUpdates());
            // Assign entry Segment offset and update Segment offset afterwards.
            operation.setStreamSegmentOffset(this.length);
        }
    }

    /**
     * Pre-processes the given operation as a Source Segment.
     *
     * @param operation The operation.
     * @throws IllegalArgumentException        If the operation is for a different Segment.
     * @throws StreamSegmentNotSealedException If the Segment is not sealed.
     * @throws StreamSegmentMergedException    If the Segment is already merged.
     * @throws StreamSegmentTruncatedException If the Segment is truncated.
     */
    void preProcessAsSourceSegment(MergeSegmentOperation operation) throws StreamSegmentNotSealedException,
            StreamSegmentMergedException, StreamSegmentTruncatedException {
        Exceptions.checkArgument(this.id == operation.getSourceSegmentId(),
                "operation", "Invalid Operation Source Segment Id.");

        if (this.merged) {
            throw new StreamSegmentMergedException(this.name);
        }

        if (!this.sealed) {
            throw new StreamSegmentNotSealedException(this.name);
        }

        if (this.startOffset > 0) {
            throw new StreamSegmentTruncatedException(this.name, "Segment cannot be merged because it is truncated.", null);
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
     * @throws BadAttributeUpdateException        If any of the given AttributeUpdates is invalid given the current state
     *                                            of the segment.
     * @throws AttributeIdLengthMismatchException If the given AttributeUpdates contains extended Attributes with the
     *                                            wrong length (compared to that which is defined on the Segment).
     * @throws MetadataUpdateException            If the operation cannot not be processed because a Metadata Update
     *                                            precondition check failed.
     */
    private void preProcessAttributes(AttributeUpdateCollection attributeUpdates)
            throws BadAttributeUpdateException, MetadataUpdateException {
        if (attributeUpdates == null) {
            return;
        }

        // Make sure that the number of existing ExtendedAttributes + incoming ExtendedAttributes does not exceed the limit.
        if (type.isTransientSegment() && this.baseAttributeValues.size() > TRANSIENT_ATTRIBUTE_LIMIT) {
            throw new MetadataUpdateException(this.containerId,
                    String.format("A Transient Segment ('%s') may not exceed %s Extended Attributes.", this.name, TRANSIENT_ATTRIBUTE_LIMIT));
        }

        // We must ensure that we aren't trying to set/update attributes that are incompatible with this Segment.
        validateAttributeIdLengths(attributeUpdates);
        for (AttributeUpdate u : attributeUpdates) {
            if (Attributes.isUnmodifiable(u.getAttributeId())) {
                throw new MetadataUpdateException(this.containerId,
                        String.format("Attribute Id '%s' on Segment Id %s ('%s') may not be modified.", u.getAttributeId(), this.id, this.name));
            }

            AttributeUpdateType updateType = u.getUpdateType();
            boolean hasValue = false;
            long previousValue = Attributes.NULL_ATTRIBUTE_VALUE;
            if (this.attributeUpdates.containsKey(u.getAttributeId())) {
                hasValue = true;
                previousValue = this.attributeUpdates.get(u.getAttributeId());
            } else if (this.baseAttributeValues.containsKey(u.getAttributeId())) {
                hasValue = true;
                previousValue = this.baseAttributeValues.get(u.getAttributeId());
            }

            // Perform validation, and set the AttributeUpdate.value to the updated value, if necessary.
            switch (updateType) {
                case ReplaceIfGreater:
                    // Verify value against existing value, if any.
                    if (hasValue && u.getValue() <= previousValue) {
                        throw new BadAttributeUpdateException(this.name, u, false,
                                String.format("Expected greater than '%s'.", previousValue));
                    }

                    break;
                case ReplaceIfEquals:
                    // Verify value against existing value, if any.
                    if (u.getComparisonValue() != previousValue || !hasValue) {
                        throw new BadAttributeUpdateException(this.name, u, !hasValue,
                                String.format("Expected '%s', given '%s'.", previousValue, u.getComparisonValue()));
                    }

                    break;
                case None:
                    // Verify value is not already set.
                    if (hasValue) {
                        throw new BadAttributeUpdateException(this.name, u, false,
                                String.format("Attribute value already set (%s).", previousValue));
                    }

                    break;
                case Accumulate:
                    if (hasValue) {
                        u.setValue(previousValue + u.getValue());
                    }

                    break;
                case Replace:
                    break;
                default:
                    throw new BadAttributeUpdateException(this.name, u, !hasValue, "Unexpected update type: " + updateType);
            }
        }

        // Evaluate and set DynamicAttributeUpdates.
        for (DynamicAttributeUpdate u : attributeUpdates.getDynamicAttributeUpdates()) {
            u.setValue(u.getValueReference().evaluate(this));
        }
    }

    private void validateAttributeIdLengths(AttributeUpdateCollection c) throws AttributeIdLengthMismatchException {
        Integer actualLength = c.getExtendedAttributeIdLength();
        if (actualLength == null) {
            // No extended attributes to validate.
            return;
        }
        int expectedLength = getAttributeIdLength();
        if (expectedLength <= 0 && actualLength > 0) {
            // We expect AttributeId.UUIDs, but we have AttributeId.Variable.
            throw new AttributeIdLengthMismatchException(this.containerId, String.format(
                    "Segment %s must have extended attributes of type UUID; tried to update with variable attributes of length %s.",
                    this.id, actualLength));
        } else if (expectedLength > 0 && expectedLength != actualLength) {
            // We have length mismatch.
            throw new AttributeIdLengthMismatchException(this.containerId, String.format(
                    "Segment %s must have extended attributes of length %s; tried to update with attributes of length %s.",
                    this.id, expectedLength, actualLength));
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

        this.length += operation.getData().getLength();
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
     * Accepts a DeleteSegmentOperation in the metadata.
     *
     * @param operation The operation to accept.
     */
    void acceptOperation(DeleteSegmentOperation operation) {
        ensureSegmentId(operation);
        this.deleted = true;
        this.isChanged = true;
    }

    /**
     * Accepts the given MergeSegmentOperation as a Target Segment (where it will be merged into).
     *
     * @param operation      The operation to accept.
     * @param sourceMetadata The metadata for the Source Segment to merge.
     * @throws MetadataUpdateException  If the operation cannot be processed because of the current state of the metadata.
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptAsTargetSegment(MergeSegmentOperation operation, SegmentMetadataUpdateTransaction sourceMetadata) throws MetadataUpdateException {
        ensureSegmentId(operation);

        if (operation.getStreamSegmentOffset() != this.length) {
            throw new MetadataUpdateException(containerId,
                    String.format("MergeSegmentOperation target offset mismatch. Expected %d, actual %d.",
                            this.length, operation.getStreamSegmentOffset()));
        }

        long transLength = operation.getLength();
        if (transLength < 0 || transLength != sourceMetadata.length) {
            throw new MetadataUpdateException(containerId,
                    "MergeSegmentOperation does not seem to have been pre-processed: " + operation);
        }
        acceptAttributes(operation.getAttributeUpdates());
        this.length += transLength;
        this.isChanged = true;
    }

    /**
     * Accepts the given operation as a Source Segment.
     *
     * @param operation The operation.
     * @throws IllegalArgumentException If the operation is for a different Segment.
     */
    void acceptAsSourceSegment(MergeSegmentOperation operation) {
        Exceptions.checkArgument(this.id == operation.getSourceSegmentId(),
                "operation", "Invalid Operation Source Segment Id.");

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
            this.attributeUpdates.put(au.getAttributeId(), au.getValue());
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
     * @param storageLength  The value to set as StorageLength.
     * @param storageSealed  The value to set as SealedInStorage.
     * @param deleted        The value to set as Deleted.
     * @param storageDeleted The value to set as DeletedInStorage.
     */
    void updateStorageState(long storageLength, boolean storageSealed, boolean deleted, boolean storageDeleted) {
        this.storageLength = storageLength;
        this.sealedInStorage = storageSealed;
        this.deleted = deleted;
        this.deletedInStorage = storageDeleted;
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
        Preconditions.checkState(isActive(), "Cannot apply changes for an inactive segment. Segment Id = %s, Segment Name = '%s'.", this.id, this.name);

        // Apply to base metadata.
        target.setLastUsed(this.lastUsed);
        target.updateAttributes(this.attributeUpdates);
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
            if (this.deletedInStorage) {
                target.markDeletedInStorage();
            }
        }

        if (this.pinned) {
            target.markPinned();
        }
    }

    private void ensureSegmentId(SegmentOperation operation) {
        Exceptions.checkArgument(this.id == operation.getStreamSegmentId(),
                "operation", "Invalid Log Operation Segment Id.");
    }

    //endregion
}
