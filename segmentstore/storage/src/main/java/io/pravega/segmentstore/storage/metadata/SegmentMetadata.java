/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

import com.google.common.base.Preconditions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;

/**
 * Represents segment metadata.
 Following metadata is stored.
 * <ul>
 *     <li>Name of the segment.</li>
 *     <li>Length of the segment.</li>
 *     <li>Epoch of the container that last owned it.</li>
 *     <li>Start offset of the segment. This is offset of the first byte available for read.</li>
 *     <li>Status bit flags.</li>
 *     <li>Maximum Rolling length of the segment.</li>
 *     <li>Name of the first chunk.</li>
 *     <li>Name of the last chunk.</li>
 *     <li>Offset corresponding to the the first byte of the first chunk.This is NOT the same as start offset of the segment.
 *      With arbitrary truncates the effective start offset might be in the middle of the first chunk. Byte at this offset may not be available for read.</li>
 *     <li>Offset of the first byte of the last chunk.</li>
 * </ul>
 */
@Data
@Builder(toBuilder = true)
@EqualsAndHashCode(callSuper = true)
@ThreadSafe
public class SegmentMetadata extends StorageMetadata {
    /**
     * Flag to indicate whether the segment is active or not.
     */
    private static final int ACTIVE = 0x0001;

    /**
     * Flag to indicate whether the segment is sealed or not.
     */
    private static final int SEALED = 0x0002;

    /**
     * Flag to indicate whether the segment is deleted or not.
     */
    private static final int DELETED = 0x0004;

    /**
     * Flag to indicate whether followup actions (like adding new chunks) after ownership changes are needed or not.
     */
    private static final int OWNERSHIP_CHANGED = 0x0008;

    /**
     * Flag to indicate whether the segment is storage system segment.
     */
    private static final int SYSTEM_SEGMENT = 0x0010;

    /**
     * Name of the segment.
     */
    private final String name;

    /**
     * Length of the segment.
     */
    private long length;

    /**
     * Number of chunks.
     */
    private int chunkCount;

    /**
     * Start offset of the segment. This is offset of the first byte available for read.
     */
    private long startOffset;

    /**
     * Status bit flags.
     */
    private int status;

    /**
     * Maximum Rolling length of the segment.
     */
    private long maxRollinglength;

    /**
     * Name of the first chunk.
     */
    private String firstChunk;

    /**
     * Name of the last chunk.
     */
    private String lastChunk;

    /**
     * Last modified time.
     */
    private long lastModified;

    /**
     * Offset of the first byte of the first chunk.
     * This is NOT the same as start offset of the segment. Byte at this offset may not be available for read.
     * With arbitrary truncates the effective start offset might be in the middle of the first chunk.
     *
     */
    private long firstChunkStartOffset;

    /**
     * Offset of the first byte of the last chunk.
     */
    private long lastChunkStartOffset;

    /**
     * Epoch of the container that last owned it.
     */
    private long ownerEpoch;

    /**
     * Retrieves the key associated with the metadata, which is the name of the segment.
     *
     * @return Name of the segment.
     */
    @Override
    public String getKey() {
        return name;
    }

    /**
     * Creates a copy of this instance.
     *
     * @return Copy of this instance.
     */
    @Override
    public StorageMetadata deepCopy() {
        return toBuilder().build();
    }

    /**
     * Sets active status.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setActive(boolean value) {
        return setFlag(ACTIVE, value);
    }

    /**
     * Sets sealed or unsealed status.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setSealed(boolean value) {
        return setFlag(SEALED, value);
    }

    /**
     * Sets whether given segment is storage system internal segment.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setStorageSystemSegment(boolean value) {
        return setFlag(SYSTEM_SEGMENT, value);
    }

    /**
     * Sets whether ownership was recently changed.
     * This value indicates that a new chunk must be created for new owner. The flag is cleared after first write by new owner.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setOwnershipChanged(boolean value) {
        return setFlag(OWNERSHIP_CHANGED, value);
    }

    /**
     * Gets active status.
     * @return True if active, false otherwise.
     */
    public boolean isActive() {
        return getFlag(ACTIVE);
    }

    /**
     * Gets sealed/unsealed status.
     * @return True if sealed, false otherwise.
     */
    public boolean isSealed() {
        return getFlag(SEALED);
    }

    /**
     * Gets whether ownership was changed.
     * @return True if changed, false otherwise.
     */
    public boolean isOwnershipChanged() {
        return getFlag(OWNERSHIP_CHANGED);
    }

    /**
     * Gets given segment is storage system internal segment.
     * @return True if segment is system segment, false otherwise.
     */
    public boolean isStorageSystemSegment() {
        return getFlag(SYSTEM_SEGMENT);
    }

    /**
     * Checks the invariants that must be held for a segment.
     */
    public void checkInvariants() {
        // Please do not use any string formatting in this method.
        // All messages should be just plain strings. This avoids unnecessarily wasting time on formating the error messages when the expecation is that check never fails.
        Preconditions.checkState(length >= 0, "length should be non-negative.");
        Preconditions.checkState(startOffset >= 0, "startOffset should be non-negative.");
        Preconditions.checkState(firstChunkStartOffset >= 0, "firstChunkStartOffset should be non-negative.");
        Preconditions.checkState(lastChunkStartOffset >= 0, "lastChunkStartOffset should be non-negative.");
        Preconditions.checkState(firstChunkStartOffset <= startOffset, "startOffset should not be smaller than firstChunkStartOffset.");
        Preconditions.checkState(length >= lastChunkStartOffset, "lastChunkStartOffset should not be greater than length.");
        Preconditions.checkState(firstChunkStartOffset <= lastChunkStartOffset, "lastChunkStartOffset should not be greater than firstChunkStartOffset.");
        Preconditions.checkState(chunkCount >= 0, "chunkCount should be non-negative.");
        if (null == firstChunk) {
            Preconditions.checkState(null == lastChunk, "lastChunk must be null when firstChunk is null.");
            Preconditions.checkState(firstChunkStartOffset == startOffset, "firstChunkStartOffset must equal startOffset when firstChunk is null.");
            Preconditions.checkState(firstChunkStartOffset == lastChunkStartOffset, "firstChunkStartOffset must equal lastChunkStartOffset when firstChunk is null.");
            Preconditions.checkState(length == startOffset, "length must equal startOffset when firstChunk is null.");
            Preconditions.checkState(chunkCount == 0, "chunkCount should be 0.");

        } else if (firstChunk.equals(lastChunk)) {
            Preconditions.checkState(firstChunkStartOffset == lastChunkStartOffset, "firstChunkStartOffset must equal lastChunkStartOffset when there is only one chunk.");
            Preconditions.checkState(chunkCount == 1, "chunkCount should be 1.");
        }
    }

    /**
     * Increment chunk count.
     */
    public void incrementChunkCount() {
        chunkCount++;
        Preconditions.checkState(chunkCount >= 0, "chunkCount should be non-negative.");
    }

    /**
     * Decrement chunk count.
     */
    public void decrementChunkCount() {
        chunkCount--;
        Preconditions.checkState(chunkCount >= 0, "chunkCount should be non-negative.");
    }

    /**
     * Sets the given bit for given mask.
     */
    private SegmentMetadata setFlag(int mask, boolean value) {
        status = value ? (status | mask) : (status & (~mask));
        return this;
    }

    /**
     * Gets the status of the bit for given mask.
     */
    private boolean getFlag(int mask) {
        return (status & mask) != 0;
    }

    /**
     * Builder that implements {@link ObjectBuilder}.
     */
    public static class SegmentMetadataBuilder implements ObjectBuilder<SegmentMetadata> {
    }

    /**
     * Serializer that implements {@link VersionedSerializer}.
     */
    public static class Serializer extends VersionedSerializer.WithBuilder<SegmentMetadata, SegmentMetadataBuilder> {
        @Override
        protected SegmentMetadataBuilder newBuilder() {
            return SegmentMetadata.builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(SegmentMetadata object, RevisionDataOutput output) throws IOException {
            output.writeUTF(object.name);
            output.writeCompactLong(object.length);
            output.writeCompactInt(object.chunkCount);
            output.writeCompactLong(object.startOffset);
            output.writeCompactInt(object.status);
            output.writeCompactLong(object.maxRollinglength);
            output.writeUTF(nullToEmpty(object.firstChunk));
            output.writeUTF(nullToEmpty(object.lastChunk));
            output.writeCompactLong(object.lastModified);
            output.writeCompactLong(object.firstChunkStartOffset);
            output.writeCompactLong(object.lastChunkStartOffset);
            output.writeCompactLong(object.ownerEpoch);
        }

        private void read00(RevisionDataInput input, SegmentMetadataBuilder b) throws IOException {
            b.name(input.readUTF());
            b.length(input.readCompactLong());
            b.chunkCount(input.readCompactInt());
            b.startOffset(input.readCompactLong());
            b.status(input.readCompactInt());
            b.maxRollinglength(input.readCompactLong());
            b.firstChunk(emptyToNull(input.readUTF()));
            b.lastChunk(emptyToNull(input.readUTF()));
            b.lastModified(input.readCompactLong());
            b.firstChunkStartOffset(input.readCompactLong());
            b.lastChunkStartOffset(input.readCompactLong());
            b.ownerEpoch(input.readCompactLong());
        }
    }
}
