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
     * Name of the segment.
     */
    private final String name;

    /**
     * Length of the segment.
     */
    private volatile long length;

    /**
     * Number of chunks.
     */
    private volatile int chunkCount;

    /**
     * Start offset of the segment. This is offset of the first byte available for read.
     */
    private volatile long startOffset;

    /**
     * Status bit flags.
     */
    private volatile int status;

    /**
     * Maximum Rolling length of the segment.
     */
    private volatile long maxRollinglength;

    /**
     * Name of the first chunk.
     */
    private volatile String firstChunk;

    /**
     * Name of the last chunk.
     */
    private volatile String lastChunk;

    /**
     * Last modified time.
     */
    private volatile long lastModified;

    /**
     * Offset of the first byte of the first chunk.
     * This is NOT the same as start offset of the segment. Byte at this offset may not be available for read.
     * With arbitrary truncates the effective start offset might be in the middle of the first chunk.
     *
     */
    private volatile long firstChunkStartOffset;

    /**
     * Offset of the first byte of the last chunk.
     */
    private volatile long lastChunkStartOffset;

    /**
     * Epoch of the container that last owned it.
     */
    private volatile long ownerEpoch;

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
        return setFlag(StatusFlags.ACTIVE, value);
    }

    /**
     * Sets sealed or unsealed status.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setSealed(boolean value) {
        return setFlag(StatusFlags.SEALED, value);
    }

    /**
     * Sets whether given segment is storage system internal segment.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setStorageSystemSegment(boolean value) {
        return setFlag(StatusFlags.SYSTEM_SEGMENT, value);
    }

    /**
     * Sets whether ownership was recently changed.
     * This value indicates that a new chunk must be created for new owner. The flag is cleared after first write by new owner.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setOwnershipChanged(boolean value) {
        return setFlag(StatusFlags.OWNERSHIP_CHANGED, value);
    }

    /**
     * Sets whether writes should be atomic.
     * @param value Value to set.
     * @return This instance so that these calls can be chained.
     */
    public SegmentMetadata setAtomicWrites(boolean value) {
        return setFlag(StatusFlags.ATOMIC_WRITES, value);
    }


    /**
     * Gets active status.
     * @return True if active, false otherwise.
     */
    public boolean isActive() {
        return getFlag(StatusFlags.ACTIVE);
    }

    /**
     * Gets sealed/unsealed status.
     * @return True if sealed, false otherwise.
     */
    public boolean isSealed() {
        return getFlag(StatusFlags.SEALED);
    }

    /**
     * Gets whether ownership was changed.
     * @return True if changed, false otherwise.
     */
    public boolean isOwnershipChanged() {
        return getFlag(StatusFlags.OWNERSHIP_CHANGED);
    }


    /**
     * Gets whether writes are atomic.
     * @return True if changed, false otherwise.
     */
    public boolean isAtomicWrite() {
        return getFlag(StatusFlags.ATOMIC_WRITES);
    }

    /**
     * Gets given segment is storage system internal segment.
     * @return True if segment is system segment, false otherwise.
     */
    public boolean isStorageSystemSegment() {
        return getFlag(StatusFlags.SYSTEM_SEGMENT);
    }

    /**
     * Checks the invariants that must be held for a segment.
     */
    public void checkInvariants() {
        Preconditions.checkState(length >= 0, "length should be non-negative. %s", this);
        Preconditions.checkState(startOffset >= 0, "startOffset should be non-negative. %s", this);
        Preconditions.checkState(firstChunkStartOffset >= 0, "firstChunkStartOffset should be non-negative. %s", this);
        Preconditions.checkState(lastChunkStartOffset >= 0, "lastChunkStartOffset should be non-negative. %s", this);
        Preconditions.checkState(firstChunkStartOffset <= startOffset, "startOffset must not be smaller than firstChunkStartOffset. %s", this);
        Preconditions.checkState(length >= lastChunkStartOffset, "lastChunkStartOffset must not be greater than length. %s", this);
        Preconditions.checkState(firstChunkStartOffset <= lastChunkStartOffset, "firstChunkStartOffset must not be greater than lastChunkStartOffset. %s", this);
        Preconditions.checkState(chunkCount >= 0, "chunkCount should be non-negative. %s", this);
        Preconditions.checkState(length >= startOffset, "length must be greater or equal to startOffset. %s", this);
        if (null == firstChunk) {
            Preconditions.checkState(null == lastChunk, "lastChunk must be null when firstChunk is null. %s", this);
            Preconditions.checkState(firstChunkStartOffset == startOffset, "firstChunkStartOffset must equal startOffset when firstChunk is null. %s", this);
            Preconditions.checkState(firstChunkStartOffset == lastChunkStartOffset, "firstChunkStartOffset must equal lastChunkStartOffset when firstChunk is null. %s", this);
            Preconditions.checkState(length == startOffset, "length must equal startOffset when firstChunk is null. %s", this);
            Preconditions.checkState(chunkCount == 0, "chunkCount should be 0. %s", this);

        } else if (firstChunk.equals(lastChunk)) {
            Preconditions.checkState(firstChunkStartOffset == lastChunkStartOffset, "firstChunkStartOffset must equal lastChunkStartOffset when there is only one chunk. %s", this);
            Preconditions.checkState(chunkCount == 1, "chunkCount should be 1. %s", this);
        } else {
            Preconditions.checkState(chunkCount >= 2, "chunkCount should be 2 or more. %s", this);
        }
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
