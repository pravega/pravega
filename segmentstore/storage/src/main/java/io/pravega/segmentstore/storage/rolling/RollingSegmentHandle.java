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
package io.pravega.segmentstore.storage.rolling;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.shared.NameUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

@ThreadSafe
class RollingSegmentHandle implements SegmentHandle {
    //region Members

    /**
     * The name of the Segment for this Handle.
     */
    @Getter
    private final String segmentName;
    @Getter
    private final boolean readOnly;
    /**
     * A pointer to the Handle for this Segment's Header.
     */
    @GuardedBy("this")
    private SegmentHandle headerHandle;
    /**
     * The Rolling Policy for this Segment.
     */
    @Getter
    private final SegmentRollingPolicy rollingPolicy;
    @GuardedBy("this")
    private int headerLength;
    @GuardedBy("this")
    private List<SegmentChunk> segmentChunks;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean deleted;
    @GuardedBy("this")
    private SegmentHandle activeChunkHandle;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RollingSegmentHandle class.
     *
     * @param headerHandle  A SegmentHandle for the Header SegmentChunk.
     * @param rollingPolicy The Rolling Policy to apply for this Segment.
     * @param segmentChunks   A ordered list of initial SegmentChunks for this handle.
     */
    RollingSegmentHandle(SegmentHandle headerHandle, SegmentRollingPolicy rollingPolicy, List<SegmentChunk> segmentChunks) {
        this.headerHandle = Preconditions.checkNotNull(headerHandle, "headerHandle");
        this.readOnly = this.headerHandle.isReadOnly();
        this.segmentName = NameUtils.getSegmentNameFromHeader(headerHandle.getSegmentName());
        Exceptions.checkNotNullOrEmpty(this.segmentName, "headerHandle.getSegmentName()");
        this.rollingPolicy = rollingPolicy == null ? SegmentRollingPolicy.NO_ROLLING : rollingPolicy;
        this.segmentChunks = Preconditions.checkNotNull(segmentChunks, "segmentChunks");
    }

    RollingSegmentHandle(SegmentHandle segmentHandle) {
        this.headerHandle = null;
        this.readOnly = segmentHandle.isReadOnly();
        this.segmentName = Exceptions.checkNotNullOrEmpty(segmentHandle.getSegmentName(), "headerHandle.getSegmentName()");
        this.rollingPolicy = SegmentRollingPolicy.NO_ROLLING;
        this.segmentChunks = Collections.singletonList(new SegmentChunk(segmentHandle.getSegmentName(), 0));
    }

    //endregion

    /**
     * Updates the contents of this handle with information from the given one.
     *
     * @param source The RollingSegmentHandle to update from.
     */
    synchronized void refresh(RollingSegmentHandle source) {
        Preconditions.checkArgument(source.getSegmentName().equals(this.getSegmentName()), "SegmentName mismatch.");
        if (this.readOnly == source.readOnly) {
            // Update the header handle, but only if both this handle and the source one have the same read-only flag.
            // Otherwise we risk attaching a read-only header handle to a read-write handle or vice-versa.
            this.headerHandle = source.headerHandle;
        }

        this.segmentChunks = new ArrayList<>(source.chunks());
        setHeaderLength(source.getHeaderLength());
        if (source.isSealed()) {
            markSealed();
        }
        if (source.isDeleted()) {
            markDeleted();
        }
    }

    //region Properties

    /**
     * Gets a pointer to the Header Handle for this RollingSegmentHandle, if it has any Header.
     */
    synchronized SegmentHandle getHeaderHandle() {
        return this.headerHandle;
    }

    /**
     * Records the fact that the Segment represented by this Handle has been sealed.
     */
    synchronized void markSealed() {
        if (!this.sealed) {
            this.sealed = true;
            this.segmentChunks = Collections.unmodifiableList(this.segmentChunks);
            this.activeChunkHandle = null;
        }
    }

    /**
     * Gets a value indicating whether the Segment represented by this Handle is sealed.
     */
    synchronized boolean isSealed() {
        return this.sealed;
    }

    /**
     * Records the fact that the Segment represented by this Handle has been deleted.
     */
    synchronized void markDeleted() {
        this.deleted = true;
    }

    /**
     * Gets a value indicating whether the Segment represented by this Handle is deleted.
     */
    synchronized boolean isDeleted() {
        return this.deleted;
    }

    /**
     * Gets a pointer to the last SegmentChunk.
     *
     * @return The last SegmentChunk, or null if no SegmentChunks exist.
     */
    synchronized SegmentChunk lastChunk() {
        return this.segmentChunks.size() == 0 ? null : this.segmentChunks.get(this.segmentChunks.size() - 1);
    }

    /**
     * Gets an unmodifiable List of all current SegmentChunks for this Handle. If the Segment is not sealed, a copy of the
     * current SegmentChunks is returned (since they may change in the future).
     *
     * @return A List with SegmentChunks.
     */
    synchronized List<SegmentChunk> chunks() {
        if (this.sealed) {
            return this.segmentChunks; // This is already an unmodifiable list.
        } else {
            return Collections.unmodifiableList(this.segmentChunks.subList(0, this.segmentChunks.size()));
        }
    }

    /**
     * Adds a new SegmentChunk.
     *
     * @param segmentChunk      The SegmentChunk to add. This SegmentChunk must be in continuity of any existing SegmentChunks.
     * @param activeChunkHandle The newly added SegmentChunk's write handle.
     */
    synchronized void addChunk(SegmentChunk segmentChunk, SegmentHandle activeChunkHandle) {
        Preconditions.checkState(!this.sealed, "Cannot add SegmentChunks for a Sealed Handle.");
        if (this.segmentChunks.size() > 0) {
            long expectedOffset = this.segmentChunks.get(this.segmentChunks.size() - 1).getLastOffset();
            Preconditions.checkArgument(segmentChunk.getStartOffset() == expectedOffset,
                    "Invalid SegmentChunk StartOffset. Expected %s, given %s.", expectedOffset, segmentChunk.getStartOffset());
        }

        // Update the SegmentChunk and its Handle atomically.
        Preconditions.checkNotNull(activeChunkHandle, "activeChunkHandle");
        Preconditions.checkArgument(!activeChunkHandle.isReadOnly(), "Active SegmentChunk handle cannot be readonly.");
        Preconditions.checkArgument(activeChunkHandle.getSegmentName().equals(segmentChunk.getName()),
                "Active SegmentChunk handle must be for the last SegmentChunk.");
        this.activeChunkHandle = activeChunkHandle;
        this.segmentChunks.add(segmentChunk);
    }

    /**
     * Adds multiple SegmentChunks.
     *
     * @param segmentChunks The SegmentChunks to add. These SegmentChunks must be in continuity of any existing SegmentChunks.
     */
    synchronized void addChunks(List<SegmentChunk> segmentChunks) {
        Preconditions.checkState(!this.sealed, "Cannot add SegmentChunks for a Sealed Handle.");
        long expectedOffset = 0;
        if (this.segmentChunks.size() > 0) {
            expectedOffset = this.segmentChunks.get(this.segmentChunks.size() - 1).getLastOffset();
        } else if (segmentChunks.size() > 0) {
            expectedOffset = segmentChunks.get(0).getStartOffset();
        }

        for (SegmentChunk s : segmentChunks) {
            Preconditions.checkArgument(s.getStartOffset() == expectedOffset,
                    "Invalid SegmentChunk StartOffset. Expected %s, given %s.", expectedOffset, s.getStartOffset());
            expectedOffset += s.getLength();
        }

        this.segmentChunks.addAll(segmentChunks);
        this.activeChunkHandle = null;
    }

    /**
     * Removes references to any {@link SegmentChunk} instances that have {@link SegmentChunk#exists()} set to false.
     */
    synchronized void excludeInexistentChunks() {
        List<SegmentChunk> newChunks = new ArrayList<>();
        boolean exists = false;
        for (SegmentChunk sc : this.segmentChunks) {
            exists |= sc.exists();
            if (exists) {
                newChunks.add(sc);
            }
        }
        this.segmentChunks = this.sealed ? Collections.unmodifiableList(newChunks) : newChunks;
    }

    /**
     * Gets a value indicating the current length of the Segment, in bytes.
     *
     * @return The length.
     */
    synchronized long length() {
        SegmentChunk lastSegmentChunk = lastChunk();
        return lastSegmentChunk == null ? 0L : lastSegmentChunk.getLastOffset();
    }

    /**
     * Gets a pointer to the Active SegmentChunk Handle.
     *
     * @return The handle.
     */
    synchronized SegmentHandle getActiveChunkHandle() {
        return this.activeChunkHandle;
    }

    /**
     * Sets the Active SegmentChunk handle.
     *
     * @param handle The handle. Must not be read-only and for the last SegmentChunk.
     */
    synchronized void setActiveChunkHandle(SegmentHandle handle) {
        Preconditions.checkArgument(handle == null || !handle.isReadOnly(), "Active SegmentChunk handle cannot be readonly.");
        SegmentChunk last = lastChunk();
        Preconditions.checkState(last != null, "Cannot set an Active SegmentChunk handle when there are no SegmentChunks.");
        Preconditions.checkArgument(handle == null || handle.getSegmentName().equals(last.getName()),
                "Active SegmentChunk handle must be for the last SegmentChunk.");
        this.activeChunkHandle = handle;
    }

    /**
     * Gets a value indicating the serialized length of the Header.
     */
    synchronized int getHeaderLength() {
        return this.headerLength;
    }

    /**
     * Sets the serialized length of the Header.
     */
    synchronized void setHeaderLength(int value) {
        this.headerLength = value;
    }

    /**
     * Increases the serialized length of the Header by the given value.
     */
    synchronized void increaseHeaderLength(int value) {
        this.headerLength += value;
    }

    @Override
    public synchronized String toString() {
        if (this.deleted) {
            return String.format("%s (Deleted)", this.segmentName);
        } else {
            return String.format("%s (%s, %s, SegmentChunks=%d)", this.segmentName, this.sealed ? "Sealed" : "Not Sealed",
                    isReadOnly() ? "R" : "RW", this.segmentChunks.size());
        }
    }

    //endregion
}
