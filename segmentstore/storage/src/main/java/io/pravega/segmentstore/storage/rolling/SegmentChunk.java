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
import io.pravega.shared.NameUtils;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;

/**
 * Represents a range of bytes within a Segment.
 */
@ThreadSafe
class SegmentChunk {
    //region Private

    /**
     * The name of the SegmentChunk.
     */
    @Getter
    private final String name;
    /**
     * The offset within the owning Segment where this SegmentChunk starts.
     */
    @Getter
    private final long startOffset;
    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean exists = true;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentChunk class.
     *
     * @param chunkName   The name of this SegmentChunk (not of the owning Segment).
     * @param startOffset The offset within the owning Segment where this SegmentChunk starts at.
     */
    SegmentChunk(String chunkName, long startOffset) {
        this.name = Exceptions.checkNotNullOrEmpty(chunkName, "chunkName");
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be a non-negative number.");
        this.startOffset = startOffset;
    }

    /**
     * Creates a new instance of the SegmentChunk class.
     *
     * @param segmentName The name of the owning Segment (not the name of this SegmentChunk).
     * @param startOffset The offset within the owning Segment where this SegmentChunk starts at.
     * @return A new SegmentChunk.
     */
    static SegmentChunk forSegment(String segmentName, long startOffset) {
        return new SegmentChunk(NameUtils.getSegmentChunkName(segmentName, startOffset), startOffset);
    }

    /**
     * Creates a new instance of the SegmentChunk class with the same information as this one, but with a new offset.
     *
     * @param newOffset The new offset.
     * @return A new SegmentChunk.
     */
    SegmentChunk withNewOffset(long newOffset) {
        SegmentChunk ns = new SegmentChunk(this.name, newOffset);
        ns.setLength(getLength());
        if (isSealed()) {
            ns.markSealed();
        }

        if (!exists()) {
            ns.markInexistent();
        }

        return ns;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating whether this SegmentChunk has been sealed.
     */
    synchronized boolean isSealed() {
        return this.sealed;
    }

    /**
     * Records the fact that this SegmentChunk has been sealed.
     */
    synchronized void markSealed() {
        this.sealed = true;
    }

    /**
     * Records the fact that this SegmentChunk has been unsealed.
     */
    synchronized void markUnsealed() {
        this.sealed = false;
    }

    /**
     * Gets a value indicating whether this SegmentChunk exists or not.
     */
    synchronized boolean exists() {
        return this.exists;
    }

    /**
     * Records the fact that this SegmentChunk no longer exists.
     */
    synchronized void markInexistent() {
        this.exists = false;
    }

    /**
     * Gets a value indicating the current length of this SegmentChunk.
     */
    synchronized long getLength() {
        return this.length;
    }

    /**
     * Increases the length of the SegmentChunk by the given delta.
     * @param delta The value to increase by.
     */
    synchronized void increaseLength(long delta) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SegmentChunk.");
        Preconditions.checkArgument(delta >= 0, "Cannot decrease the length of a SegmentChunk.");
        this.length += delta;
    }

    /**
     * Sets the length of the SegmentChunk.
     * @param length The new length.
     */
    synchronized void setLength(long length) {
        Preconditions.checkState(!this.sealed, "Cannot increase the length of a sealed SegmentChunk.");
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        this.length = length;
    }

    /**
     * Gets a value indicating the last Offset of this SegmentChunk.
     */
    synchronized long getLastOffset() {
        return this.startOffset + this.length;
    }

    @Override
    public synchronized String toString() {
        return String.format("%s (%d+%d%s)", this.name, this.startOffset, this.length,
                this.exists ? (this.sealed ? ", sealed" : "") : ", deleted");
    }

    //endregion
}