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
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.util.SortedIndex;
import javax.annotation.concurrent.GuardedBy;

/**
 * An entry in the Read Index with data at a particular offset.
 */
abstract class ReadIndexEntry implements SortedIndex.IndexEntry {
    //region Members

    private final long streamSegmentOffset;
    @GuardedBy("this")
    private int generation;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @throws IllegalArgumentException if the offset is a negative number.
     */
    ReadIndexEntry(long streamSegmentOffset) {
        Preconditions.checkArgument(streamSegmentOffset >= 0, "streamSegmentOffset must be a non-negative number.");

        this.streamSegmentOffset = streamSegmentOffset;
    }

    //endregion

    //region Properties

    /**
     * Gets the value of the generation for this ReadIndexEntry.
     *
     * @return The entry's generation.
     */
    synchronized int getGeneration() {
        return this.generation;
    }

    /**
     * Sets the current generation of this ReadIndexEntry.
     *
     * @param generation The current generation.
     */
    synchronized void setGeneration(int generation) {
        this.generation = generation;
    }

    /**
     * Gets a value indicating the StreamSegment offset for this entry.
     */
    long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets the length of this entry.
     */
    abstract long getLength();

    /**
     * Gets a value indicating the last Offset in the StreamSegment pertaining to this entry.
     */
    long getLastStreamSegmentOffset() {
        return this.streamSegmentOffset + getLength() - 1;
    }

    /**
     * Gets a value indicating whether this ReadIndexEntry actually points to data (vs. being a meta-entry).
     */
    abstract boolean isDataEntry();

    /**
     * Gets the address in the CacheStorage where the contents of this ReadIndexEntry is located. The result of this method
     * is undefined if {@link #isDataEntry()} is false.
     *
     * @return The CacheStorage address.
     */
    abstract int getCacheAddress();

    @Override
    public synchronized String toString() {
        return String.format("Offset = %d, Length = %d, Gen = %d", this.streamSegmentOffset, getLength(), this.generation);
    }

    @Override
    public long key() {
        return this.streamSegmentOffset;
    }

    //endregion
}
