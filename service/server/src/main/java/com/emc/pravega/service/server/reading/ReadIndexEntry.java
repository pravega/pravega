/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.google.common.base.Preconditions;

/**
 * An entry in the Read Index with data at a particular offset.
 */
class ReadIndexEntry {
    //region Members

    private final long streamSegmentOffset;
    private final long length;
    private int generation;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @throws IllegalArgumentException if the offset is a negative number.
     * @throws IllegalArgumentException if the length is a negative number.
     */
    ReadIndexEntry(long streamSegmentOffset, long length) {
        Preconditions.checkArgument(streamSegmentOffset >= 0, "streamSegmentOffset must be a non-negative number.");
        Preconditions.checkArgument(length >= 0, "length", "length must be a non-negative number.");

        this.streamSegmentOffset = streamSegmentOffset;
        this.length = length;
    }

    //endregion

    //region Properties

    /**
     * Gets the value of the generation for this ReadIndexEntry.
     *
     * @return The entry's generation.
     */
    int getGeneration() {
        return this.generation;
    }

    /**
     * Sets the current generation of this ReadIndexEntry.
     *
     * @param generation The current generation.
     */
    void setGeneration(int generation) {
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
    long getLength() {
        return this.length;
    }

    /**
     * Gets a value indicating the last Offset in the StreamSegment pertaining to this entry.
     */
    long getLastStreamSegmentOffset() {
        return this.streamSegmentOffset + this.length - 1;
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, Length = %d, Gen = %d", this.streamSegmentOffset, this.length, this
                .generation);
    }

    //endregion
}
