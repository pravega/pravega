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

package com.emc.logservice.server.reading;

import com.emc.nautilus.common.Exceptions;

/**
 * An entry in the Read Index with data at a particular offset..
 */
abstract class ReadIndexEntry {
    //region Members

    private final long streamSegmentOffset;
    private final long length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadIndexEntry class.
     *
     * @param streamSegmentOffset The StreamSegment offset for this entry.
     * @throws IllegalArgumentException if the offset is a negative number.
     * @throws IllegalArgumentException if the length is a negative number.
     */
    protected ReadIndexEntry(long streamSegmentOffset, long length) {
        Exceptions.checkArgument(streamSegmentOffset >= 0, "streamSegmentOffset", "Offset must be a non-negative number.");
        Exceptions.checkArgument(length >= 0, "length", "Length must be a non-negative number.");

        this.streamSegmentOffset = streamSegmentOffset;
        this.length = length;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the StreamSegment offset for this entry.
     *
     * @return
     */
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the last Offset in the StreamSegment pertaining to this entry.
     *
     * @return
     */
    public long getLastStreamSegmentOffset() {
        return this.streamSegmentOffset + this.length - 1;
    }

    /**
     * Gets a byte array containing the data for this entry.
     *
     * @return
     */
    public long getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, Length = %d", getStreamSegmentOffset(), getLength());
    }

    //endregion
}
