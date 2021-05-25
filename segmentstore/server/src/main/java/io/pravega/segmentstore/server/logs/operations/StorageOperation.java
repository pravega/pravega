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
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.segmentstore.server.SegmentOperation;

/**
 * Log Operation that deals with Storage Operations. This is generally the direct result of an external operation.
 */
public abstract class StorageOperation extends Operation implements SegmentOperation {
    //region Members

    private long streamSegmentId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StorageOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment this operation relates to.
     */
    public StorageOperation(long streamSegmentId) {
        super();
        setStreamSegmentId(streamSegmentId);
    }

    /**
     * Deserialization constructor.
     */
    protected StorageOperation() {
    }

    //endregion

    //region StorageOperation Properties

    @Override
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value indicating the Offset within the StreamSegment where this operation applies.
     * @return The offset within the Streamsegment.
     */
    public abstract long getStreamSegmentOffset();

    /**
     * Gets a value indicating the Length of this StorageOperation.
     * @return The value indicating the length of this StorageOperation.
     */
    public abstract long getLength();

    /**
     * Gets a value indicating the Offset within the StreamSegment following the last byte that this operation applies 
     * (i.e., the next write's offset).
     * @return The Offset within the StreamSegment following the last byte that this operation applies.
     */
    public long getLastStreamSegmentOffset() {
        return getStreamSegmentOffset() + getLength();
    }

    /**
     * Sets the Id of the StreamSegment.
     *
     * @param streamSegmentId The id to set.
     */
    protected void setStreamSegmentId(long streamSegmentId) {
        this.streamSegmentId = streamSegmentId;
    }

    @Override
    public String toString() {
        return String.format("%s, SegmentId = %d", super.toString(), getStreamSegmentId());
    }

    //endregion
}
