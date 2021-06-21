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

import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import java.io.IOException;

/**
 * Log Operation that indicates a Segment is to be merged into another Segment.
 */
public class MergeSegmentOperation extends StorageOperation {
    //region Members

    private long streamSegmentOffset;
    private long length;
    private long sourceSegmentId;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MergeSegmentOperation class.
     *
     * @param targetSegmentId The Id of the Target StreamSegment (the StreamSegment to merge into).
     * @param sourceSegmentId The Id of the Source StreamSegment (the StreamSegment to be merged).
     */
    public MergeSegmentOperation(long targetSegmentId, long sourceSegmentId) {
        super(targetSegmentId);
        this.sourceSegmentId = sourceSegmentId;
        this.length = -1;
        this.streamSegmentOffset = -1;
    }

    /**
     * Deserialization constructor.
     */
    private MergeSegmentOperation() {
    }

    //endregion

    //region MergeSegmentOperation Properties

    /**
     * Gets a value indicating the Id of the Source StreamSegment (the StreamSegment to be merged).
     *
     * @return The Id.
     */
    public long getSourceSegmentId() {
        return this.sourceSegmentId;
    }

    /**
     * Sets the length of the Source StreamSegment.
     *
     * @param value The length.
     */
    public void setLength(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.length = value;
    }

    /**
     * Sets the offset of the Target StreamSegment to merge at.
     *
     * @param value The offset.
     */
    public void setStreamSegmentOffset(long value) {
        // No need for parameter validation here. We will check for them upon serialization.
        this.streamSegmentOffset = value;
    }

    //endregion

    //region Operation Implementation

    /**
     * Gets a value indicating the Offset in the Target StreamSegment to merge at.
     *
     * @return The offset.
     */
    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the Length of the Source StreamSegment.
     *
     * @return The length.
     */
    @Override
    public long getLength() {
        return this.length;
    }

    @Override
    public String toString() {
        return String.format(
                "%s, SourceSegmentId = %d, Length = %s, MergeOffset = %s",
                super.toString(),
                getSourceSegmentId(),
                toString(getLength(), -1),
                toString(getStreamSegmentOffset(), -1));
    }

    //endregion

    static class Serializer extends OperationSerializer<MergeSegmentOperation> {
        private static final int SERIALIZATION_LENGTH = 5 * Long.BYTES;

        @Override
        protected OperationBuilder<MergeSegmentOperation> newBuilder() {
            return new OperationBuilder<>(new MergeSegmentOperation());
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected void beforeSerialization(MergeSegmentOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.length >= 0, "Source StreamSegment Length has not been assigned.");
            Preconditions.checkState(o.streamSegmentOffset >= 0, "Target StreamSegment Offset has not been assigned.");
        }

        private void write00(MergeSegmentOperation o, RevisionDataOutput target) throws IOException {
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.sourceSegmentId);
            target.writeLong(o.length);
            target.writeLong(o.streamSegmentOffset);
        }

        private void read00(RevisionDataInput source, OperationBuilder<MergeSegmentOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.sourceSegmentId = source.readLong();
            b.instance.length = source.readLong();
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}
