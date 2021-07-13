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
import io.pravega.common.Exceptions;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import java.io.IOException;
import lombok.Getter;

/**
 * Log Operation that indicates a Segment is to be deleted.
 */
public class DeleteSegmentOperation extends StorageOperation {
    @Getter
    private long streamSegmentId;
    private long streamSegmentOffset;

    /**
     * Creates a new instance of the DeleteSegmentOperation class.
     *
     * @param segmentId The Id of the Segment to delete.
     */
    public DeleteSegmentOperation(long segmentId) {
        this.streamSegmentId = segmentId;
    }

    /**
     * Deserialization constructor.
     */
    private DeleteSegmentOperation() {
    }

    /**
     * Sets the length of the StreamSegment at the time of sealing.
     *
     * @param value The length.
     */
    public void setStreamSegmentOffset(long value) {
        Exceptions.checkArgument(value >= 0, "value", "StreamSegment Offset must be a non-negative number.");
        this.streamSegmentOffset = value;
    }

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public String toString() {
        return String.format("%s, SegmentId = %d", super.toString(), getStreamSegmentId());
    }

    @Override
    public OperationType getType() {
        return OperationType.Deletion; // Deletion operation type gets elevated priority.
    }

    static class Serializer extends OperationSerializer<DeleteSegmentOperation> {
        private static final int SERIALIZATION_LENGTH = 3 * Long.BYTES;

        @Override
        protected OperationBuilder<DeleteSegmentOperation> newBuilder() {
            return new OperationBuilder<>(new DeleteSegmentOperation());
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
        protected void beforeSerialization(DeleteSegmentOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentId > 0, "SegmentId has not been assigned.");
        }

        private void write00(DeleteSegmentOperation o, RevisionDataOutput target) throws IOException {
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.getStreamSegmentOffset());
        }

        private void read00(RevisionDataInput source, OperationBuilder<DeleteSegmentOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}
