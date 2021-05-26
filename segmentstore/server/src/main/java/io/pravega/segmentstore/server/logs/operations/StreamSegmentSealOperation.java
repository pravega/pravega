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

/**
 * Log Operation that indicates a StreamSegment has been sealed.
 */
public class StreamSegmentSealOperation extends StorageOperation {
    //region Members

    private long streamSegmentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentSealOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to seal.
     */
    public StreamSegmentSealOperation(long streamSegmentId) {
        super(streamSegmentId);
        this.streamSegmentOffset = -1;
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentSealOperation() {
    }

    //endregion

    //region StreamSegmentSealOperation Implementation

    /**
     * Sets the length of the StreamSegment at the time of sealing.
     *
     * @param value The length.
     */
    public void setStreamSegmentOffset(long value) {
        Exceptions.checkArgument(value >= 0, "value", "StreamSegment Offset must be a non-negative number.");
        this.streamSegmentOffset = value;
    }

    //endregion

    //region Operation Implementation

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
        return String.format("%s, Length = %s", super.toString(), toString(this.streamSegmentOffset, -1));
    }

    //endregion

    static class Serializer extends OperationSerializer<StreamSegmentSealOperation> {
        private static final int SERIALIZATION_LENGTH = 3 * Long.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentSealOperation> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentSealOperation());
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
        protected void beforeSerialization(StreamSegmentSealOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentOffset >= 0, "StreamSegment Offset has not been assigned.");
        }

        private void write00(StreamSegmentSealOperation o, RevisionDataOutput target) throws IOException {
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.getStreamSegmentId());
            target.writeLong(o.streamSegmentOffset);
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentSealOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.setStreamSegmentId(source.readLong());
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}
