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
import io.pravega.segmentstore.server.SegmentOperation;
import java.io.IOException;
import lombok.Getter;

/**
 * Log Operation that indicates a StreamSegment is to be truncated.
 */
public class StreamSegmentTruncateOperation extends StorageOperation implements SegmentOperation {
    //region Members

    @Getter
    private long streamSegmentId;
    /**
     * The Offset at which to truncate the StreamSegment.
     */
    @Getter
    private long streamSegmentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentTruncateOperation class.
     *
     * @param streamSegmentId The Id of the StreamSegment to truncate.
     * @param offset          The Offset at which to truncate.
     */
    public StreamSegmentTruncateOperation(long streamSegmentId, long offset) {
        super(streamSegmentId);
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
        this.streamSegmentId = streamSegmentId;
        this.streamSegmentOffset = offset;
    }

    /**
     * Deserialization constructor.
     */
    private StreamSegmentTruncateOperation() {
    }

    @Override
    public long getLength() {
        return 0;
    }

    //endregion

    //region Operation Implementation

    @Override
    public String toString() {
        return String.format("%s, Offset = %s", super.toString(), this.streamSegmentOffset);
    }

    //endregion

    static class Serializer extends OperationSerializer<StreamSegmentTruncateOperation> {
        private static final int SERIALIZATION_LENGTH = 3 * Long.BYTES;

        @Override
        protected OperationBuilder<StreamSegmentTruncateOperation> newBuilder() {
            return new OperationBuilder<>(new StreamSegmentTruncateOperation());
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void write00(StreamSegmentTruncateOperation o, RevisionDataOutput target) throws IOException {
            target.length(SERIALIZATION_LENGTH);
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.streamSegmentId);
            target.writeLong(o.streamSegmentOffset);
        }

        private void read00(RevisionDataInput source, OperationBuilder<StreamSegmentTruncateOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.streamSegmentOffset = source.readLong();
        }
    }
}
