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
import io.pravega.segmentstore.server.ContainerMetadata;

import java.io.IOException;

/**
 * Log Operation that represents setting a Stream Segment as pinned.
 */
public class PinSegmentOperation extends MetadataOperation {

    //region Members

    private String streamSegmentName;
    private long streamSegmentId;
    private boolean pinned;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the PinSegmentOperation class for a non-transaction Segment.
     *
     * @param streamSegmentName Name of the Segment.
     * @param streamSegmentId Id for this Segment.
     * @param pinned If the Segment should be pinned.
     */
    public PinSegmentOperation(String streamSegmentName, long streamSegmentId, boolean pinned) {
        this.streamSegmentName = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.pinned = pinned;
    }

    /**
     * Deserialization constructor.
     */
    private PinSegmentOperation() {
    }

    //endregion

    //region PinOperation implementation.

    /**
     * Gets a value indicating the Name of the StreamSegment.
     * @return The Name of the StreamSegment.
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }

    /**
     * Gets a value indicating the Id of the StreamSegment.
     * @return The Id of the StreamSegment.
     */
    public long getStreamSegmentId() {
        return this.streamSegmentId;
    }

    /**
     * Gets a value indicating whether this Segment's Metadata is to be pinned to memory.
     *
     * @return True if pinned, false otherwise.
     */
    public boolean isPinned() {
        return this.pinned;
    }

    /**
     * Indicates that this Segment's Metadata is to be pinned to memory.
     */
    public void markPinned() {
        this.pinned = true;
    }

    @Override
    public String toString() {
        return String.format(
                "%s, Id = %s, Name = %s, Pinned = %s",
                super.toString(),
                toString(getStreamSegmentId(), ContainerMetadata.NO_STREAM_SEGMENT_ID),
                getStreamSegmentName(),
                isPinned());
    }

    //endregion

    static class Serializer extends OperationSerializer<PinSegmentOperation> {
        private static final int STATIC_LENGTH = 2 * Long.BYTES + Byte.BYTES;

        @Override
        protected OperationBuilder<PinSegmentOperation> newBuilder() {
            return new OperationBuilder<>(new PinSegmentOperation());
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
        protected void beforeSerialization(PinSegmentOperation o) {
            super.beforeSerialization(o);
            Preconditions.checkState(o.streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "StreamSegment Id has not been assigned.");
        }

        private void write00(PinSegmentOperation o, RevisionDataOutput target) throws IOException {
            target.length(STATIC_LENGTH + target.getUTFLength(o.streamSegmentName));
            target.writeLong(o.getSequenceNumber());
            target.writeLong(o.streamSegmentId);
            target.writeUTF(o.streamSegmentName);
            target.writeBoolean(o.pinned);
        }

        private void read00(RevisionDataInput source, OperationBuilder<PinSegmentOperation> b) throws IOException {
            b.instance.setSequenceNumber(source.readLong());
            b.instance.streamSegmentId = source.readLong();
            b.instance.streamSegmentName = source.readUTF();
            b.instance.pinned = source.readBoolean();
        }
    }
}
