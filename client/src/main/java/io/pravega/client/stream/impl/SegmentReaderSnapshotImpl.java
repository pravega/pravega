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

package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.SegmentReaderSnapshot;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

import java.io.IOException;
import java.nio.ByteBuffer;

@EqualsAndHashCode(callSuper = true)
public class SegmentReaderSnapshotImpl extends SegmentReaderSnapshotInternal {
    private static final SegmentReaderSnapshotImpl.SegmentReaderSnapshotImplSerializer
            SERIALIZER = new SegmentReaderSnapshotImpl.SegmentReaderSnapshotImplSerializer();
    private final Segment segment;
    private final long position;
    private final boolean isEndOfSegment;

    @Builder
    SegmentReaderSnapshotImpl(Segment segment, long position, boolean isEndOfSegment) {
        Preconditions.checkNotNull(segment, "Segment ");
        this.segment = segment;
        this.position = position;
        this.isEndOfSegment = isEndOfSegment;
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @Override
    public String getSegmentId() {
        return segment.getScopedName();
    }

    @Override
    Segment getSegment() {
        return segment;
    }

    @Override
    long getPosition() {
        return position;
    }

    @Override
    boolean isEndOfSegment() {
        return isEndOfSegment;
    }

    @SneakyThrows(IOException.class)
    public static SegmentReaderSnapshot fromBytes(ByteBuffer buff) {
        return SERIALIZER.deserialize(new ByteArraySegment(buff));
    }

    static class SegmentReaderSnapshotImplBuilder implements ObjectBuilder<SegmentReaderSnapshotImpl> {
    }

    static class SegmentReaderSnapshotImplSerializer
            extends VersionedSerializer.WithBuilder<SegmentReaderSnapshotImpl, SegmentReaderSnapshotImpl.SegmentReaderSnapshotImplBuilder> {

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, SegmentReaderSnapshotImpl.SegmentReaderSnapshotImplBuilder readerSnapshotImplBuilder) throws IOException {
            readerSnapshotImplBuilder.isEndOfSegment(revisionDataInput.readBoolean());
            readerSnapshotImplBuilder.position(revisionDataInput.readCompactLong());
            readerSnapshotImplBuilder.segment(Segment.fromScopedName(revisionDataInput.readUTF()));
        }

        private void write00(SegmentReaderSnapshotImpl segmentReaderSnapshot, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeBoolean(segmentReaderSnapshot.isEndOfSegment);
            revisionDataOutput.writeCompactLong(segmentReaderSnapshot.position);
            revisionDataOutput.writeUTF(segmentReaderSnapshot.segment.getScopedName());
        }

        @Override
        protected SegmentReaderSnapshotImpl.SegmentReaderSnapshotImplBuilder newBuilder() {
            return SegmentReaderSnapshotImpl.builder();
        }
    }
}
