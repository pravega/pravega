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
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.Stream;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.nio.ByteBuffer;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;

/**
 * Implementation of the EventPointer interface. We use
 * an EventPointerInternal abstract class as an intermediate
 * class to make pointer instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
public final class EventPointerImpl extends EventPointerInternal {
    private static final EventPointerSerializer SERIALIZER = new EventPointerSerializer();
    private final Segment segment;
    private final long eventStartOffset;
    private final int eventLength;

    @Builder(builderClassName = "EventPointerBuilder")
    EventPointerImpl(Segment segment, long eventStartOffset, int eventLength) {
        this.segment = segment;
        this.eventStartOffset = eventStartOffset;
        this.eventLength = eventLength;
    }

    @Override
    Segment getSegment() {
        return segment;
    }

    @Override
    long getEventStartOffset() {
        return eventStartOffset;
    }

    @Override
    int getEventLength() {
        return eventLength;
    }

    @Override
    public EventPointerInternal asImpl() {
        return this;
    }

    public static EventPointer fromString(String eventPointer) {
        int i = eventPointer.lastIndexOf(":");
        Preconditions.checkArgument(i > 0, "Invalid event pointer: %s", eventPointer);
        String[] offset = eventPointer.substring(i + 1).split("-");
        Preconditions.checkArgument(offset.length == 2, "Invalid event pointer: %s", eventPointer);
        return new EventPointerImpl(Segment.fromScopedName(eventPointer.substring(0, i)), Long.parseLong(offset[0]),
                                    Integer.parseInt(offset[1]));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(segment.getScopedName());
        sb.append(':');
        sb.append(eventStartOffset);
        sb.append('-');
        sb.append(eventLength);
        return sb.toString();
    }

    private static class EventPointerBuilder implements ObjectBuilder<EventPointerImpl> {
    }

    private static class EventPointerSerializer extends VersionedSerializer.WithBuilder<EventPointerImpl, EventPointerBuilder> {

        @Override
        protected EventPointerBuilder newBuilder() {
            return builder();
        }

        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        private void read00(RevisionDataInput revisionDataInput, EventPointerBuilder builder) throws IOException {
            builder.segment(Segment.fromScopedName(revisionDataInput.readUTF()));
            builder.eventStartOffset(revisionDataInput.readCompactLong());
            builder.eventLength(revisionDataInput.readCompactInt());
        }

        private void write00(EventPointerImpl pointer, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(pointer.getSegment().getScopedName());
            revisionDataOutput.writeCompactLong(pointer.eventStartOffset);
            revisionDataOutput.writeCompactInt(pointer.getEventLength());
        }
    }

    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }

    @SneakyThrows(IOException.class)
    public static EventPointerInternal fromBytes(ByteBuffer data) {
        return SERIALIZER.deserialize(new ByteArraySegment(data));
    }

    @Override
    public Stream getStream() {
        return Stream.of(segment.getScope(), segment.getStreamName());
    }

}
