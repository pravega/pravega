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
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeoutException;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.Retry;
import lombok.*;
import lombok.extern.slf4j.Slf4j;


@Beta
@Slf4j
public class SegmentIteratorImpl<T> implements SegmentIterator<T> {

    private final Segment segment;
    private final Serializer<T> deserializer;
    @Getter
    private final long startingOffset;
    private final long endingOffset;
    private final EventSegmentReader input;

    private final Retry.RetryWithBackoff backoffSchedule = Retry.withExpBackoff(1, 10, 9, 30000);

    private static final SegmentIteratorImpl.SegmentIteratorSerializer SERIALIZER = new SegmentIteratorImpl.SegmentIteratorSerializer();

    @Builder(builderClassName = "SegmentIteratorBuilder")
    public SegmentIteratorImpl(SegmentInputStreamFactory factory, Segment segment,
            Serializer<T> deserializer, long startingOffset, long endingOffset) {
        this.segment = segment;
        this.deserializer = deserializer;
        this.startingOffset = startingOffset;
        this.endingOffset = endingOffset;
        input = factory.createEventReaderForSegment(segment, startingOffset, endingOffset);
    }
    @Override
    public boolean hasNext() {
        return input.getOffset() < endingOffset;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // retry in-case of an empty ByteBuffer
        ByteBuffer read =
                backoffSchedule.retryWhen(t -> t instanceof TimeoutException)
                               .run(() -> {
                                   try {
                                       ByteBuffer buffer = input.read();
                                       if (buffer == null) {
                                           log.warn("Empty buffer while reading from Segment {} at offset {}",
                                                   input.getSegmentId(), input.getOffset());
                                           throw new TimeoutException(input.toString());
                                       }
                                       return buffer;
                                   } catch (NoSuchSegmentException | SegmentTruncatedException e) {
                                       throw new TruncatedDataException("Segment " + segment + " has been truncated.");
                                   }
                               });

        return deserializer.deserialize(read);
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void close() {
        input.close();
    }

    static class SegmentIteratorBuilder<T> implements ObjectBuilder<SegmentIteratorImpl> {
    }

    private static class SegmentIteratorSerializer extends VersionedSerializer.WithBuilder<SegmentIteratorImpl, SegmentIteratorBuilder<Object>> {
        @Override
        protected byte getWriteVersion() {
            return 0;
        }

        @Override
        protected void declareVersions() {
            version(0).revision(0, this::write00, this::read00);
        }

        @Override
        protected SegmentIteratorBuilder<Object> newBuilder() {
            return builder();
        }

        private void read00(RevisionDataInput revisionDataInput, SegmentIteratorImpl.SegmentIteratorBuilder builder) throws IOException {
            builder.segment(Segment.fromScopedName(revisionDataInput.readUTF()));
            builder.startingOffset(revisionDataInput.readCompactLong());
            builder.endingOffset(revisionDataInput.readCompactLong());
        }

        private void write00(SegmentIteratorImpl segmentIterator, RevisionDataOutput revisionDataOutput) throws IOException {
            revisionDataOutput.writeUTF(segmentIterator.segment.getScopedName());
            revisionDataOutput.writeCompactLong(segmentIterator.getStartingOffset());
            revisionDataOutput.writeCompactLong(segmentIterator.endingOffset);
        }

    }
    @Override
    @SneakyThrows(IOException.class)
    public ByteBuffer toBytes() {
        ByteArraySegment serialized = SERIALIZER.serialize(this);
        return ByteBuffer.wrap(serialized.array(), serialized.arrayOffset(), serialized.getLength());
    }
    @Override
    @SneakyThrows(IOException.class)
    public  SegmentIterator fromBytes(ByteBuffer serializedPosition) {
        return SERIALIZER.deserialize(new ByteArraySegment(serializedPosition));
    }
}
