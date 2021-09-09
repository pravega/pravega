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

package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.integration.selftest.adapters.TruncateableArray;
import java.io.IOException;
import lombok.Getter;
import lombok.SneakyThrows;

/**
 * Represents an Event with a Routing Key and payload that can be appended to a Stream.
 */
public class Event implements ProducerUpdate {
    //region Members

    private static final int PREFIX_LENGTH = Integer.BYTES;
    private static final int OWNER_ID_LENGTH = Integer.BYTES;
    private static final int ROUTING_KEY_LENGTH = Integer.BYTES;
    private static final int SEQUENCE_LENGTH = Integer.BYTES;
    private static final int START_TIME_LENGTH = Long.BYTES;
    private static final int LENGTH_LENGTH = Integer.BYTES;
    static final int HEADER_LENGTH = PREFIX_LENGTH + OWNER_ID_LENGTH + ROUTING_KEY_LENGTH + SEQUENCE_LENGTH + START_TIME_LENGTH + LENGTH_LENGTH;
    private static final int PREFIX = (int) Math.pow(Math.E, 20);

    /**
     * Event Owner. Generally test-assigned StreamId/SegmentId.
     */
    @Getter
    private final int ownerId;

    /**
     * Event Routing Key.
     */
    @Getter
    private final int routingKey;

    /**
     * Sequence within Owner.
     */
    @Getter
    private final int sequence;

    /**
     * Test-assigned start time, in Nanos. This value only makes sense within the context of the Test process that
     * created this object.
     */
    @Getter
    private final long startTime;

    /**
     * Length of the Event payload, in bytes (this excludes the HEADER_LENGTH, which will need to be added in order to
     * determine full serialization length.
     */
    @Getter
    private final int contentLength;

    /**
     * Full serialization, including Header and Content.
     */
    @Getter
    private final ArrayView serialization;

    @Getter
    private final ByteBuf writeBuffer;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Event class.
     *
     * @param ownerId    Owner Id (Stream Id, Segment Id, etc)
     * @param routingKey Routing Key to use.
     * @param sequence   Event Sequence Number.
     * @param startTime  Start (creation) time, in Nanos.
     * @param length     Desired length of the append.
     */
    Event(int ownerId, int routingKey, int sequence, long startTime, int length) {
        this.ownerId = ownerId;
        this.routingKey = routingKey;
        this.sequence = sequence;
        this.startTime = startTime;
        this.serialization = new ByteArraySegment(serialize(length));
        this.contentLength = this.serialization.getLength() - PREFIX_LENGTH;
        this.writeBuffer = Unpooled.wrappedBuffer(this.serialization.array(), this.serialization.arrayOffset(), this.serialization.getLength());
    }

    /**
     * Creates a new instance of the Event class.
     *
     * @param source       A Source ArrayView to deserialize from.
     * @param sourceOffset A starting offset within the Source where to begin deserializing from.
     */
    @SneakyThrows(IOException.class)
    public Event(ArrayView source, int sourceOffset) {
        // Extract prefix and validate.
        int prefix = readInt(source, sourceOffset);
        sourceOffset += PREFIX_LENGTH;
        Preconditions.checkArgument(prefix == PREFIX, "Prefix mismatch.");

        // Extract ownerId.
        this.ownerId = readInt(source, sourceOffset);
        sourceOffset += OWNER_ID_LENGTH;

        this.routingKey = readInt(source, sourceOffset);
        sourceOffset += ROUTING_KEY_LENGTH;

        this.sequence = readInt(source, sourceOffset);
        sourceOffset += SEQUENCE_LENGTH;

        // Extract start time.
        this.startTime = readLong(source, sourceOffset);
        sourceOffset += START_TIME_LENGTH;

        // Extract length.
        this.contentLength = readInt(source, sourceOffset);
        sourceOffset += LENGTH_LENGTH;
        Preconditions.checkArgument(this.contentLength >= 0, "Payload length must be a positive integer.");
        Preconditions.checkElementIndex(sourceOffset + contentLength, source.getLength() + 1, "Insufficient data in given source.");

        // We make a copy of the necessary range in the input ArrayView since it may be unusable by the time we read from
        // it again. TODO: make TruncateableArray return a sub-TruncateableArray which does not require copying.
        if (source instanceof TruncateableArray || sourceOffset != HEADER_LENGTH) {
            this.serialization = new ByteArraySegment(StreamHelpers.readAll(
                    source.getReader(sourceOffset - HEADER_LENGTH, getTotalLength()), getTotalLength()));
        } else {
            this.serialization = source;
        }
        this.writeBuffer = Unpooled.wrappedBuffer(this.serialization.array(), this.serialization.arrayOffset(), this.serialization.getLength());
    }

    private int readInt(ArrayView truncateableArray, int offset) {
        // TruncateableArray does not support a BufferView.Reader, so we need to read our own numbers.
        return BitConverter.makeInt(truncateableArray.get(offset),
                truncateableArray.get(offset + 1),
                truncateableArray.get(offset + 2),
                truncateableArray.get(offset + 3));
    }

    private long readLong(ArrayView truncateableArray, int offset) {
        // TruncateableArray does not support a BufferView.Reader, so we need to read our own numbers.
        return BitConverter.makeLong(truncateableArray.get(offset),
                truncateableArray.get(offset + 1),
                truncateableArray.get(offset + 2),
                truncateableArray.get(offset + 3),
                truncateableArray.get(offset + 4),
                truncateableArray.get(offset + 5),
                truncateableArray.get(offset + 6),
                truncateableArray.get(offset + 7));
    }

    //endregion

    //region Properties

    @Override
    public void release() {
        this.writeBuffer.release();
    }

    /**
     * Gets a value indicating the total length of the append, including the Header and its Contents.
     *
     * @return The result.
     */
    public int getTotalLength() {
        return HEADER_LENGTH + this.contentLength;
    }

    @Override
    public String toString() {
        return String.format("Owner = %d, Key = %d, Sequence = %d, Start = %d, Length = %d",
                this.ownerId, this.routingKey, this.sequence, this.startTime, getTotalLength());
    }

    //endregion

    //region Serialization, Deserialization and Validation

    /**
     * Format: [Header][Key][Length][Contents]
     * * [Header]: is a sequence of bytes identifying the start of an append
     * * [OwnerId]: the owning Stream/Segment id
     * * [RoutingKey]: the event's routing key
     * * [Sequence]: the event's sequence number
     * * [StartTime]: the event's start time.
     * * [Length]: length of [Contents]
     * * [Contents]: a deterministic result of [Key] & [Length].
     */
    private byte[] serialize(int length) {
        Preconditions.checkArgument(length >= HEADER_LENGTH, "length is insufficient to accommodate header.");
        byte[] payload = new byte[length];

        // Header: PREFIX + ownerId + routingKey + sequence + start time + Key + Length
        int offset = 0;
        offset += BitConverter.writeInt(payload, offset, PREFIX);
        offset += BitConverter.writeInt(payload, offset, this.ownerId);
        offset += BitConverter.writeInt(payload, offset, this.routingKey);
        offset += BitConverter.writeInt(payload, offset, this.sequence);
        offset += BitConverter.writeLong(payload, offset, this.startTime);
        int contentLength = length - HEADER_LENGTH;
        offset += BitConverter.writeInt(payload, offset, contentLength);
        assert offset == HEADER_LENGTH : "Event header has a different length than expected";

        // Content
        writeContent(payload, offset);
        return payload;
    }

    private void writeContent(byte[] result, int offset) {
        int nextValue = this.sequence;
        while (offset < result.length) {
            result[offset++] = (byte) (nextValue % 255 + Byte.MIN_VALUE);
            nextValue += this.routingKey + 1;
        }
    }

    /**
     * Validates the contents of the Event, based on information from its Header.
     */
    void validateContents() {
        int offset = HEADER_LENGTH;
        int endOffset = offset + this.contentLength;
        int nextValue = this.sequence;
        while (offset < endOffset) {
            byte expectedValue = (byte) (nextValue % 255 + Byte.MIN_VALUE);
            if (this.serialization.get(offset) != expectedValue) {
                throw new IllegalStateException(String.format(
                        "Event Corrupted. Payload at index %d differs. Expected %d, actual %d.",
                        offset - HEADER_LENGTH, expectedValue, this.serialization.get(offset)));
            }

            nextValue += this.routingKey + 1;
            offset++;
        }
    }

    //endregion
}
