/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import lombok.Getter;

/**
 * Represents an Append with a Routing Key and payload.
 */
class Append {
    //region Members

    private static final int PREFIX_LENGTH = Integer.BYTES;
    private static final int OWNER_ID_LENGTH = Integer.BYTES;
    private static final int ROUTING_KEY_LENGTH = Integer.BYTES;
    private static final int SEQUENCE_LENGTH = Integer.BYTES;
    private static final int START_TIME_LENGTH = Long.BYTES;
    private static final int LENGTH_LENGTH = Integer.BYTES;
    static final int NO_ROUTING_KEY = -1;
    static final int HEADER_LENGTH = PREFIX_LENGTH + OWNER_ID_LENGTH + ROUTING_KEY_LENGTH + SEQUENCE_LENGTH + START_TIME_LENGTH + LENGTH_LENGTH;
    private static final int PREFIX = (int) Math.pow(Math.E, 20);

    @Getter
    private final int ownerId;
    @Getter
    private final int routingKey;
    @Getter
    private final int sequence;
    @Getter
    private final long startTime;
    @Getter
    private final int contentLength;
    @Getter
    private final ArrayView serialization;
    private final int contentOffset;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Append class.
     *
     * @param ownerId    Owner Id (Stream Id, Segment Id, etc)
     * @param routingKey Routing Key to use.
     * @param sequence   Append Sequence Number.
     * @param startTime  Start (creation) time, in Nanos.
     * @param length     Desired length of the append.
     */
    Append(int ownerId, int routingKey, int sequence, long startTime, int length) {
        this.ownerId = ownerId;
        this.routingKey = routingKey;
        this.sequence = sequence;
        this.startTime = startTime;
        this.serialization = new ByteArraySegment(serialize(length));
        this.contentLength = this.serialization.getLength() - PREFIX_LENGTH;
        this.contentOffset = HEADER_LENGTH;
    }

    /**
     * Creates a new instance of the Append class.
     *
     * @param source       A Source ArrayView to deserialize from.
     * @param sourceOffset A starting offset within the Source where to begin deserializing from.
     */
    Append(ArrayView source, int sourceOffset) {
        this.serialization = Preconditions.checkNotNull(source, "source");

        // Extract prefix and validate.
        int prefix = BitConverter.readInt(source, sourceOffset);
        sourceOffset += PREFIX_LENGTH;
        Preconditions.checkArgument(prefix == PREFIX, "Prefix mismatch.");

        // Extract ownerId.
        this.ownerId = BitConverter.readInt(source, sourceOffset);
        sourceOffset += OWNER_ID_LENGTH;

        this.routingKey = BitConverter.readInt(source, sourceOffset);
        sourceOffset += ROUTING_KEY_LENGTH;

        this.sequence = BitConverter.readInt(source, sourceOffset);
        sourceOffset += SEQUENCE_LENGTH;

        // Extract start time.
        this.startTime = BitConverter.readLong(source, sourceOffset);
        sourceOffset += START_TIME_LENGTH;

        // Extract length.
        this.contentLength = BitConverter.readInt(source, sourceOffset);
        sourceOffset += LENGTH_LENGTH;
        Preconditions.checkArgument(this.contentLength >= 0, "Payload length must be a positive integer.");
        Preconditions.checkArgument(sourceOffset + contentLength <= source.getLength(), "Insufficient data in given source.");
        this.contentOffset = sourceOffset;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the total length of the append, including the Header and its Contents.
     *
     * @return The result.
     */
    int getTotalLength() {
        return HEADER_LENGTH + this.contentLength;
    }

    @Override
    public String toString() {
        return String.format("Owner = %d, Key = %d, Sequence = %d, Start = %d, Length = %d",
                this.ownerId, this.routingKey, this.sequence, this.startTime, getTotalLength());
    }

    //endregion

    //region Serialization, Deserialization and Validation

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
        assert offset == HEADER_LENGTH : "Append header has a different length than expected";

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
     * Validates the contents of the Append, based on information from its Header.
     */
    void validateContents() {
        int offset = this.contentOffset;
        int endOffset = offset + this.contentLength;
        int nextValue = this.sequence;
        while (offset < endOffset) {
            byte expectedValue = (byte) (nextValue % 255 + Byte.MIN_VALUE);
            if (this.serialization.get(offset) != expectedValue) {
                throw new IllegalStateException(String.format(
                        "Append Corrupted. Payload at index %d differs. Expected %d, actual %d.",
                        offset - this.contentOffset, expectedValue, this.serialization.get(offset)));
            }

            nextValue += this.routingKey + 1;
            offset++;
        }
    }

    //endregion
}
