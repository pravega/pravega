/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.server.AttributeSerializer;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import lombok.Getter;

/**
 * Current state of a segment. Objects of this class can be serialized/deserialized to/from a State Store.
 */
class SegmentState {
    //region Members

    private static final byte SERIALIZATION_VERSION = 0;
    @Getter
    private final String segmentName;
    @Getter
    private final long segmentId;
    @Getter
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentState class.
     *
     * @param segmentId         The Id of the Segment.
     * @param segmentProperties The SegmentProperties to create from.
     */
    SegmentState(long segmentId, SegmentProperties segmentProperties) {
        this(segmentId, segmentProperties.getName(), segmentProperties.getAttributes());
    }

    private SegmentState(long segmentId, String segmentName, Map<UUID, Long> attributes) {
        this.segmentId = segmentId;
        this.segmentName = segmentName;
        this.attributes = attributes;
    }

    //endregion

    //region Serialization

    /**
     * Serializes this instance of the SegmentState to the given DataOutputStream.
     *
     * @param target The DataOutputStream to serialize to.
     * @throws IOException If an exception occurred.
     */
    public void serialize(DataOutputStream target) throws IOException {
        target.writeByte(SERIALIZATION_VERSION);
        target.writeLong(this.segmentId);
        target.writeUTF(this.segmentName);
        AttributeSerializer.serialize(this.attributes, target);
    }

    /**
     * Deserializes a new instance of the SegmentState class from the given DataInputStream.
     *
     * @param source The DataInputStream to deserialize from.
     * @return The deserialized SegmentState.
     * @throws IOException If an exception occured.
     */
    public static SegmentState deserialize(DataInputStream source) throws IOException {
        byte version = source.readByte();
        if (version == SERIALIZATION_VERSION) {
            long segmentId = source.readLong();
            String segmentName = source.readUTF();
            Map<UUID, Long> attributes = AttributeSerializer.deserialize(source);
            return new SegmentState(segmentId, segmentName, attributes);
        } else {
            throw new IOException(String.format("Unsupported version: %d.", version));
        }
    }

    //endregion
}
