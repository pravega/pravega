/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.logs.operations.AttributeSerializer;
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
    private final Map<UUID, Long> attributes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentState class.
     *
     * @param segmentProperties The SegmentProperties to create from.
     */
    SegmentState(SegmentProperties segmentProperties) {
        this(segmentProperties.getName(), segmentProperties.getAttributes());
    }

    private SegmentState(String segmentName, Map<UUID, Long> attributes) {
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
        target.writeUTF(segmentName);
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
            String segmentName = source.readUTF();
            Map<UUID, Long> attributes = AttributeSerializer.deserialize(source);
            return new SegmentState(segmentName, attributes);
        } else {
            throw new IOException(String.format("Unsupported version: %d.", version));
        }
    }

    //endregion
}
