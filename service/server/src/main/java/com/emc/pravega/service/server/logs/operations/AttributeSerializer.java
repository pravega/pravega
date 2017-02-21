/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.service.server.logs.operations;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Helps serialize and deserialize Attribute-Value pairs.
 */
public final class AttributeSerializer {
    /**
     * Serializes the given Attribute-Value pairs to the given DataOutputStream.
     *
     * @param attributes A Map of AttributeId-Value that contains the pairs to serialize.
     * @param stream     The DataOutputStream to write to.
     * @throws IOException If an exception occurred.
     */
    public static void serialize(Map<UUID, Long> attributes, DataOutputStream stream) throws IOException {
        stream.writeShort(attributes.size());
        for (Map.Entry<UUID, Long> attribute : attributes.entrySet()) {
            stream.writeLong(attribute.getKey().getMostSignificantBits());
            stream.writeLong(attribute.getKey().getLeastSignificantBits());
            stream.writeLong(attribute.getValue());
        }
    }

    /**
     * Deserializes a collection of Attribute-Value pairs from the given DataInputStream. The data must have been
     * serialized using the serialize() method.
     *
     * @param stream The DataInputStream to read from.
     * @return A Map of AttributeId-Value that contains the deserialized pairs.
     * @throws IOException If an exception occurred.
     */
    public static Map<UUID, Long> deserialize(DataInputStream stream) throws IOException {
        short attributeCount = stream.readShort();
        Map<UUID, Long> attributes = new HashMap<>();
        for (int i = 0; i < attributeCount; i++) {
            long mostSigBits = stream.readLong();
            long leastSigBits = stream.readLong();
            long value = stream.readLong();
            attributes.put(new UUID(mostSigBits, leastSigBits), value);
        }

        return attributes;
    }
}
