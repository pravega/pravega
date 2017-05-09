/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server;

import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.AttributeUpdateType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    /**
     * Serializes the given AttributeUpdates to the given DataOutputStream.
     *
     * @param attributeUpdates A Collection of AttributeUpdates to serialize.
     * @param stream           The DataOutputStream to write to.
     * @throws IOException If an exception occurred.
     */
    public static void serializeUpdates(Collection<AttributeUpdate> attributeUpdates, DataOutputStream stream) throws IOException {
        stream.writeShort(attributeUpdates == null ? 0 : attributeUpdates.size());
        if (attributeUpdates != null) {
            for (AttributeUpdate au : attributeUpdates) {
                UUID attributeId = au.getAttributeId();
                stream.writeLong(attributeId.getMostSignificantBits());
                stream.writeLong(attributeId.getLeastSignificantBits());
                stream.writeByte(au.getUpdateType().getTypeId());
                stream.writeLong(au.getValue());
            }
        }
    }

    /**
     * Deserializes a collection of AttributeUpdates from the given DataInputStream. The data must have been
     * serialized using the serializeUpdates() method.
     *
     * @param stream The DataInputStream to read from.
     * @return A Collection of AttributeUpdates.
     * @throws IOException If an exception occurred.
     */
    public static Collection<AttributeUpdate> deserializeUpdates(DataInputStream stream) throws IOException {
        short attributeCount = stream.readShort();
        Collection<AttributeUpdate> result;
        if (attributeCount > 0) {
            result = new ArrayList<>(attributeCount);
            for (int i = 0; i < attributeCount; i++) {
                long idMostSig = stream.readLong();
                long idLeastSig = stream.readLong();
                UUID attributeId = new UUID(idMostSig, idLeastSig);
                AttributeUpdateType updateType = AttributeUpdateType.get(stream.readByte());
                long value = stream.readLong();
                result.add(new AttributeUpdate(attributeId, updateType, value));
            }
        } else {
            result = Collections.emptyList();
        }

        return result;
    }
}
