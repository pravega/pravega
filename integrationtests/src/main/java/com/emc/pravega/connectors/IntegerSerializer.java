/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.connectors;

import com.emc.pravega.stream.Serializer;

import java.nio.ByteBuffer;

public final class IntegerSerializer implements Serializer<Integer> {
    @Override
    public ByteBuffer serialize(Integer value) {
        ByteBuffer result = ByteBuffer.allocate(4).putInt(value);
        result.rewind();
        return result;
    }

    @Override
    public Integer deserialize(ByteBuffer serializedValue) {
        return serializedValue.getInt();
    }
}