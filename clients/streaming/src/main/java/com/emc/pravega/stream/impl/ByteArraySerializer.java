/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Serializer;

import java.nio.ByteBuffer;

/**
 * An implementation of {@link Serializer} that converts byte arrays.
 */
public class ByteArraySerializer implements Serializer<byte[]> {
    @Override
    public ByteBuffer serialize(byte[] value) {
        return ByteBuffer.wrap(value);
    }

    @Override
    public byte[] deserialize(ByteBuffer serializedValue) {
        byte[] result = new byte[serializedValue.remaining()];
        serializedValue.get(result);
        return result;
    }
}
