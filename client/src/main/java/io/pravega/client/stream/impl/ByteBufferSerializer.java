package io.pravega.client.stream.impl;

import io.pravega.client.stream.Serializer;
import java.nio.ByteBuffer;

/**
 * An implementation of {@link Serializer} that accepts byteBuffers.
 */
public class ByteBufferSerializer implements Serializer<ByteBuffer> {

    @Override
    public ByteBuffer serialize(ByteBuffer value) {
        return value.slice().asReadOnlyBuffer();
    }

    @Override
    public ByteBuffer deserialize(ByteBuffer serializedValue) {
        return serializedValue.slice();
    }

}
