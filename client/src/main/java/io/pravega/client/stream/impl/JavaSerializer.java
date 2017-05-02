/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;

import io.pravega.client.stream.Serializer;
import lombok.EqualsAndHashCode;

/**
 * An implementation of {@link Serializer} that uses Java serialization.
 */
@EqualsAndHashCode
public class JavaSerializer<T extends Serializable> implements Serializer<T>, Serializable {

    @Override
    public ByteBuffer serialize(T value) {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout;
        try {
            oout = new ObjectOutputStream(bout);
            oout.writeObject(value);
            oout.close();
            bout.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ByteBuffer.wrap(bout.toByteArray());
    }

    @Override
    @SuppressWarnings("unchecked")
    public T deserialize(ByteBuffer serializedValue) {
        ByteArrayInputStream bin = new ByteArrayInputStream(serializedValue.array(),
                serializedValue.position(),
                serializedValue.remaining());
        ObjectInputStream oin;
        try {
            oin = new ObjectInputStream(bin);
            return (T) oin.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
