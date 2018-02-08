/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import io.pravega.common.io.SerializationException;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

public interface RevisionDataInput extends DataInput {
    InputStream asStream();

    default long readCompactLong() throws IOException {
        //read first byte
        long b1 = readUnsignedByte();
        int header = (byte) (b1 >>> 6);
        b1 &= 0x3F;

        switch (header) {
            case 0:
                // Only this byte.
                return b1;
            case 1:
                // 2 bytes
                return (b1 << 8) + readUnsignedByte();
            case 2:
                // 4 bytes
                return (b1 << 24)
                        + ((long) readUnsignedByte() << 16)
                        + readUnsignedShort();
            case 3:
                // All 8 bytes
                return (b1 << 56)
                        + ((long) readUnsignedByte() << 48)
                        + ((long) readUnsignedShort() << 32)
                        + (readInt() & 0xFFFF_FFFFL);
            default:
                throw new SerializationException(String.format(
                        "Unable to deserialize compact long. Unrecognized header value %d.", header));
        }
    }

    default int readCompactInt() throws IOException {
        //read first byte
        int b1 = readUnsignedByte();
        int header = (byte) (b1 >>> 6);
        b1 &= 0x3F;

        switch (header) {
            case 0:
                // Only this byte.
                return b1;
            case 1:
                // 2 bytes
                return (b1 << 8) + readUnsignedByte();
            case 2:
                // 3 bytes
                return (b1 << 16)
                        + readUnsignedShort();
            case 3:
                // All 4 bytes
                return (b1 << 24)
                        + (readUnsignedByte() << 16)
                        + readUnsignedShort();
            default:
                throw new SerializationException(String.format(
                        "Unable to deserialize compact int. Unrecognized header value %d.", header));
        }
    }

    default UUID readUUID() throws IOException {
        return new UUID(readLong(), readLong());
    }

    default <T> Collection<T> readCollection(ElementDeserializer<T> elementDeserializer) throws IOException {
        return readCollection(elementDeserializer, ArrayList::new);
    }

    default <T, C extends Collection<T>> C readCollection(ElementDeserializer<T> elementDeserializer, Supplier<C> newCollection) throws IOException {
        C result = newCollection.get();
        int count = readInt();
        for (int i = 0; i < count; i++) {
            result.add(elementDeserializer.apply(this));
        }
        return result;
    }

    default <K, V> Map<K, V> readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer) throws IOException {
        return readMap(keyDeserializer, valueDeserializer, HashMap::new);
    }

    default <K, V, M extends Map<K, V>> M readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer, Supplier<M> newMap) throws IOException {
        M result = newMap.get();
        int count = readInt();
        for (int i = 0; i < count; i++) {
            result.put(keyDeserializer.apply(this), valueDeserializer.apply(this));
        }

        return result;
    }

    @FunctionalInterface
    interface ElementDeserializer<T> {
        T apply(RevisionDataInput dataInput) throws IOException;
    }

}
