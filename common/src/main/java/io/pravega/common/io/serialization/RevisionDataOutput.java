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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

/**
 * Extension to DataOutput that adds support for a few new constructs and supports formatting a Serialization Revision.
 * An instance of a RevisionDataOutput is created for each Serialization Revision and closed when that Revision's serialization
 * is done - it is not shared between multiple revisions.
 *
 * This interface is designed to serialize data that can be consumed using RevisionDataInput.
 */
public interface RevisionDataOutput extends DataOutput {
    //region DataOutput Extensions

    /**
     * Gets a value indicating whether this instance of a RevisionDataOutput requires length() to be called prior to writing
     * anything to it.
     *
     * @return True if Length must be declared beforehand (by invoking length()) or not.
     */
    boolean requiresExplicitLength();

    /**
     * If requiresExplicitLength() == true, this method will write 4 bytes at the current position representing the expected
     * serialization length (via the argument). In this case, this method must be called prior to invoking any write method
     * on this object.
     * If requiresExplicitLength() == false, this method will have no effect, since the length can be auto-calculated when
     * the RevisionDataOutput is closed.
     *
     * @param length The length to declare.
     * @throws IOException If an IO Exception occurred.
     */
    void length(int length) throws IOException;

    /**
     * Gets a pointer to the OutputStream that this RevisionDataOutput writes to.
     *
     * @return The OutputStream.
     */
    OutputStream getBaseStream();

    /**
     * Calculates the length, in bytes, of the given String as serialized by using writeUTF(). Invoking this method will
     * not actually write the String.
     *
     * @param s The string to measure.
     * @return The writeUTF() length of the String. Note that this may be different from s.length().
     */
    default int getUTFLength(String s) {
        // This code is extracted out of DataOutputStream.writeUTF(). If we change the underlying implementation, this
        // needs to change as well.
        int charCount = s.length();
        int length = 0;
        for (int i = 0; i < charCount; ++i) {
            char c = s.charAt(i);
            if (c >= 1 && c <= 127) {
                length++;
            } else if (c > 2047) {
                length += 3;
            } else {
                length += 2;
            }
        }
        return length;
    }

    /**
     * Calculates the length, in bytes, of the given Long as serialized by using writeCompactLong(). Invoking this method
     * will not actually write the value.
     *
     * @param value The value to measure.
     * @return The writeCompactLong() length of the value. This is a value between 1 and Long.BYTES (inclusive).
     * @throws IllegalArgumentException If value is negative or greater than 2^62-1.
     */
    default int getCompactLongLength(long value) {
        if (value < 0 || value >= 0x3FFF_FFFF_FFFF_FFFFL) {
            throw new IllegalArgumentException("writeCompactLong can only serialize non-negative longs up to 2^62.");
        } else if (value > 0x3FFF_FFFF) {
            return 8;
        } else if (value > 0x3FFF) {
            return 4;
        } else if (value > 0x3F) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * Encodes the given Long into a compact serialization of 1, 2, 4 or 8 bytes. The actual number of bytes can be
     * calculated using getCompactLongLength().
     * <p>
     * This value must be read using RevisionDataInput.readCompactLong(). It cannot be read using DataInput.readLong().
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is negative or greater than 2^62-1.
     */
    default void writeCompactLong(long value) throws IOException {
        if (value < 0 || value > 0x3FFF_FFFF_FFFF_FFFFL) {
            throw new IllegalArgumentException("writeCompactLong can only serialize non-negative longs up to 2^62.");
        } else if (value > 0x3FFF_FFFF) {
            // All 8 bytes
            writeInt((int) (value >>> 32 | 0xC000_0000));
            writeInt((int) value);
        } else if (value > 0x3FFF) {
            // Only 4 bytes.
            writeInt((int) (value | 0x8000_0000));
        } else if (value > 0x3F) {
            // Only 2 bytes.
            writeShort((short) (value | 0x4000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    /**
     * Calculates the length, in bytes, of the given Integer as serialized by using writeCompactInt(). Invoking this method
     * will not actually write the value.
     *
     * @param value The value to measure.
     * @return The writeCompactInt() length of the value. This is a value between 1 and Integer.BYTES (inclusive).
     * @throws IllegalArgumentException If value is negative or greater than 2^30-1.
     */
    default int getCompactIntLength(int value) {
        if (value < 0 || value >= 0x3FFF_FFFF) {
            throw new IllegalArgumentException("writeCompactInt can only serialize non-negative longs up to 2^30.");
        } else if (value > 0x3F_FFFF) {
            return 4;
        } else if (value > 0x3FFF) {
            return 3;
        } else if (value > 0x3F) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * Encodes the given Integer into a compact serialization of 1, 2, 3 or 4 bytes. The actual number of bytes can be
     * calculated using getCompactIntLength().
     * <p>
     * This value must be read using RevisionDataInput.readCompactInt(). It cannot be read using DataInput.readInt().
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is negative or greater than 2^60-1.
     */
    default void writeCompactInt(int value) throws IOException {
        if (value < 0 || value > 0x3FFF_FFFF) {
            throw new IllegalArgumentException("writeCompactInt can only serialize non-negative longs up to 2^30.");
        } else if (value > 0x3F_FFFF) {
            // All 4 bytes
            writeInt(value | 0xC000_0000);
        } else if (value > 0x3FFF) {
            // 3 bytes.
            writeByte((byte) (value >>> 16 & 0xFF | 0x80));
            writeShort((short) value);
        } else if (value > 0x3F) {
            // 2 Bytes.
            writeShort((short) (value | 0x4000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    /**
     * Serializes the given UUID as two consecutive Long values.
     * <p>
     * This value must be read using RevisionDataInput.readUUID().
     *
     * @param uuid The UUID to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    default void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    /**
     * Serializes the given Collection using the given ElementSerializer. It first writes a Compact Integer representing
     * the number of elements in the collection, followed by each element's serialization, in the same order as returned
     * by the Collection's iterator.
     *
     * @param collection        The Collection to serialize. Can be null (in which case an Empty Collection will be deserialized
     *                          by RevisionDataInput.readCollection()).
     * @param elementSerializer A Function that serializes a single element of the collection to a RevisionDataOutput.
     * @param <T>               Type of the elements in the Collection.
     * @throws IOException If an IO Exception occurred.
     */
    default <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException {
        if (collection == null) {
            writeCompactInt(0);
            return;
        }

        writeInt(collection.size());
        for (T e : collection) {
            elementSerializer.accept(this, e);
        }
    }

    /**
     * Serializes the given Map using the given ElementSerializers (one for Key and one for Value). It first writes a
     * Compact Integer representing the number of elements in the Map, followed by each pair's serialization (first the key,
     * then the value), in the same order as returned by the Map's iterator.
     *
     * @param map             The Map to serialize. Can be null (in which case an Empty Map will be deserialized
     *                        by RevisionDataInput.readCollection()).
     * @param keySerializer   A Function that serializes a single Key of the Map to a RevisionDataOutput.
     * @param valueSerializer A Function that serializes a single Value of the Map to a RevisionDataOutput.
     * @param <K>             Type of the Map's Keys.
     * @param <V>             Type of the Map's Values.
     * @throws IOException If an IO Exception occurred.
     */
    default <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException {
        if (map == null) {
            writeInt(0);
            return;
        }

        writeCompactInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet()) {
            keySerializer.accept(this, e.getKey());
            valueSerializer.accept(this, e.getValue());
        }
    }

    /**
     * Defines a Function signature that can serialize an element to a RevisionDataOutput.
     *
     * @param <T> Type of the element to serialize.
     */
    @FunctionalInterface
    interface ElementSerializer<T> {
        void accept(RevisionDataOutput dataOutput, T element) throws IOException;
    }

    //endregion

    //region Factory

    /**
     * Wraps the given OutputStream into an object implementing CloseableRevisionDataOutput (and implicitly RevisionDataOutput).
     *
     * @param outputStream The OutputStream to wrap.
     * @return A new instance of the RandomRevisionDataOutput class if the given outputStream is a RandomOutput (supports seeking),
     * or a new instance of the NonSeekableRevisionDataOutput otherwise.
     * @throws IOException If an IO Exception occurred. This is because the RandomRevisionDataOutput constructor pre-allocates
     *                     4 bytes for the length.
     */
    static CloseableRevisionDataOutput wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomOutput) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    /**
     * Defines a RevisionDataOutput that can be closed. This separation is needed since we don't want to expose the close()
     * method outside of this package (and give the caller a chance to prematurely close our OutputStream).
     */
    interface CloseableRevisionDataOutput extends RevisionDataOutput, AutoCloseable {
        @Override
        void close() throws IOException;
    }

    //endregion
}
