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
    /**
     * Maximum value that can be encoded using writeCompactLong().
     */
    long COMPACT_LONG_MAX = 0x3FFF_FFFF_FFFF_FFFFL;

    /**
     * Minimum value that can be encoded using writeCompactLong().
     */
    long COMPACT_LONG_MIN = 0L;

    /**
     * Maximum value that can be encoded using writeCompactInt().
     */
    int COMPACT_INT_MAX = 0x3FFF_FFFF;

    /**
     * Minimum value that can be encoded using writeCompactInt().
     */
    int COMPACT_INT_MIN = 0;

    /**
     * The number of bytes required to encode a UUID.
     */
    int UUID_BYTES = 2 * Long.BYTES;

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
    int getUTFLength(String s);

    /**
     * Calculates the length, in bytes, of the given Long as serialized by using writeCompactLong(). Invoking this method
     * will not actually write the value.
     *
     * @param value The value to measure.
     * @return The writeCompactLong() length of the value. This is a value between 1 and Long.BYTES (inclusive).
     * @throws IllegalArgumentException If value is negative or greater than 2^62-1.
     */
    int getCompactLongLength(long value);

    /**
     * Encodes the given Long into a compact serialization of 1, 2, 4 or 8 bytes. The actual number of bytes can be
     * calculated using getCompactLongLength(). The first two bits of the given value are ignored and will be reserved
     * for serialization, hence this can only serialize values in the interval [0, 2^62).
     * <p>
     * This value must be read using RevisionDataInput.readCompactLong(). It cannot be read using DataInput.readLong().
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is negative or greater than 2^62-1.
     */
    void writeCompactLong(long value) throws IOException;

    /**
     * Calculates the length, in bytes, of the given Integer as serialized by using writeCompactInt(). Invoking this method
     * will not actually write the value. The first two bits of the given value are ignored and will be reserved
     * for serialization, hence this can only serialize values in the interval [0, 2^30).
     *
     * @param value The value to measure.
     * @return The writeCompactInt() length of the value. This is a value between 1 and Integer.BYTES (inclusive).
     * @throws IllegalArgumentException If value is negative or greater than 2^30-1.
     */
    int getCompactIntLength(int value);

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
    void writeCompactInt(int value) throws IOException;

    /**
     * Serializes the given UUID as two consecutive Long values.
     * <p>
     * This value must be read using RevisionDataInput.readUUID().
     *
     * @param uuid The UUID to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    void writeUUID(UUID uuid) throws IOException;

    /**
     * Calculates the number of bytes required to serialize a Collection.
     *
     * @param elementCount  The size of the collection.
     * @param elementLength The size (in bytes) of each element's serialization.
     * @return The number of bytes.
     */
    int getCollectionLength(int elementCount, int elementLength);

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
    <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException;

    /**
     * Calculates the number of bytes required to serialize a Map.
     *
     * @param elementCount The size of the Map.
     * @param keyLength    The size (in bytes) of each key's serialization.
     * @param valueLength  The size (in bytes) of each value's serialization.
     * @return The number of bytes.
     */
    int getMapLength(int elementCount, int keyLength, int valueLength);

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
    <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException;

    /**
     * Defines a Function signature that can serialize an element to a RevisionDataOutput.
     *
     * @param <T> Type of the element to serialize.
     */
    @FunctionalInterface
    interface ElementSerializer<T> {
        void accept(RevisionDataOutput dataOutput, T element) throws IOException;
    }
}
