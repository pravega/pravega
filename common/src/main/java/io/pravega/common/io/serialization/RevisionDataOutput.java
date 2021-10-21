/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.io.serialization;

import io.pravega.common.io.DirectDataOutput;
import io.pravega.common.util.BufferView;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToIntFunction;

/**
 * Extension to DataOutput that adds support for a few new constructs and supports formatting a Serialization Revision.
 * An instance of a RevisionDataOutput is created for each Serialization Revision and closed when that Revision's serialization
 * is done - it is not shared between multiple revisions.
 *
 * This interface is designed to serialize data that can be consumed using {@link RevisionDataInput}.
 */
public interface RevisionDataOutput extends DataOutput, DirectDataOutput {
    /**
     * Maximum value that can be encoded using {@link #writeCompactLong}.
     */
    long COMPACT_LONG_MAX = 0x3FFF_FFFF_FFFF_FFFFL - 1;

    /**
     * Minimum value that can be encoded using {@link #writeCompactLong}.
     */
    long COMPACT_LONG_MIN = 0L;

    /**
     * Maximum value that can be encoded using {@link #writeCompactSignedLong}.
     */
    long COMPACT_SIGNED_LONG_MAX = 0x1FFF_FFFF_FFFF_FFFFL - 1;

    /**
     * Minimum value that can be encoded using {@link #writeCompactSignedLong}.
     */
    long COMPACT_SIGNED_LONG_MIN = -0x1FFF_FFFF_FFFF_FFFFL;

    /**
     * Maximum value that can be encoded using {@link #writeCompactInt}.
     */
    int COMPACT_INT_MAX = 0x3FFF_FFFF - 1;

    /**
     * Minimum value that can be encoded using {@link #writeCompactInt}.
     */
    int COMPACT_INT_MIN = 0;

    /**
     * The number of bytes required to encode a UUID.
     */
    int UUID_BYTES = 2 * Long.BYTES;

    /**
     * Gets a value indicating whether this instance of a RevisionDataOutput requires {@link #length} to be called
     * prior to writing anything to it.
     *
     * @return True if Length must be declared beforehand (by invoking length()) or not.
     */
    boolean requiresExplicitLength();

    /**
     * If {@link #requiresExplicitLength} == true, this method will write 4 bytes at the current position representing the expected
     * serialization length (via the argument). In this case, this method must be called prior to invoking any write method
     * on this object.
     * If {@link #requiresExplicitLength} == false, this method will have no effect, since the length can be auto-calculated when
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
     * Calculates the length, in bytes, of the given String as serialized by using {@link #writeUTF}. Invoking this method will
     * not actually write the String.
     *
     * @param s The string to measure.
     * @return The {@link #writeUTF} length of the String. Note that this may be different from s.length().
     */
    int getUTFLength(String s);

    /**
     * Calculates the length, in bytes, of the given Long as serialized by using {@link #writeCompactLong}. Invoking this method
     * will not actually write the value.
     *
     * @param value The value to measure.
     * @return The {@link #writeCompactLong} length of the value. This is a value between 1 and {@link Long#BYTES} (inclusive).
     * @throws IllegalArgumentException If value is not in the interval [0, 2^62).
     */
    int getCompactLongLength(long value);

    /**
     * Encodes the given Long into a compact serialization of 1, 2, 4 or 8 bytes. The actual number of bytes can be
     * calculated using {@link #getCompactLongLength}. The first two bits of the given value are ignored and will be reserved
     * for serialization, hence this can only serialize values in the interval [0, 2^62).
     *
     * This value must be read using {@link RevisionDataInput#readCompactLong}. It cannot be read using {@link DataInput#readLong}
     * or using {@link RevisionDataInput#readCompactSignedLong}.
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is not in the interval [0, 2^62).
     */
    void writeCompactLong(long value) throws IOException;

    /**
     * Calculates the length, in bytes, of the given signed Long as serialized by using {@link #writeCompactSignedLong}.
     * Invoking this method will not actually write the value.
     *
     * @param value The value to measure.
     * @return The {@link #writeCompactSignedLong} length of the value. This is a value between 1 and {@link Long#BYTES} (inclusive).
     * @throws IllegalArgumentException If value is not in the interval [-2^61, 2^61).
     */
    int getCompactSignedLongLength(long value);

    /**
     * Encodes the given signed Long into a compact serialization of 1, 2, 4 or 8 bytes. The actual number of bytes can be
     * calculated using {@link #getCompactSignedLongLength}. The first three bits of the given value are ignored and will
     * be reserved for serialization, hence this can only serialize values in the interval [-2^61, 2^61).
     * <p>
     * This value must be read using {@link RevisionDataInput#readCompactSignedLong}. It cannot be read using
     * {@link DataInput#readLong} or using {@link RevisionDataInput#readCompactLong}.
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is not in the interval [-2^61, 2^61).
     */
    void writeCompactSignedLong(long value) throws IOException;

    /**
     * Calculates the length, in bytes, of the given Integer as serialized by using {@link #writeCompactInt}. Invoking this method
     * will not actually write the value.
     *
     * @param value The value to measure.
     * @return The {@link #writeCompactInt} length of the value. This is a value between 1 and {@link Integer#BYTES} (inclusive).
     * @throws IllegalArgumentException If value is not in the interval [0, 2^30).
     */
    int getCompactIntLength(int value);

    /**
     * Encodes the given Integer into a compact serialization of 1, 2, 3 or 4 bytes. The actual number of bytes can be
     * calculated using {@link #getCompactIntLength}. The first two bits of the given value are ignored and will be reserved
     * or serialization, hence this can only serialize values in the interval [0, 2^30).
     *
     * This value must be read using {@link RevisionDataInput#readCompactInt}. It cannot be read using {@link DataInput#readInt}.
     *
     * @param value The value to serialize.
     * @throws IOException              If an IO Exception occurred.
     * @throws IllegalArgumentException If value is not in the interval [0, 2^30).
     */
    void writeCompactInt(int value) throws IOException;

    /**
     * Serializes the given UUID as two consecutive Long values.
     *
     * This value must be read using {@link RevisionDataInput#readUUID}.
     *
     * @param uuid The UUID to serialize.
     * @throws IOException If an IO Exception occurred.
     */
    void writeUUID(UUID uuid) throws IOException;

    /**
     * Calculates the number of bytes required to serialize a Collection or array. This method can be used to estimate the
     * serialization length of both {@link #writeCollection} and {@link #writeArray}.
     *
     * @param elementCount  The size of the collection.
     * @param elementLength The size (in bytes) of each element's serialization.
     * @return The number of bytes.
     */
    int getCollectionLength(int elementCount, int elementLength);

    /**
     * Calculates the number of bytes required to serialize a Collection. This method can be used to estimate the
     * serialization length of {@link #writeCollection}.
     *
     * @param collection            The Collection to measure.
     * @param elementLengthProvider A Function that, given an Element of type T, will return its serialization length.
     * @param <T>                   Type of the Collection's Elements.
     * @return The number of bytes.
     */
    <T> int getCollectionLength(Collection<T> collection, ToIntFunction<T> elementLengthProvider);

    /**
     * Calculates the number of bytes required to serialize an array. This method can be used to estimate the
     * serialization length of {@link #writeArray}.
     *
     * @param array                 The array to measure.
     * @param elementLengthProvider A Function that, given an Element of type T, will return its serialization length.
     * @param <T>                   Type of the Array's Elements
     * @return The number of bytes.
     */
    <T> int getCollectionLength(T[] array, ToIntFunction<T> elementLengthProvider);

    /**
     * Serializes the given Collection using the given {@link ElementSerializer}. It first writes a Compact Integer representing
     * the number of elements in the collection, followed by each element's serialization, in the same order as returned
     * by the Collection's iterator.
     *
     * @param collection        The Collection to serialize. Can be null (in which case an Empty Collection will be deserialized
     *                          by {@link RevisionDataInput#readCollection}).
     * @param elementSerializer A Function that serializes a single element of the collection to a {@link RevisionDataOutput}.
     * @param <T>               Type of the elements in the Collection.
     * @throws IOException If an IO Exception occurred.
     */
    <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException;

    /**
     * Serializes the given array using the given {@link ElementSerializer}. It first writes a Compact Integer representing
     * the number of elements in the array, followed by each element's serialization, in the order in which they appear in
     * the array.
     *
     * @param array             The array to serialize. Can be null (in which case an Empty array will be deserialized
     *                          by {@link RevisionDataInput#readArray}).
     * @param elementSerializer A Function that serializes a single element of the array to a {@link RevisionDataOutput}.
     * @param <T>               Type of the elements in the array.
     * @throws IOException If an IO Exception occurred.
     */
    <T> void writeArray(T[] array, ElementSerializer<T> elementSerializer) throws IOException;

    /**
     * Serializes the given byte array. Equivalent to calling {@link #writeArray}(array, 0, array.length)}.
     *
     * @param array The array to serialize. Can be null (in which case an Empty array will be deserialized
     *              by {@link RevisionDataInput#readArray}).
     * @throws IOException If an IO Exception occurred.
     */
    default void writeArray(byte[] array) throws IOException {
        writeArray(array, 0, array == null ? 0 : array.length);
    }

    /**
     * Serializes the given byte array. It first writes a Compact Integer representing the length to serialize, followed
     * by the actual array elements being written.
     *
     * @param array  The array to serialize. Can be null (in which case an Empty array will be deserialized
     *               by {@link RevisionDataInput#readArray})).
     * @param offset The offset within the array to begin serializing at. This is ignored if array == null.
     * @param length The number of elements to serialize. This is ignored if array == null.
     * @throws IOException If an IO Exception occurred.
     */
    void writeArray(byte[] array, int offset, int length) throws IOException;

    /**
     * Serializes the given {@link BufferView}. It first writes a Compact Integer representing {@link BufferView#getLength()},
     * followed by {@link BufferView#getLength()} bytes representing the data from the {@link BufferView}.
     *
     * This can be read back using {@link RevisionDataInput#readArray()} as this method serializes the data in the same
     * ways as {@link #writeArray}.
     *
     * @param buffer The {@link BufferView} to serialize. Can be null (in which case an Empty array will be deserialized
     *               by {@link RevisionDataInput#readArray})).
     * @throws IOException If an IO Exception occurred.
     */
    @Override
    void writeBuffer(BufferView buffer) throws IOException;

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
     * Calculates the number of bytes required to serialize a Map.
     *
     * @param map                 The Map to measure.
     * @param keyLengthProvider   A Function that, given a Key of type K, will return its serialization length.
     * @param valueLengthProvider A Function that, given a Value of type V, will return its serialization length.
     * @param <K>                 Type of the Map's Keys.
     * @param <V>                 Type of the Map's Values.
     * @return The number of bytes.
     */
    <K, V> int getMapLength(Map<K, V> map, ToIntFunction<K> keyLengthProvider, ToIntFunction<V> valueLengthProvider);

    /**
     * Serializes the given Map using the given {@link ElementSerializer}s (one for Key and one for Value). It first writes a
     * Compact Integer representing the number of elements in the Map, followed by each pair's serialization (first the key,
     * then the value), in the same order as returned by the Map's iterator.
     *
     * @param map             The Map to serialize. Can be null (in which case an Empty Map will be deserialized
     *                        by {@link RevisionDataInput#readMap}).
     * @param keySerializer   A Function that serializes a single Key of the Map to a {@link RevisionDataOutput}.
     * @param valueSerializer A Function that serializes a single Value of the Map to a {@link RevisionDataOutput}.
     * @param <K>             Type of the Map's Keys.
     * @param <V>             Type of the Map's Values.
     * @throws IOException If an IO Exception occurred.
     */
    <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException;

    /**
     * Defines a Function signature that can serialize an element to a {@link RevisionDataOutput}.
     *
     * @param <T> Type of the element to serialize.
     */
    @FunctionalInterface
    interface ElementSerializer<T> {
        void accept(RevisionDataOutput dataOutput, T element) throws IOException;
    }
}
