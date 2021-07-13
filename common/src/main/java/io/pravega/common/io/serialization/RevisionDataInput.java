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

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.Supplier;

/**
 * Extension to DataInput that adds support for a few new constructs. An instance of RevisionDataInput is created for each
 * Serialization Revision and closed when that Revision's serialization is fully consumed - it is not shared between multiple revisions.
 *
 * This interface is designed to be used to consume data serialized using {@link RevisionDataOutput}.
 *
 */
public interface RevisionDataInput extends DataInput {
    /**
     * Gets a pointer to the InputStream that this RevisionDataInput reads from.
     *
     * @return The InputStream.
     */
    InputStream getBaseStream();

    /**
     * Gets the number of bytes remaining to read from the {@link RevisionDataInput}.
     * NOTE: this may be different from {@link InputStream#available()}; this returns the number of bytes remaining for
     * reading from those that were declared using {@link RevisionDataOutput#length(int)} at writing time.
     *
     * @return The number of bytes remaining.
     */
    int getRemaining();

    /**
     * Decodes a Long that has been serialized using {@link RevisionDataOutput#writeCompactLong}. After this method is complete,
     * the underlying InputStream may have advanced by 1, 2, 4, or 8 bytes.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeCompactLong}. It may throw a SerializationException (after reading 1 byte) or it may produce
     * a result that is not as expected.
     *
     * @return The decoded compact Long. This number should be in the interval [0, 2^62).
     * @throws IOException If an IO Exception occurred.
     */
    long readCompactLong() throws IOException;

    /**
     * Decodes a Long that has been serialized using {@link RevisionDataOutput#writeCompactSignedLong}. After this method
     * is complete, the underlying InputStream may have advanced by 1, 2, 4, or 8 bytes.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeCompactSignedLong}. It may throw a SerializationException (after reading 1 byte) or
     * it may produce a result that is not as expected.
     *
     * @return The decoded compact signed Long. This number should be in the interval [-2^61, 2^61).
     * @throws IOException If an IO Exception occurred.
     */
    long readCompactSignedLong() throws IOException;

    /**
     * Decodes an Integer that has been serialized using {@link RevisionDataOutput#writeCompactInt}. After this method is complete,
     * the underlying InputStream may have advanced by 1, 2, 3, or 4 bytes.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * RevisionDataOutput.writeCompactInt(). It may throw a SerializationException (after reading 1 byte) or it may produce
     * a result that is not as expected.
     *
     * @return The decoded compact Integer. This number should be in the interval [0, 2^30).
     * @throws IOException If an IO Exception occurred.
     */
    int readCompactInt() throws IOException;

    /**
     * Decodes a UUID that has been serialized using {@link RevisionDataOutput#writeUUID}.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeUUID}.
     *
     * @return A new UUID.
     * @throws IOException If an IO Exception occurred.
     */
    UUID readUUID() throws IOException;

    /**
     * Decodes a generic Collection that has been serialized using {@link RevisionDataOutput#writeCollection}. The underlying type
     * of the collection will be an ArrayList. Should a different type of Collection be desired, consider using the appropriate
     * overload of this method.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeCollection}.
     *
     * @param elementDeserializer A Function that will decode a single element of the Collection from the given RevisionDataInput.
     * @param <T>                 Type of the elements in the Collection.
     * @return A new Collection. If the original collection passed to {@link RevisionDataOutput#writeCollection} was null, this
     * will return an empty collection.
     * @throws IOException If an IO Exception occurred.
     */
    <T> Collection<T> readCollection(ElementDeserializer<T> elementDeserializer) throws IOException;

    /**
     * Decodes a specific Collection that has been serialized using {@link RevisionDataOutput#writeCollection}.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeCollection}.
     *
     * @param elementDeserializer A Function that will decode a single element of the Collection from the given RevisionDataInput.
     * @param newCollection       A Supplier that will create a new instance of the Collection type desired.
     * @param <T>                 Type of the elements in the Collection.
     * @param <C>                 Type of the Collection desired to be instantiated and returned.
     * @return A new Collection. If the original Collection passed to {@link RevisionDataOutput#writeCollection} was null, this
     * will return an empty collection.
     * @throws IOException If an IO Exception occurred.
     */
    <T, C extends Collection<T>> C readCollection(ElementDeserializer<T> elementDeserializer, Supplier<C> newCollection) throws IOException;

    /**
     * Decodes a specific Collection that has been serialized using {@link RevisionDataOutput#writeCollection}.
     * It populates the supplied builder with the deserialized collection elements. 
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeCollection}.
     *
     * @param elementDeserializer  A Function that will decode a single element of the Collection from the given RevisionDataInput.
     * @param newCollectionBuilder A {@link com.google.common.collect.ImmutableCollection.Builder} that will create a new instance of the
     *                             {@link com.google.common.collect.ImmutableCollection} of desired type.
     * @param <T>                  Type of the elements in the Collection.
     * @param <C>                  Type of the Collection whose builder needs to be populated.
     * @throws IOException If an IO Exception occurred.
     */
    <T, C extends ImmutableCollection<T>> void readCollection(
            ElementDeserializer<T> elementDeserializer, C.Builder<T> newCollectionBuilder) throws IOException;

    /**
     * Decodes a specific array that has been serialized using
     * {@link RevisionDataOutput#writeArray(Object[], RevisionDataOutput.ElementSerializer)}.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeArray(Object[], RevisionDataOutput.ElementSerializer)}.
     *
     * @param elementDeserializer A Function that will decode a single element of the Collection from the given RevisionDataInput.
     * @param newArray            A Function that will create a new instance of the array type desired, with the specified length.
     * @param <T>                 Type of the elements in the array.
     * @return A new array. If the original array passed to {@link RevisionDataOutput#writeArray} was null, this
     * will return an empty array.
     * @throws IOException If an IO Exception occurred.
     */
    <T> T[] readArray(ElementDeserializer<T> elementDeserializer, IntFunction<T[]> newArray) throws IOException;

    /**
     * Decodes a byte array that has been serialized using {@link RevisionDataOutput#writeArray(byte[])}.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeArray(byte[])}.
     *
     * @return A new byte array. If the original array passed to {@link RevisionDataOutput#writeArray(byte[])} was null, this
     * will return an empty array.
     * @throws IOException If an IO Exception occurred.
     */
    byte[] readArray() throws IOException;

    /**
     * Decodes a generic Map that has been serialized using {@link RevisionDataOutput#writeMap}. The underlying type of the map
     * will be a HashMap. Should a different type of Map be desired, consider using the appropriate overload of this method.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeMap}.
     *
     * @param keyDeserializer   A Function that will decode a single Key of the Map from the given RevisionDataInput.
     * @param valueDeserializer A Function that will decode a single Value of the Map from the given RevisionDataInput.
     * @param <K>               Type of the Keys in the Map.
     * @param <V>               Type of the Values in the Map.
     * @return A new Map. If the original Map passed to {@link RevisionDataOutput#writeMap} was null, this will return an empty map.
     * @throws IOException If an IOException occurred.
     */
    <K, V> Map<K, V> readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer) throws IOException;

    /**
     * Decodes a specific Map that has been serialized using {@link RevisionDataOutput#writeMap}.
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeMap}.
     *
     * @param keyDeserializer   A Function that will decode a single Key of the Map from the given RevisionDataInput.
     * @param valueDeserializer A Function that will decode a single Value of the Map from the given RevisionDataInput.
     * @param newMap            A Supplier that will create a new instance of the Map type desired.
     * @param <K>               Type of the Keys in the Map.
     * @param <V>               Type of the Values in the Map.
     * @param <M>               Type of the Map desired to be instantiated and returned.
     * @return A new Map. If the original Map passed to {@link RevisionDataOutput#writeMap} was null, this will return an empty map.
     * @throws IOException If an IOException occurred.
     */
    <K, V, M extends Map<K, V>> M readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer, Supplier<M> newMap) throws IOException;

    /**
     * Decodes a specific Map that has been serialized using {@link RevisionDataOutput#writeMap} and populates the supplied
     * ImmutableMap builder and builds the immutable map. 
     *
     * This method has undefined behavior if the data starting at the current position was not encoded using
     * {@link RevisionDataOutput#writeMap}.
     *
     * @param keyDeserializer   A Function that will decode a single Key of the Map from the given RevisionDataInput.
     * @param valueDeserializer A Function that will decode a single Value of the Map from the given RevisionDataInput.
     * @param newMapBuilder     An {@link com.google.common.collect.ImmutableMap.Builder} that will create a new
     *                          instance of the {@link com.google.common.collect.ImmutableMap}
     *                          type desired.
     * @param <K>               Type of the Keys in the Map.
     * @param <V>               Type of the Values in the Map.
     * @param <M>               Type of Map whose builder needs to be populated. 
     * @throws IOException If an IOException occurred.
     */
    <K, V, M extends ImmutableMap<K, V>> void readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer, 
                                          M.Builder<K, V> newMapBuilder) throws IOException;

    /**
     * Defines a Function signature that can deserialize an element from a RevisionDataInput.
     *
     * @param <T> Type of the element to deserialize.
     */
    @FunctionalInterface
    interface ElementDeserializer<T> {
        T apply(RevisionDataInput dataInput) throws IOException;
    }
}
