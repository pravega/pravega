/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage;

import io.pravega.common.util.ByteArraySegment;

/**
 * Defines a Cache that can be used by the ReadIndex.
 */
public interface Cache extends AutoCloseable {
    /**
     * Gets a value indicating the Id of this cache.
     */
    String getId();

    /**
     * Inserts a new entry into the cache.
     *
     * @param key  The the key of the entry.
     * @param data The payload associated with the given key.
     */
    void insert(Key key, byte[] data);

    /**
     * Inserts a new entry into the cache.
     *
     * @param key  The the key of the entry.
     * @param data A ByteArraySegment representing the payload associated with the given key.
     */
    void insert(Key key, ByteArraySegment data);

    /**
     * Retrieves a cache entry with given key.
     *
     * @param key The key to search by.
     * @return The payload associated with the key, or null if no such entry exists.
     */
    byte[] get(Key key);

    /**
     * Removes any cache entry that is associated with the given key.
     *
     * @param key The key of the entry to remove.
     */
    void remove(Key key);

    /**
     * Closes this cache and releases all resources owned by it.
     */
    @Override
    void close();

    //region Key

    /**
     * Defines a generic Key for an entry in the Cache.
     */
    abstract class Key {

        /**
         * Gets a pointer to a byte array representing the serialization of the Cache Key.
         */
        public abstract byte[] serialize();

        /**
         * For in-memory representations of the Cache, hashCode() is required.
         */
        @Override
        public abstract int hashCode();

        /**
         * For in-memory representation of the Cache, equals() is required.
         */
        @Override
        public abstract boolean equals(Object obj);
    }

    //endregion
}
