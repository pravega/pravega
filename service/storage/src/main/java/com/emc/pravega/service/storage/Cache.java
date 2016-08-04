package com.emc.pravega.service.storage;

/**
 * Defines a Cache that can be used by the ReadIndex.
 */
public interface Cache<K extends Cache.Key> {
    /**
     * Inserts a new entry into the cache.
     *
     * @param key  The the key of the entry.
     * @param data The payload associated with the given key.
     */
    void insert(K key, byte[] data);

    /**
     * Retrieves a cache entry with given key.
     *
     * @param key The key to search by.
     * @return The payload associated with the key, or null if no such entry exists.
     */
    byte[] get(K key);

    /**
     * Removes any cache entry that is associated with the given key.
     *
     * @param key The key of the entry to remove.
     * @return True if removed, false if no such entry exists.
     */
    boolean remove(K key);

    /**
     * Defines a Key for an entry in the Cache.
     */
    interface Key {

        /**
         * Gets a pointer to a byte array representing the serialization of the Cache Key.
         *
         * @return
         */
        byte[] getSerialization();
    }
}
