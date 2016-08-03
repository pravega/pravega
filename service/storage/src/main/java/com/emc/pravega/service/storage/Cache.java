package com.emc.pravega.service.storage;

/**
 * Defines a Cache that can be used by the ReadIndex.
 */
public interface Cache {
    /**
     * Inserts a new CacheEntry.
     *
     * @param entry The entry to insert.
     */
    void insert(CacheEntry entry);

    /**
     * Retrieves a CacheEntry with given key.
     *
     * @param key The key to search by.
     * @return The CacheEntry, or null if no such entry exists.
     */
    CacheEntry get(CacheKey key);

    /**
     * Removes any CacheEntry that is associated with the given key.
     *
     * @param key The key of the entry to remove.
     * @return True if removed, false if no such entry exists.
     */
    boolean remove(CacheKey key);

    /**
     * Defines an entry in the Cache.
     */
    interface CacheEntry {
        /**
         * Gets a pointer to the Key of the CacheEntry,
         * @return The Key.
         */
        CacheKey getKey();

        /**
         * Gets a pointer to a byte array containing the data of the CacheEntry.
         * @return
         */
        byte[] getData();
    }

    /**
     * Defines a Key for an entry in the Cache.
     */
    interface CacheKey {

        /**
         * Gets a pointer to a byte array representing the serialization of the Cache Key.
         * @return
         */
        byte[] getSerialization();
    }
}
