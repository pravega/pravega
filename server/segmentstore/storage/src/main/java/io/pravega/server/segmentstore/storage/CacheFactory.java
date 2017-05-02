/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.storage;

/**
 * Defines a Factory for caches.
 */
public interface CacheFactory extends AutoCloseable {
    /**
     * Creates a new Cache with given id.
     *
     * @param id The Id of the Cache to create.
     */
    Cache getCache(String id);

    @Override
    void close();
}
