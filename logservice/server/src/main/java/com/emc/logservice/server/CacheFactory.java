package com.emc.logservice.server;

/**
 * Defines a Factory for StreamSegmentCaches.
 */
public interface CacheFactory {
    /**
     * Creates an instance of a Cache class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this cache.
     * @return The result.
     * @throws NullPointerException If any of the arguments are null.
     */
    Cache createCache(ContainerMetadata containerMetadata);
}
