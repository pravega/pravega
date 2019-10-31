/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

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
