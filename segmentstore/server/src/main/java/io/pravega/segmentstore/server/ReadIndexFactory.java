/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.ReadOnlyStorage;

/**
 * Defines a Factory for ReadIndex objects.
 */
public interface ReadIndexFactory extends AutoCloseable {
    /**
     * Creates an instance of a ReadIndex class with given arguments.
     *
     * @param containerMetadata A Container Metadata for this ReadIndex.
     * @param storage           A ReadOnlyStorage to use for reading data that is not in the cache.
     * @return The ReadIndex object for StreamSegments
     */
    ReadIndex createReadIndex(ContainerMetadata containerMetadata, ReadOnlyStorage storage);

    @Override
    void close();
}
