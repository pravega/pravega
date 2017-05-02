/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.segmentstore.storage;

/**
 * Defines a Factory for Storage Adapters.
 */
public interface StorageFactory {
    /**
     * Creates a new instance of a Storage adapter.
     */
    Storage createStorageAdapter();
}
