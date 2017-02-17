/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage;

/**
 * Defines a Factory for Storage Adapters.
 */
public interface StorageFactory extends AutoCloseable {
    /**
     * Gets a reference to an instance of a Storage class.
     */
    Storage getStorageAdapter();

    @Override
    void close();
}
