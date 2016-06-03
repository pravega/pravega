package com.emc.logservice.storageabstraction;

/**
 * Defines a Factory for Storage Adapters.
 */
public interface StorageFactory extends AutoCloseable{
    /**
     * Gets a reference to an instance of a Storage class.
     *
     * @return
     */
    Storage getStorageAdapter();

    @Override
    void close();
}
