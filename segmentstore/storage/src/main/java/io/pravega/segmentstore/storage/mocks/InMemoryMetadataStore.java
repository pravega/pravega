/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.mocks;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import lombok.val;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * InMemoryMetadataStore stores the key-values in memory.
 */
public class InMemoryMetadataStore extends BaseMetadataStore {

    private final AtomicBoolean entryTracker = new AtomicBoolean(false);

    /**
     * Write through cache.
     */
    private final Map<String, TransactionData> backingStore = new ConcurrentHashMap<>();

    /**
     * Reads a metadata record for the given key.
     *
     * @param key Key for the metadata record.
     * @return Associated {@link io.pravega.segmentstore.storage.metadata.BaseMetadataStore.TransactionData}.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    protected TransactionData read(String key) throws StorageMetadataException {
        synchronized (this) {
            val retValue = backingStore.get(key);
            if (null == retValue) {
                return TransactionData.builder()
                        .key(key)
                        .persisted(true)
                        .dbObject(this)
                        .build();
            }
            return retValue;
        }
    }

    /**
     * Writes transaction data from a given list to the metadata store.
     *
     * @param dataList List of transaction data to write.
     * @throws StorageMetadataException Exception related to storage metadata operations.
     */
    protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException {
        synchronized (this) {
            Preconditions.checkState(!entryTracker.getAndSet(true), "writeAll should never be called concurrently");
            try {
                for (TransactionData data : dataList) {
                    Preconditions.checkState(null != data.getKey());
                    val key = data.getKey();
                    if (backingStore.containsKey(key)) {
                        Preconditions.checkState(this == data.getDbObject(), "Data is not owned.");
                        val oldValue = backingStore.get(key);
                        if (!(oldValue.getVersion() < data.getVersion())) {
                            Preconditions.checkState(oldValue.getVersion() <= data.getVersion(), "Attempt to overwrite newer version");
                        }
                    }
                    data.setDbObject(this);
                    Preconditions.checkState(!data.isPinned(), "Pinned data should not be stored");
                    backingStore.put(key, data);
                }
            } finally {
                entryTracker.set(false);
            }
        }
    }

    /**
     * Creates a clone for given {@link InMemoryMetadataStore}. Useful to simulate zombie segment container.
     *
     * @param original Metadata store to clone.
     * @return Clone of given instance.
     */
    public static InMemoryMetadataStore clone(InMemoryMetadataStore original) {
        InMemoryMetadataStore cloneStore = new InMemoryMetadataStore();
        synchronized (original) {
            synchronized (cloneStore) {
                cloneStore.setVersion(original.getVersion());
                for (val entry : original.backingStore.entrySet()) {
                    val key = entry.getKey();
                    val transactionData = entry.getValue();
                    val cloneData = transactionData.toBuilder()
                            .key(key)
                            .value(transactionData.getValue() != null ? transactionData.getValue().deepCopy() : null)
                            .version(transactionData.getVersion())
                            .build();
                    Preconditions.checkState(transactionData.equals(cloneData));
                    // Make sure it is a deep copy.
                    if (transactionData.getValue() != null) {
                        Preconditions.checkState(transactionData.getValue() != cloneData.getValue());
                    }
                    cloneData.setDbObject(cloneStore);
                    cloneStore.backingStore.put(key, cloneData);
                }
            }
        }
        return cloneStore;
    }
}

