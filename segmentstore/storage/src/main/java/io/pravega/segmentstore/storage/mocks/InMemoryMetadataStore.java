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

import io.pravega.segmentstore.storage.metadata.BaseMetadataStore;
import io.pravega.segmentstore.storage.metadata.StorageMetadataException;
import lombok.val;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * InMemoryMetadataStore stores the key-values in memory.
 */
public class InMemoryMetadataStore extends BaseMetadataStore {

    /**
     * Write through cache.
     */
    private final Map<String, TransactionData> backingStore = new ConcurrentHashMap<>();

    protected TransactionData read( String key) throws StorageMetadataException {
        return backingStore.get(key);
    }

    protected void writeAll(Collection<TransactionData> dataList) throws StorageMetadataException {
        for (TransactionData data : dataList) {
            backingStore.put(data.getValue().getKey(), data);
        }
    }

    public static InMemoryMetadataStore clone(InMemoryMetadataStore original) {
        InMemoryMetadataStore cloneStore = new InMemoryMetadataStore();
        for (val entry : original.backingStore.entrySet()) {
            cloneStore.backingStore.put(entry.getKey(),
                    entry.getValue().toBuilder().value(entry.getValue().getValue().copy()).build());
        }
        return cloneStore;
    }
}

