/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.storage.metadata;

import com.google.common.base.Preconditions;

import lombok.Getter;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implemenation of the MetadataTransaction.
 * This implementation delegates all calls to underlying storage.
 */
public class MetadataTransaction implements AutoCloseable {
    @Getter
    UUID id;
    ChunkMetadataStore store;
    boolean isCommited = false;
    @Getter
    private final ConcurrentHashMap<String, BaseMetadataStore.TransactionData> data;

    public MetadataTransaction(ChunkMetadataStore store)  {
        id = UUID.randomUUID();
        this.store = Preconditions.checkNotNull(store, "store");
        data = new ConcurrentHashMap<String, BaseMetadataStore.TransactionData>();
    }

    /**
     * Commits the transaction.
     * @throws Exception If transaction could not be commited.
     */
    public void commit() throws Exception {
        store.commit(this, false, false);
        isCommited = true;
    }


    /**
     * Commits the transaction.
     * @param lazyWrite true if data can be written lazily.
     * @throws Exception If transaction could not be commited.
     */
    public void commit(boolean lazyWrite) throws Exception {
        store.commit(this, lazyWrite, false);
        isCommited = true;
    }

    /**
     * Commits the transaction.
     * @param lazyWrite true if data can be written lazily.
     * @param skipStoreCheck true if data is not to be reloaded from store.
     * @throws Exception If transaction could not be commited.
     */
    public void commit(boolean lazyWrite, boolean skipStoreCheck) throws Exception {
        store.commit(this, lazyWrite, skipStoreCheck);
        isCommited = true;
    }

    /**
     * Aborts the transaction.
     * @throws Exception If transaction could not be commited.
     */
    public void abort() throws Exception {
        store.abort(this);
    }

    public void close() throws Exception {
        if (!isCommited) {
            store.abort(this);
        }
    }
}

