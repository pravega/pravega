/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that can be used to modify a Table.
 * @param <KeyT> Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface TableWriter<KeyT, ValueT> extends AutoCloseable {
    /**
     * Inserts or updates an existing Table Entry into this Table.
     * @param entry The Entry to insert or update. If {@link VersionedEntry#getVersion()} is null, this will perform a
     *              Blind Update, otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(VersionedEntry<KeyT, ValueT> entry);

    /**
     * Removes the given key from this Table.
     * @param key  The Key to remove. If {@link VersionedKey#getVersion()} is null, this will perform a Blind Update,
     *             otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(VersionedKey<KeyT> key);

    /**
     * Begins a new Transaction for this Table.
     * @return A CompletableFuture that, when completed, will return a {@link TableTransaction} that can be used to
     * operate on the new transaction.
     */
    CompletableFuture<TableTransaction<KeyT, ValueT>> beginTransaction();

    @Override
    void close();
}
