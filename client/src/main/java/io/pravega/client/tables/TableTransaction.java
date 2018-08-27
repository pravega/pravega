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
 * Defines all operations that can be made on a Table Transaction.
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface TableTransaction<KeyT, ValueT> extends AutoCloseable {
    /**
     * Inserts or updates an existing Table Entry into this Table Segment. See {@link TableWriter#put(VersionedEntry)}.
     *
     * @param entry The Entry to insert or update. If {@link VersionedEntry#getVersion()} is null, this will perform a
     *              Blind Update, otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(VersionedEntry<KeyT, ValueT> entry);

    /**
     * Removes the given key from this Table Segment. See {@link TableWriter#remove(VersionedKey)}.
     * @param key  The Key to remove. If {@link VersionedKey#getVersion()} is null, this will perform a Blind Update,
     *             otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(VersionedKey<KeyT> key);

    /**
     * Commits the contents of this Transaction into the parent Table as one
     * atomic update.
     */
    CompletableFuture<Void> commit();

    /**
     * Aborts this transaction.
     */
    CompletableFuture<Void> abort();
}
