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
 * Notes about Updates:
 * * Updates (Blind or Conditional) in this TableTransaction will not be reflected in the main Table until {@link #commit()}
 * is invoked (and completes successfully).
 * * Conditional Updates in this TableTransaction can only be done by comparing with Keys in this Transaction. Comparisons
 * cannot be made against Keys from the main Table.
 * * {@link #commit()} cannot be made conditional against the state of the main Table.
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface TableTransaction<KeyT, ValueT> extends AutoCloseable {
    /**
     * Performs a Blind Update by inserting or updating the Value for a Key in this Table, regardless of whether the Key
     * exists or not or what its Version is. See {@link TableWriter#put(KeyT, ValueT)}.
     *
     * @param key   The Key to insert or update.
     * @param value The Value to set for the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value);

    /**
     * Performs a Conditional Update by inserting or updating the Value for a Key in this Table.
     * See {@link TableWriter#put(KeyT, ValueT, KeyVersion)}.
     *
     * @param key            The Key to insert or update.
     * @param value          The Value to set for the Key.
     * @param compareVersion A {@link KeyVersion} that will need to match the version of the Key in the Table. Consider
     *                       using {@link KeyVersion#NOT_EXISTS} if this is used to insert a Key which must not previously
     *                       exist.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value, KeyVersion compareVersion);

    /**
     * Performs a Blind Removal of the given key from this Table, regardless if whether the Key exists or not or what its
     * Version is. See {@link TableWriter#remove(KeyT)}.
     *
     * @param key The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(KeyT key);

    /**
     * Performs a Conditional Removal of the given key from this Table. See {@link TableWriter#remove(KeyT, KeyVersion)}.
     *
     * @param key            The Key to remove.
     * @param compareVersion A {@link KeyVersion} that will need to match the version of the Key in the Table.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(KeyT key, KeyVersion compareVersion);

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
