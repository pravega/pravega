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

import com.google.common.annotations.Beta;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that can be used to modify a Table.
 *
 * Types of updates:
 * * Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether that Key
 * previously existed or not, and regardless of what that Key's version is.
 * * Conditional Updates will only overwrite an existing value if the specified {@link KeyVersion} matches that Key's version.
 * * Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also succeed (albeit
 * with no effect) if the Key does not exist.
 * * Conditional Removals will remove a Key only if the specified {@link KeyVersion} matches that Key's version.
 *
 * @param <KeyT> Table Key Type.
 * @param <ValueT> Table Value Type.
 */
@Beta
public interface TableWriter<KeyT, ValueT> extends AutoCloseable {

    /**
     * Inserts a new Key-Value pair in this Table, but only if the Key does not already exist. Consider using
     * {@link #put(KeyT, ValueT)} should the intent be to insert or update an existing key.
     *
     * @param key   The Key to insert or update.
     * @param value The Value to set for the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> create(KeyT key, ValueT value);

    /**
     * Performs an Unconditional Update by inserting or updating the Value for a Key in this Table, regardless of whether
     * the Key exists or not or what its Version is.
     *
     * @param key   The Key to insert or update.
     * @param value The Value to set for the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value);

    /**
     * Performs a Conditional Update by updating the Value for an existing Key in this Table. The Update will only be
     * accepted if the given compareVersion matches the existing version for that Key, otherwise it will be rejected with
     * no effect on the Table.
     *
     * @param key            The Key to insert or update.
     * @param value          The Value to set for the Key.
     * @param compareVersion A {@link KeyVersion} that will need to match the version of the Key in the Table.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} In case the conditional update failed.
     * </ul>
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value, KeyVersion compareVersion);

    /**
     * Performs an Unconditional Removal of the given key from this Table, regardless if whether the Key exists or not or
     * what its Version is.
     * @param key  The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(KeyT key);

    /**
     * Performs a Conditional Removal of the given key from this Table. The Remove will only be accepted if the given
     * compareVersion matches the existing version for that Key, otherwise it will be rejected with no effect on the Table.
     *
     * @param key            The Key to remove.
     * @param compareVersion A {@link KeyVersion} that will need to match the version of the Key in the Table.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} In case the conditional update failed.
     * </ul>
     */
    CompletableFuture<Void> remove(KeyT key, KeyVersion compareVersion);

    /**
     * Begins a new Transaction for this Table.
     * @return A CompletableFuture that, when completed, will return a {@link TableTransaction} that can be used to
     * operate on the new transaction.
     */
    CompletableFuture<TableTransaction<KeyT, ValueT>> beginTransaction();

    @Override
    void close();
}
