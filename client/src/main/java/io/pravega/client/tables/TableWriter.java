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
 *
 * Types of updates:
 *  * * Blind Updates will insert and/or overwrite any existing values for the given Key, regardless of whether that Key
 *  * previously existed or not, and regardless of what that Key's version is.
 *  * * Conditional Updates will only overwrite an existing value if the specified version matches that Key's version. If
 *  * the key does not exist, the {@link TableKey} or {@link TableEntry} must have been created with {@link KeyVersion#NOT_EXISTS}
 *  * in order for the update to succeed.
 *  * * Blind Removals will remove a Key regardless of what that Key's version is. The operation will also succeed (albeit
 *  * with no effect) if the Key does not exist.
 *  * * Conditional Removals will remove a Key only if the specified {@link TableKey#getVersion()} matches that Key's version.
 *  * It will also fail (with no effect) if the Key does not exist and Version is not set to {@link KeyVersion#NOT_EXISTS}.
 *
 * @param <KeyT> Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface TableWriter<KeyT, ValueT> extends AutoCloseable {
    /**
     * Performs a Blind Update by inserting or updating the Value for a Key in this Table, regardless of whether the Key
     * exists or not or what its Version is.
     *
     * @param key   The Key to insert or update.
     * @param value The Value to set for the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(KeyT key, ValueT value);

    /**
     * Performs a Conditional Update by inserting or updating the Value for a Key in this Table.
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
     * Version is.
     * @param key  The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(KeyT key);

    /**
     * Performs a Conditional Removal of the given key from this Table.
     *
     * @param key            The Key to remove.
     * @param compareVersion A {@link KeyVersion} that will need to match the version of the Key in the Table.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
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
