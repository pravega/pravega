/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.pravega.common.util.AsyncIterator;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import lombok.Data;

/**
 * Defines all operations that are supported on a Table Segment.
 *
 * Types of updates:
 * * Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether that Key
 * previously existed or not, and regardless of what that Key's version is.
 * * Conditional Updates will only overwrite an existing value if the specified version matches that Key's version. If
 * the key does not exist, the {@link TableKey} or {@link TableEntry} must have been created with {@link KeyVersion#NOT_EXISTS}
 * in order for the update to succeed.
 * * Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also succeed (albeit
 * with no effect) if the Key does not exist.
 * * Conditional Removals will remove a Key only if the specified {@link TableKey#getVersion()} matches that Key's version.
 * It will also fail (with no effect) if the Key does not exist and Version is not set to {@link KeyVersion#NOT_EXISTS}.
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
interface TableSegment<KeyT, ValueT> extends AutoCloseable {

    /**
     * Inserts or updates an existing Table Entry into this Table Segment.
     * @param entry The Entry to insert or update. If {@link TableEntry#getKey()#getVersion()} is null, this will perform
     *              an Unconditional Update, otherwise it will perform a Conditional Update based on the information
     *              provided. See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<KeyVersion> put(TableEntry<KeyT, ValueT> entry);

    /**
     * Inserts new or updates existing Table Entries into this Table Segment.
     *
     * @param entries A Collection of entries to insert or update. If for at least one such entry,
     *                {@link TableEntry#getKey()#getVersion()} returns a non-null value, this will perform an atomic
     *                Conditional Update where all the entries either get applied or none will; otherwise a Unconditional
     *                Update will be performed. See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain a map of {@link KeyT} to {@link KeyVersion} which
     * represents the versions for the inserted/updated keys. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * This exception will contain the Keys that failed the validation.
     * </ul>
     */
    CompletableFuture<Map<KeyT, KeyVersion>> put(Collection<TableEntry<KeyT, ValueT>> entries);

    /**
     * Removes the given key from this Table Segment.
     * @param key  The Key to remove. If {@link TableKey#getVersion()} is null, this will perform an Unconditional Remove,
     *             otherwise it will perform a Conditional Remove based on the information provided.
     *             See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<Void> remove(TableKey<KeyT> key);

    /**
     * Removes one or more keys from this Table Segment.
     *
     * @param keys A Collection of keys to remove. If for at least one such key, {@link TableKey#getVersion()} returns
     *             a non-null value, this will perform an atomic Conditional Remove where all the keys either get removed
     *             or none will; otherwise an Unconditional Remove will be performed. See {@link TableSegment} doc for more
     *             details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * This exception will contain the Keys that failed the validation.
     * </ul>
     */
    CompletableFuture<Void> remove(Collection<TableKey<KeyT>> keys);

    /**
     * Gets the latest value for the given Key.
     *
     * @param key The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    CompletableFuture<TableEntry<KeyT, ValueT>> get(KeyT key);

    /**
     * Gets the latest values for the given Keys.
     *
     * @param keys A Collection of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a map of {@link KeyT} to {@link TableEntry} for those
     * keys that have a value in the index. All other keys will not be included.
     */
    CompletableFuture<Map<KeyT, TableEntry<KeyT, ValueT>>> get(Collection<KeyT> keys);

    /**
     * Creates and registers a {@link KeyUpdateListener} for all updates to this {@link TableSegment}, subject to the given
     * {@link KeyUpdateFilter}.
     *
     * @param filter   A {@link KeyUpdateFilter} that will specify which Keys should the {@link KeyUpdateListener} listen to.
     * @param executor An {@link Executor} that will be used to invoke all callbacks on.
     * @return A CompletableFuture that, when completed, will contain an {@link KeyUpdateListener}.
     */
    CompletableFuture<KeyUpdateListener<KeyT, ValueT>> createListener(KeyUpdateFilter<KeyT> filter, Executor executor);

    /**
     * Creates a new Iterator over all the Keys in the Table Segment.
     *
     * @param state An {@link IteratorState} that represents a continuation token that can be used to resume a previously
     *              interrupted iteration. This can be obtained by invoking {@link IteratorItem#getState()}. A null value
     *              will create an iterator that lists all keys.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Keys in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(IteratorState state);

    /**
     * Creates a new Iterator over all the Entries in the Table Segment.
     *
     * @param state An {@link IteratorState} that represents a continuation token that can be used to resume a previously
     *              interrupted iteration. This can be obtained by invoking {@link IteratorItem#getState()}. A null value
     *              will create an iterator that lists all Entries.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Entries in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(IteratorState state);

    @Override
    void close();

    /**
     * An iteration result item returned by {@link AsyncIterator} when invoking {@link #entryIterator(IteratorState)} or
     * {@link #keyIterator(IteratorState)}.
     *
     * @param <T> Iterator Item type..
     */
    @Data
    class IteratorItem<T> {
        /**
         * Gets an {@link IteratorState} that can be used to reinvoke {@link TableSegment#entryIterator(IteratorState)} or
         * {@link TableSegment#keyIterator(IteratorState)}if a previous iteration has been interrupted (by losing the
         * pointer to the {@link AsyncIterator}), system restart, etc.
         */
        private final IteratorState state;
        /**
         * A List of items that are contained in this instance. The items in this list are not necessarily related to each
         * other, nor are they guaranteed to be in any particular order.
         */
        private final List<T> items;
    }
}
