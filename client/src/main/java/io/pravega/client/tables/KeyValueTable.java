/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import io.pravega.common.util.AsyncIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface KeyValueTable<KeyT, ValueT> {
    /**
     * Inserts a new or updates an existing {@link TableEntry} that does not belong to any Key Family into this
     * {@link KeyValueTable}.
     *
     * @param entry The {Entry} to insert or update. If {@link TableEntry#getKey()}{@link TableKey#getVersion()} indicates
     *              a conditional update, this will perform an atomic update conditioned on the server-side version matching
     *              the provided one. See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<KeyVersion> put(TableEntry<KeyT, ValueT> entry);

    /**
     * Inserts a new or updates an existing {@link TableEntry} that belongs to a specific Key Family into this
     * {@link KeyValueTable}.
     *
     * @param keyFamily The Key Family for the {@link TableEntry}.
     * @param entry     The {Entry} to insert or update. If {@link TableEntry#getKey()}{@link TableKey#getVersion()}
     *                  indicates a conditional update, this will perform an atomic update conditioned on the server-side
     *                  version matching the provided one. See {@link KeyValueTable} doc for more details on Types of
     *                  Updates.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<KeyVersion> put(String keyFamily, TableEntry<KeyT, ValueT> entry) {
        return put(keyFamily, Collections.singletonList(entry)).thenApply(result -> result.get(0));
    }

    /**
     * Inserts new or updates existing {@link TableEntry} instances that belong to the same Key Family into this
     * {@link KeyValueTable}. All changes are performed atomically (either all or none will be accepted).
     *
     * @param keyFamily The Key Family for the all provided {@link TableEntry} instances.
     * @param entries   A List of {@link TableEntry to insert or update. If for at least one such entry,
     *                  {@link TableEntry#getKey()}{@link TableKey#getVersion()} indicates a conditional update,
     *                  this will perform an atomic Conditional Update where all the {@link TableEntry either get applied
     *                  or none will; otherwise a Unconditional Update will be performed.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain a List of {@link KeyVersion} instances which
     * represent the versions for the inserted/updated keys. The size of this list will be the same as entries.size()
     * and the versions will be in the same order as the entries. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<List<KeyVersion>> put(String keyFamily, List<TableEntry<KeyT, ValueT>> entries);

    /**
     * Removes a {@link TableKey} that does not belong to any Key Family from this Table Segment.
     *
     * @param key The Key to remove. If {@link TableKey#getVersion()} indicates a conditional update, this will
     *            perform an atomic removal conditioned on the server-side version matching the provided one.
     *            See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<Void> remove(TableKey<KeyT> key);

    /**
     * Removes a {@link TableKey} that does belongs to specific Key Family from this Table Segment.
     *
     * @param keyFamily The Key Family for the {@link TableKey}.
     * @param key       The Key to remove. If {@link TableKey#getVersion()} indicates a conditional update, this will
     *                  perform an atomic removal conditioned on the server-side version matching the provided one.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<Void> remove(String keyFamily, TableKey<KeyT> key) {
        return remove(keyFamily, Collections.singleton(key));
    }

    /**
     * Removes one or more {@link TableKey} instances that belong to the same Key Family from this Table Segment.
     * All removals are performed atomically (either all keys or no key will be removed).
     *
     * @param keyFamily The Key Family for the {@link TableKey}.
     * @param keys      A Collection of keys to remove. If for at least one such key, {@link TableKey#getVersion()}
     *                  indicates a conditional update, this will perform an atomic Conditional Remove where all the keys
     *                  either get removed or none will; otherwise an Unconditional Remove will be performed.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * This exception will contain the Keys that failed the validation.
     * </ul>
     */
    CompletableFuture<Void> remove(String keyFamily, Collection<TableKey<KeyT>> keys);

    default CompletableFuture<TableEntry<KeyT, ValueT>> get(KeyT key) {
        return get(Collections.singletonList(key)).thenApply(result -> result.get(0));
    }

    CompletableFuture<List<TableEntry<KeyT, ValueT>>> get(List<KeyT> keys);

    default CompletableFuture<TableEntry<KeyT, ValueT>> get(String keyFamily, KeyT key) {
        return get(keyFamily, Collections.singletonList(key)).thenApply(result -> result.get(0));
    }

    CompletableFuture<List<TableEntry<KeyT, ValueT>>> get(String keyFamily, List<KeyT> keys);

    AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(String keyFamily, int maxKeysAtOnce, IteratorState state);

    AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(String keyFamily, int maxEntriesAtOnce, IteratorState state);
}
