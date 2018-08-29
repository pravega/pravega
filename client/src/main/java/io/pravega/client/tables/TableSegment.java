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
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface TableSegment<KeyT, ValueT> extends AutoCloseable {
    /**
     * Inserts or updates an existing Table Entry into this Table Segment.
     * @param entry The Entry to insert or update. If {@link VersionedEntry#getVersion()} is null, this will perform a
     *              Blind Update, otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(VersionedEntry<KeyT, ValueT> entry);

    /**
     * Inserts new or updates existing Table Entries into this Table Segment.
     *
     * @param entries A Collection of entries to insert or update. If for at least one such entry,
     *                {@link VersionedKey#getVersion()} returns a non-null value, this will perform an atomic
     *                Conditional Update where all the entries either get applied or none will; otherwise a Blind Update
     *                will be performed.
     * @return A CompletableFuture that, when completed, will contain a map of {@link KeyT} to {@link KeyVersion} which
     * represents the versions for the inserted/updated keys.
     */
    CompletableFuture<Map<KeyT, KeyVersion>> put(Collection<VersionedEntry<KeyT, ValueT>> entries);

    /**
     * Removes the given key from this Table Segment.
     * @param key  The Key to remove. If {@link VersionedKey#getVersion()} is null, this will perform a Blind Update,
     *             otherwise it will perform a Conditional Update based on the information provided.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(VersionedKey<KeyT> key);

    /**
     * Removes one or more keys from this Table Segment.
     *
     * @param keys A Collection of keys to remove. If for at least one such key, {@link VersionedKey#getVersion()} returns
     *             a non-null value, this will perform an atomic Conditional Update where all the keys either get removed
     *             or none will; otherwise a Blind Update will be performed.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed.
     */
    CompletableFuture<Void> remove(Collection<VersionedKey<KeyT>> keys);

    /**
     * Gets the latest value for the given Key.
     *
     * @param key The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    CompletableFuture<GetResult> get(KeyT key);

    /**
     * Gets the latest values for the given Keys.
     *
     * @param keys A Collection of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a map of {@link KeyT} to {@link GetResult} for those
     * keys that have a value in the index. All other keys will not be included.
     */
    CompletableFuture<Map<KeyT, GetResult<ValueT>>> get(Collection<KeyT> keys);

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
     * Creates a new Iterator over all the Entries in the Table Segment.
     *
     * @param continuationToken An {@link IteratorState} that represents a continuation token that can be used to resume
     *                          a previously interrupted iteration. This can be obtained by invoking
     *                          {@link IteratorItem#getContinuationToken()}. A null value will create an iterator that
     *                          lists all keys.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the Entries in this Table Segment.
     */
    CompletableFuture<AsyncIterator<IteratorItem<KeyT, ValueT>>> iterator(IteratorState continuationToken);

    @Override
    void close();

    /**
     * An iteration result item returned by {@link AsyncIterator} when invoking {@link #iterator(IteratorState)}.
     *
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     */
    @Data
    class IteratorItem<KeyT, ValueT> {
        /**
         * Gets an {@link IteratorState} that can be used to reinvoke {@link TableSegment#iterator(IteratorState)} if a
         * previous iteration has been interrupted (by losing the pointer to the {@link AsyncIterator}), system restart, etc.
         * Invoking {@link TableSegment#iterator(IteratorState)} will continue the iterator from the next item after this
         * one.
         */
        private final IteratorState continuationToken;
        /**
         * A List of {@link VersionedEntry} instances that are contained in this instance. For efficiency reasons, entries
         * may be batched together. The entries in this list are not necessarily related to each other, nor are they
         * guaranteed to be in any particular order.
         */
        private final List<VersionedEntry<KeyT, ValueT>> entries;
    }
}
