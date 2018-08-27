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
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
     * @param entries A list of entries to insert or update. If for at least one such entry, {@link VersionedKey#getVersion()}
     *                returns a non-null value, this will perform an atomic Conditional Update where all the entries either
     *                get applied or none will; otherwise a Blind Update will be performed.
     * @return A CompletableFuture that, when completed, will contain a list of {@link KeyVersion}s associated with the
     * newly inserted or updated entries. These will be in the same order as the entries given.
     */
    CompletableFuture<List<KeyVersion>> put(List<VersionedEntry<KeyT, ValueT>> entries);

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
     * @param keys A list of keys to remove. If for at least one such key, {@link VersionedKey#getVersion()} returns a
     *             non-null value, this will perform an atomic Conditional Update where all the keys either get removed
     *             or none will; otherwise a Blind Update will be performed.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed.
     */
    CompletableFuture<Void> remove(List<VersionedKey<KeyT>> keys);

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
     * @param keys A list of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a list of results for the given Keys. The results
     * will be in the same order as the keys. For any key that does not exist, a null value will be at its corresponding
     * index.
     */
    CompletableFuture<List<GetResult<ValueT>>> get(List<KeyT> keys);

    /**
     * Registers a listener for all updates to a single key.
     *
     * @param listener The listener to register.
     * @return A CompletableFuture that, when completed, will indicate the listener has been registered.
     */
    CompletableFuture<Void> registerListener(KeyUpdateListener<KeyT, ValueT> listener);

    /**
     * Unregisters a registered listener.
     *
     * @param listener THe listener to unregister.
     */
    void unregisterListener(KeyUpdateListener<KeyT, ValueT> listener);

    /**
     * Creates a new Iterator over all the Entries in the Table Segment.
     *
     * @param continuationToken A continuation token that can be used to resume a previously interrupted iteration. This
     *                          can be obtained by invoking {@link IteratorItem#getContinuationToken()}.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the Entries in this Table Segment.
     */
    CompletableFuture<AsyncIterator<IteratorItem<KeyT, ValueT>>> iterator(Object continuationToken);

    @Override
    void close();

    /**
     * An iteration result item returned by {@link AsyncIterator} when invoking {@link #iterator(Object)}.
     *
     * @param <KeyT>   Key Type.
     * @param <ValueT> Value Type.
     */
    @Data
    class IteratorItem<KeyT, ValueT> {
        /**
         * Gets an object that can be used to reinvoke {@link TableSegment#iterator(Object)} if a previous
         * iteration has been interrupted (by losing the pointer to the {@link AsyncIterator}), system restart, etc.
         * TODO: this will be properly defined when the implementation is ready, but it will be a serializable object (String, byte[], etc.).
         */
        private final Object continuationToken;
        /**
         * A List of {@link VersionedEntry} instances that are contained in this instance. For efficiency reasons, entries
         * may be batched together. The entries in this list are not necessarily related to each other, nor are they
         * guaranteed to be in any particular order.
         */
        private final List<VersionedEntry<KeyT, ValueT>> entries;
    }
}
