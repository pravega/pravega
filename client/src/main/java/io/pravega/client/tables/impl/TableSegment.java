/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables.impl;

import io.netty.buffer.ByteBuf;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorState;
import io.pravega.client.tables.TableEntry;
import io.pravega.client.tables.TableKey;
import io.pravega.common.util.AsyncIterator;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Data;

/**
 * Defines all operations that are supported on a Table Segment.
 * <p>
 * Types of updates:
 * * Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether that Key
 * previously existed or not, and regardless of what that Key's version is.
 * * Conditional Updates will only overwrite an existing value if the specified version matches that Key's version. If
 * the key does not exist, the {@link TableKey} or {@link TableEntry} must have been created with
 * {@link TableSegmentKeyVersion#NOT_EXISTS} in order for the update to succeed.
 * * Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also succeed (albeit
 * with no effect) if the Key does not exist.
 * * Conditional Removals will remove a Key only if the specified {@link TableKey#getVersion()} matches that Key's version.
 * It will also fail (with no effect) if the Key does not exist and Version is not set to
 * {@link TableSegmentKeyVersion#NOT_EXISTS}.
 */
public interface TableSegment extends AutoCloseable {

    /**
     * Inserts or updates an existing Table Entry into this Table Segment.
     * @param entry The Entry to insert or update. If {@link TableEntry#getKey()}{@link TableKey#getVersion()} is null,
     *              this will perform an Unconditional Update, otherwise it will perform a Conditional Update
     *              based on the information provided. See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain the {@link TableSegmentKeyVersion} associated with
     * the newly inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<TableSegmentKeyVersion> put(TableSegmentEntry entry) {
        return put(Collections.singletonList(entry)).thenApply(result -> result.get(0));
    }

    /**
     * Inserts new or updates existing Table Entries into this Table Segment.
     *
     * @param entries A List of entries to insert or update. If for at least one such entry,
     *                {@link TableEntry#getKey()}{@link TableKey#getVersion()} returns a non-null value,
     *                this will perform an atomic Conditional Update where all the entries either get applied or
     *                none will; otherwise a Unconditional Update will be performed.
     *                See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableSegmentKeyVersion} instances
     * which represent the versions for the inserted/updated keys. The size of this list will be the same as entries.size()
     * and the versions will be in the same order as the entries. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * This exception will contain the Keys that failed the validation.
     * </ul>
     */
    CompletableFuture<List<TableSegmentKeyVersion>> put(List<TableSegmentEntry> entries);

    /**
     * Removes the given key from this Table Segment.
     *
     * @param key The Key to remove. If {@link TableKey#getVersion()} is null, this will perform an Unconditional Remove,
     *            otherwise it will perform a Conditional Remove based on the information provided.
     *            See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<Void> remove(TableSegmentKey key) {
        return remove(Collections.singleton(key));
    }

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
    CompletableFuture<Void> remove(Collection<TableSegmentKey> keys);

    /**
     * Gets the latest value for the given Key.
     *
     * @param key A {@link ByteBuf} representing the Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    default CompletableFuture<TableSegmentEntry> get(ByteBuf key) {
        return get(Collections.singletonList(key)).thenApply(result -> result.get(0));
    }

    /**
     * Gets the latest values for the given Keys.
     *
     * @param keys A List of {@link ByteBuf} instances representing the Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableSegmentEntry} instances for
     * the requested keys. The size of the list will be the same as keys.size() and the results will be in the same order
     * as the requested keys. Any keys which do not have a value will have a null entry at their index.
     */
    CompletableFuture<List<TableSegmentEntry>> get(List<ByteBuf> keys);

    /**
     * Creates a new Iterator over all the Keys in the Table Segment.
     *
     * @param maxKeysAtOnce The maximum number of entries to return with each call to {@link AsyncIterator#getNext()}.
     * @param state         An {@link IteratorState} that represents a continuation token that can be used to resume a
     *                      previously interrupted iteration. This can be obtained by invoking {@link IteratorItem#getState()}.
     *                      A null value will create an iterator that lists all keys.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Keys in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableSegmentKey>> keyIterator(int maxKeysAtOnce, IteratorState state);

    /**
     * Creates a new Iterator over all the Entries in the Table Segment.
     *
     * @param maxEntriesAtOnce The maximum number of entries to return with each call to {@link AsyncIterator#getNext()}.
     * @param state            An {@link IteratorState} that represents a continuation token that can be used to resume
     *                         a previously interrupted iteration. This can be obtained by invoking
     *                         {@link IteratorItem#getState()}. A null value will create an iterator that lists all Entries.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Entries in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableSegmentEntry>> entryIterator(int maxEntriesAtOnce, IteratorState state);

    @Override
    void close();

    /**
     * An iteration result item returned by {@link AsyncIterator} when invoking {@link #entryIterator(int, IteratorState)} or
     * {@link #keyIterator(int, IteratorState)}.
     *
     * @param <T> Iterator Item type.
     */
    @Data
    class IteratorItem<T> {
        /**
         * Gets an {@link IteratorState} that can be used to reinvoke {@link TableSegment#entryIterator(int, IteratorState)} or
         * {@link TableSegment#keyIterator(int, IteratorState)}if a previous iteration has been interrupted (by losing the
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
