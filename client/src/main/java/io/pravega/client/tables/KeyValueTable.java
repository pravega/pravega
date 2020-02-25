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

/**
 * Defines all operations that are supported on a Key-Value Table.
 * <p>
 * A Key-Value Table is a distributed Key-Value Store that indexes Entries by Keys. It uses Table Segments (non-distributed
 * Key-Value Store backed by a single Pravega Segment) as the fundamental storage primitive and provides a unified view
 * of all Table Segments involved. Each {@link TableKey} is hashed to a Table Partition which may be represented by one
 * or more Table Segments (depending on the Key-Value Table configuration chosen when it was created). Such partitioning
 * enables the Key-Value Table to be distributed across the Pravega cluster but also introduces some constraints for
 * certain operations (such as multi-key/entry atomic updates). See below for details.
 * <p>
 * >Key Families are used to group related Keys together in the same Table Partition, which allows multiple
 * keys/entries belonging to the same Key Family to be updated/removed atomically.
 * <ul>
 * <li> Multiple Keys/Entries in the same Key Family can be updated or removed atomically (either all at once or none).
 * <li> Iterating through all Keys/Entries in the same Key Family is possible.
 * <li> The same Key may exist in multiple Key Families or even not be associated with any Key Family at all. Such keys
 * are treated as distinct keys and will not interfere with each other (i.e., if key K1 exists in Key Families F1 and F2,
 * then F1.K1 is different from F2.K1 and both are different from K1 (no Key Family association).
 * <li> Keys that do not belong to any Key Family will be uniformly distributed across the Key-Value Table Partitions and
 * cannot be used for multi-key/entry atomic updates or removals or be iterated on.
 * <li> Improper use of Key Families may result in degraded performance. If a disproportionate number of Keys/Entries
 * are placed in the same Key Family (compared to the number of Key/Entries in other Key Families), it may not be
 * possible to uniformly distribute the Key-Value Table Entries across the cluster and more load will be placed on a
 * single backing Table Segment instead of spreading such load across many Table Segments. An ideally balanced Key-Value
 * Table will be one where Keys are not part of any Key Families or the number of Keys in each Key Family is approximately
 * the same. An improperly designed Key-Value Table will have all Keys part of a single Key Family which will cause a
 * single Table Segment to bear the full storage and processing load of the entire Key-Value Table.
 * </ul>
 * <p>
 * Types of Updates:
 * <ul>
 * <li> Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether
 * that Key previously existed or not, and regardless of what that Key's version is.
 * <li> Conditional Updates will only overwrite an existing value if the specified version matches that Key's version.
 * If the key does not exist, the {@link TableKey} or {@link TableEntry} must have been created with
 * {@link KeyVersion#NOT_EXISTS} in order for the update to succeed.
 * <li> Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also
 * succeed (albeit with no effect) if the Key does not exist.
 * <li> Conditional Removals will remove a Key only if the specified {@link TableKey#getVersion()} matches that Key's
 * version. It will also fail (with no effect) if the Key does not exist and Version is not set to
 * {@link KeyVersion#NOT_EXISTS}.
 * </ul>
 * <p>
 * Conditional Update Responses:
 * <ul>
 * <li> Success: the update or removal has been atomically validated and performed; all updates or removals in the
 * request have been accepted.
 * <li> Failure: the update or removal has been rejected due to version mismatch; no update or removal has been performed.
 * <li> {@link NoSuchKeyException}: the update or removal has been conditioned on a specific version (different from
 * {@link KeyVersion#NOT_EXISTS} or {@link KeyVersion#NO_VERSION}) but the {@link TableKey} does not exist in the
 * {@link KeyValueTable}.
 * <li> {@link BadKeyVersionException}: the update or removal has been conditioned on a specific version (different from
 * {@link KeyVersion#NO_VERSION} but the {@link TableKey} exists in the {@link KeyValueTable} with a different version.
 * </ul>
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface KeyValueTable<KeyT, ValueT> {
    /**
     * Inserts a new or updates an existing {@link TableEntry} that does not belong to any Key Family into this
     * {@link KeyValueTable}.
     *
     * @param entry The Entry to insert or update. If {@link TableEntry#getKey()}{@link TableKey#getVersion()} indicates
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
     * @param entry     The Entry to insert or update. If {@link TableEntry#getKey()}{@link TableKey#getVersion()}
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
     * @param entries   A List of {@link TableEntry} instances to insert or update. If for at least one such entry,
     *                  {@link TableEntry#getKey()}{@link TableKey#getVersion()} indicates a conditional update,
     *                  this will perform an atomic Conditional Update conditioned on the server-side versions matching
     *                  the provided ones (for all {@link TableEntry} instances that have one); otherwise a Unconditional
     *                  Update will be performed.
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
     * @param key The {@link TableKey} to remove. If {@link TableKey#getVersion()} indicates a conditional update, this
     *            will perform an atomic removal conditioned on the server-side version matching the provided one.
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
     * @param key       The {@link TableKey} to remove. If {@link TableKey#getVersion()} indicates a conditional update,
     *                  this will perform an atomic removal conditioned on the server-side version matching the provided
     *                  one. See {@link KeyValueTable} doc for more details on Types of Updates.
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
     *                  indicates a conditional update, this will perform an atomic Conditional Remove conditioned on the
     *                  server-side versions matching the provided ones (for all {@link TableKey} instances that have one);
     *                  otherwise an Unconditional Remove will be performed.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<Void> remove(String keyFamily, Collection<TableKey<KeyT>> keys);

    /**
     * Gets the latest value for the a Key that does not belong to any Key Family.
     *
     * @param key The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    default CompletableFuture<TableEntry<KeyT, ValueT>> get(KeyT key) {
        return get(Collections.singletonList(key)).thenApply(result -> result.get(0));
    }

    /**
     * Gets the latest values for a set of Keys that do not belong to any Key Family.
     *
     * @param keys A List of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableEntry} instances for the
     * requested keys. The size of the list will be the same as keys.size() and the results will be in the same order
     * as the requested keys. Any keys which do not have a value will have a null entry at their index.
     */
    CompletableFuture<List<TableEntry<KeyT, ValueT>>> get(List<KeyT> keys);

    /**
     * Gets the latest value for the a Key that belong to a specific Key Family.
     *
     * @param keyFamily The Key Family for the Key.
     * @param key       The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    default CompletableFuture<TableEntry<KeyT, ValueT>> get(String keyFamily, KeyT key) {
        return get(keyFamily, Collections.singletonList(key)).thenApply(result -> result.get(0));
    }

    /**
     * Gets the latest values for a set of Keys that do belong to the same Key Family.
     *
     * @param keyFamily The Key Family for all requested Keys.
     * @param keys      A List of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableEntry} instances for the
     * requested keys. The size of the list will be the same as keys.size() and the results will be in the same order
     * as the requested keys. Any keys which do not have a value will have a null entry at their index.
     */
    CompletableFuture<List<TableEntry<KeyT, ValueT>>> get(String keyFamily, List<KeyT> keys);

    /**
     * Creates a new Iterator over all the {@link TableKey}s in this {@link KeyValueTable} that belong to a specific
     * Key Family.
     *
     * @param keyFamily     The Key Family for which to iterate over keys.
     * @param maxKeysAtOnce The maximum number of {@link TableKey}s to return with each call to
     *                      {@link AsyncIterator#getNext()}.
     * @param state         An {@link IteratorState} that represents a continuation token that can be used to resume a
     *                      previously interrupted iteration. This can be obtained by invoking {@link IteratorItem#getState()}.
     *                      A null value will create an iterator that lists all keys.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Keys in this {@link KeyValueTable} that
     * belong to a specific Key Family.
     */
    AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(String keyFamily, int maxKeysAtOnce, IteratorState state);

    /**
     * Creates a new Iterator over all the {@link TableEntry} instances in this {@link KeyValueTable} that belong to a
     * specific Key Family.
     *
     * @param keyFamily        The Key Family for which to iterate over entries.
     * @param maxEntriesAtOnce The maximum number of {@link TableEntry} instances to return with each call to
     *                         {@link AsyncIterator#getNext()}.
     * @param state            An {@link IteratorState} that represents a continuation token that can be used to resume a
     *                         previously interrupted iteration. This can be obtained by invoking {@link IteratorItem#getState()}.
     *                         A null value will create an iterator that lists all entries.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Entries in this {@link KeyValueTable}
     * that belong to a specific Key Family.
     */
    AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(String keyFamily, int maxEntriesAtOnce, IteratorState state);
}
