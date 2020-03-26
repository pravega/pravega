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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.NonNull;

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
 * Key Families are used to group related Keys together in the same Table Partition, which allows multiple
 * keys/entries belonging to the same Key Family to be updated/removed atomically.
 * <ul>
 * <li> Multiple Keys/Entries in the same Key Family can be updated or removed atomically (either all changes will be
 * applied or none will).
 * <li> Iterating through all Keys/Entries in the same Key Family is possible.
 * <li> The same Key may exist in multiple Key Families or even not be associated with any Key Family at all. Such keys
 * are treated as distinct keys and will not interfere with each other (i.e., if key K1 exists in Key Families F1 and F2,
 * then F1.K1 is different from F2.K1 and both are different from K1 (no Key Family association).
 * <li> Keys that do not belong to any Key Family will be uniformly distributed across the Key-Value Table Partitions and
 * cannot be used for multi-key/entry atomic updates or removals or be iterated on.
 * <li> {@link TableKey}s belonging to the same Key Family are grouped into the same Table Segment; as such, the choice
 * of Key Families can have performance implications. An ideally balanced Key-Value Table is one where no {@link TableKey}
 * is part of any Key Family or the number of {@link TableKey}s in each Key Family is approximately the same. To enable
 * a uniform distribution of {@link TableKey}s over the Key-Value Table, it is highly recommended not to use Key Families
 * at all. If this situation cannot be avoided (i.e., multi-entry atomic updates or iterators are required), then it is
 * recommended that Key Families themselves be diversified and {@link TableKey}s be equally distributed across them. Such
 * approaches will ensure that the Key-Value Table load will be spread across all its Table Segments. An undesirable
 * situation is an extreme case where all the {@link TableKey}s in the Key-Value Table are associated with a single
 * Key Family; in this case the entire Key-Value Table load will be placed on a single backing Table Segment instead of
 * spreading it across many Table Segments, leading to eventual performance degradation.
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
 * <p>
 * Key, Key Family and Value serialization constraints:
 * <ul>
 * <li> For any given Key that does not belong to a Key Family, the serialization length for that Key must not exceed
 * 8190 bytes.
 * <li> For any given Key K that belongs to a Key Family KF, the UTF8 serialization for KF plus the serialization length
 * of K must not exceed 8190 bytes.
 * <li> Any {@link TableEntry} Value must not serialize to more than 1040384 bytes (1MB - 8KB).
 * </ul>
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface KeyValueTable<KeyT, ValueT> extends AutoCloseable {
    /**
     * Unconditionally inserts a new or updates an existing Entry in the {@link KeyValueTable}.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to insert or update.
     * @param value     The Value to be associated with the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<KeyVersion> put(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value);

    /**
     * Conditionally inserts a new Entry in the {@link KeyValueTable} if and only if the given Key is not already present.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to insert.
     * @param value     The Value to be associated with the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If the Key is already present in the {@link KeyValueTable} for the
     * provided Key Family.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<KeyVersion> putIfAbsent(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value);

    /**
     * Unconditionally inserts new or updates existing {@link TableEntry} instances that belong to the same Key Family
     * into this {@link KeyValueTable}. All changes are performed atomically (either all or none will be accepted).
     *
     * @param keyFamily The Key Family for the all provided {@link TableEntry} instances.
     * @param entries   An {@link Iterable} of {@link Map.Entry} instances to insert or update.
     * @return A CompletableFuture that, when completed, will contain a List of {@link KeyVersion} instances which
     * represent the versions for the inserted/updated keys. The size of this list will be the same as the number of
     * items in entries and the versions will be in the same order as the entries.
     */
    CompletableFuture<List<KeyVersion>> putAll(@NonNull String keyFamily, @NonNull Iterable<Map.Entry<KeyT, ValueT>> entries);

    /**
     * Conditionally updates an existing Entry in the {@link KeyValueTable} if and only if the given Key exists and its
     * version matches the given {@link KeyVersion}.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to update.
     * @param value     The new Value to be associated with the Key.
     * @param version   A {@link KeyVersion} representing the version that this Key must have in order to replace it.
     * @return A CompletableFuture that, when completed, will contain the {@link KeyVersion} associated with the
     * updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If the Key is not present present in the {@link KeyValueTable} for the
     * provided Key Family or it is and has a different {@link KeyVersion}.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<KeyVersion> replace(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value,
                                          @NonNull KeyVersion version);

    /**
     * Inserts new or updates existing {@link TableEntry} instances that belong to the same Key Family into this
     * {@link KeyValueTable}. All changes are performed atomically (either all or none will be accepted).
     *
     * @param keyFamily The Key Family for the all provided {@link TableEntry} instances.
     * @param entries   An {@link Iterable} of {@link TableEntry} instances to insert or update. If for at least one
     *                  such entry, {@link TableEntry#getKey()}{@link TableKey#getVersion()} indicates a conditional
     *                  update, this will perform an atomic Conditional Update conditioned on the server-side versions
     *                  matching the provided ones (for all {@link TableEntry} instances that have one); otherwise an
     *                  Unconditional Update will be performed.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain a List of {@link KeyVersion} instances which
     * represent the versions for the inserted/updated keys. The size of this list will be the same as entries.size()
     * and the versions will be in the same order as the entries. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<List<KeyVersion>> replaceAll(@NonNull String keyFamily, @NonNull Iterable<TableEntry<KeyT, ValueT>> entries);

    /**
     * Unconditionally removes a {@link TableKey} from this Table Segment. If the Key does not exist, no action will be
     * taken.
     *
     * @param keyFamily (Optional) The Key Family for the Key to remove.
     * @param key       The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key);

    /**
     * Conditionally Removes a Key from this Table Segment.
     *
     * @param keyFamily (Optional) The Key Family for the {@link TableKey} to remove.
     * @param key       The Key to remove.
     * @param version   A {@link KeyVersion} representing the version that this Key must have in order to remove it.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key, @NonNull KeyVersion version);

    /**
     * Removes one or more {@link TableKey} instances that belong to the same Key Family from this Table Segment.
     * All removals are performed atomically (either all keys or no key will be removed).
     *
     * @param keyFamily The Key Family for the {@link TableKey}.
     * @param keys      An {@link Iterable} of keys to remove. If for at least one such key, {@link TableKey#getVersion()}
     *                  indicates a conditional update, this will perform an atomic Conditional Remove conditioned on the
     *                  server-side versions matching the provided ones (for all {@link TableKey} instances that have one);
     *                  otherwise an Unconditional Remove will be performed.
     *                  See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Void> removeAll(@NonNull String keyFamily, @NonNull Iterable<TableKey<KeyT>> keys);

    /**
     * Gets the latest value for the a Key that belong to a specific Key Family.
     *
     * @param keyFamily (Optional) The Key Family for the Key to get.
     * @param key       The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    CompletableFuture<TableEntry<KeyT, ValueT>> get(@Nullable String keyFamily, @NonNull KeyT key);

    /**
     * Gets the latest values for a set of Keys that do belong to the same Key Family.
     *
     * @param keyFamily (Optional) The Key Family for all requested Keys.
     * @param keys      An {@link Iterable} of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableEntry} instances for the
     * requested keys. The size of the list will be the same as keys.size() and the results will be in the same order
     * as the requested keys. Any keys which do not have a value will have a null entry at their index.
     */
    CompletableFuture<List<TableEntry<KeyT, ValueT>>> getAll(@Nullable String keyFamily, @NonNull Iterable<KeyT> keys);

    /**
     * Creates a new Iterator over all the {@link TableKey}s in this {@link KeyValueTable} that belong to a specific
     * Key Family.
     *
     * @param keyFamily     The Key Family for which to iterate over keys.
     * @param maxKeysAtOnce The maximum number of {@link TableKey}s to return with each call to
     *                      {@link AsyncIterator#getNext()}.
     * @param state         (Optional) An {@link IteratorState} that represents a continuation token that can be used to
     *                      resume a previously interrupted iteration. This can be obtained by invoking
     *                      {@link IteratorItem#getState()}. A null value will create an iterator that lists all keys.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Keys in this {@link KeyValueTable} that
     * belong to a specific Key Family.
     */
    AsyncIterator<IteratorItem<TableKey<KeyT>>> keyIterator(@NonNull String keyFamily, int maxKeysAtOnce,
                                                            @Nullable IteratorState state);

    /**
     * Creates a new Iterator over all the {@link TableEntry} instances in this {@link KeyValueTable} that belong to a
     * specific Key Family.
     *
     * @param keyFamily        The Key Family for which to iterate over entries.
     * @param maxEntriesAtOnce The maximum number of {@link TableEntry} instances to return with each call to
     *                         {@link AsyncIterator#getNext()}.
     * @param state            (Optional) An {@link IteratorState} that represents a continuation token that can be used
     *                         to resume a previously interrupted iteration. This can be obtained by invoking
     *                         {@link IteratorItem#getState()}. A null value will create an iterator that lists all entries.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Entries in this {@link KeyValueTable}
     * that belong to a specific Key Family.
     */
    AsyncIterator<IteratorItem<TableEntry<KeyT, ValueT>>> entryIterator(@NonNull String keyFamily, int maxEntriesAtOnce,
                                                                        @Nullable IteratorState state);

    /**
     * Closes the {@link KeyValueTable}. No more updates, removals, retrievals or iterators may be performed using it.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
