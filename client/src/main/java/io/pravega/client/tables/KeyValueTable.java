/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.tables;

import com.google.common.annotations.Beta;
import io.pravega.client.tables.impl.TableSegment;
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
 * <li> Keys belonging to the same Key Family are grouped into the same Table Segment; as such, the choice of Key Families
 * can have performance implications. An ideally balanced Key-Value Table is one where no Key is part of any Key Family
 * or the number of Keys in each Key Family is approximately the same. To enable a uniform distribution of Keys over the
 * Key-Value Table, it is highly recommended not to use Key Families at all. If this situation cannot be avoided (i.e.,
 * multi-entry atomic updates or iterators are required), then it is recommended that Key Families themselves be
 * diversified and Keys be equally distributed across them. Such approaches will ensure that the Key-Value Table load
 * will be spread across all its Table Segments. An undesirable situation is an extreme case where all the Keys in the
 * Key-Value Table are associated with a single Key Family; in this case the entire Key-Value Table load will be placed
 * on a single backing Table Segment instead of spreading it across many Table Segments, leading to eventual performance
 * degradation.
 * </ul>
 * <p>
 * Types of Updates:
 * <ul>
 * <li> Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether
 * that Key previously existed or not, and regardless of what that Key's version is. Such updates can be performed using
 * {@link #put(String, Object, Object)} or {@link #putAll(String, Iterable)}.
 * <li> Conditional Updates will only overwrite an existing value if the specified {@link Version} matches the one that
 * is currently present on the server. Conditional inserts can be performed using {@link #putIfAbsent(String, Object, Object)}
 * and will only succeed if the given Key does not already exist in the given Key Family. Conditional updates can be
 * performed using {@link #replace(String, Object, Object, Version)}.
 * <li> Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also
 * succeed (albeit with no effect) if the Key does not exist. Such removals can be performed using
 * {@link #remove(String, Object)}.
 * <li> Conditional Removals will remove a Key only if the specified {@link Version} matches the one that is currently
 * present on the server. Such removals can be performed using {@link #remove(String, Object, Version)}.
 * <li> Multi-key updates allow mixing different types of updates in the same update batch. Some entries may be conditioned
 * on their Keys not existing at all ({@link TableEntry#getKey()}{@link TableKey#getVersion()} equals
 * {@link Version#NOT_EXISTS}), some may be conditioned on specific versions and some may not have condition attached at
 * all ({@link TableEntry#getKey()}{@link TableKey#getVersion()} equals {@link Version#NO_VERSION}). All the conditions
 * that are present in the update batch must be satisfied in order for the update batch to be accepted - the condition
 * checks and updates are performed atomically. Use {@link #replaceAll(String, Iterable)} for such an update.
 * <li> Multi-key removals allow mixing different types of removals in the same removal batch. Some removals may be
 * conditioned on their affected Keys having a specific version and some may not have a condition attached at all
 * ({@link TableKey#getVersion()} equals {@link Version#NO_VERSION}). Although unusual, it is possible to have a removal
 * conditioned on a Key not existing ({@link TableKey#getVersion()} equals {@link Version#NOT_EXISTS}); such an update
 * will have no effect on that Key if it doesn't exist but it will  prevent the rest of the removals from the same batch
 * from being applied - this can be used in scenarios where a set of Keys must be removed only if a particular Key is not
 * present. All the conditions that are present in the removal batch must be satisfied in order for the removal batch to
 * be accepted - the condition checks and updates are performed atomically. Use {@link #removeAll(String, Iterable)} for
 * such a removal.
 * </ul>
 * <p>
 * Conditional Update Responses:
 * <ul>
 * <li> Success: the update or removal has been atomically validated and performed; all updates or removals in the
 * request have been accepted.
 * <li> Failure: the update or removal has been rejected due to version mismatch; no update or removal has been performed.
 * <li> {@link NoSuchKeyException}: the update or removal has been conditioned on a specific version (different from
 * {@link Version#NOT_EXISTS} or {@link Version#NO_VERSION}) but the Key does not exist in the {@link KeyValueTable}.
 * <li> {@link BadKeyVersionException}: the update or removal has been conditioned on a specific version (different from
 * {@link Version#NO_VERSION} but the Key exists in the {@link KeyValueTable} with a different version.
 * </ul>
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
@Beta
public interface KeyValueTable<KeyT, ValueT> extends AutoCloseable {
    /**
     * The maximum serialization length of a Table Segment Key.
     */
    int MAXIMUM_SERIALIZED_KEY_LENGTH = TableSegment.MAXIMUM_KEY_LENGTH / 2;

    /**
     * The maximum serialized length of a Table Segment Value.
     */
    int MAXIMUM_SERIALIZED_VALUE_LENGTH = TableSegment.MAXIMUM_VALUE_LENGTH;

    /**
     * Unconditionally inserts a new or updates an existing Entry in the {@link KeyValueTable}.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to insert or update.
     * @param value     The Value to be associated with the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link Version} associated with the newly
     * inserted or updated entry.
     */
    CompletableFuture<Version> put(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value);

    /**
     * Conditionally inserts a new Entry in the {@link KeyValueTable} if the given Key is not already present.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to insert.
     * @param value     The Value to be associated with the Key.
     * @return A CompletableFuture that, when completed, will contain the {@link Version} associated with the newly
     * inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If the Key is already present in the {@link KeyValueTable} for the
     * provided Key Family.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Version> putIfAbsent(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value);

    /**
     * Unconditionally inserts new or updates existing {@link TableEntry} instances that belong to the same Key Family
     * into this {@link KeyValueTable}. All changes are performed atomically (either all or none will be accepted).
     *
     * @param keyFamily The Key Family for the all provided Table Entries.
     * @param entries   An {@link Iterable} of {@link java.util.Map.Entry} instances to insert or update.
     * @return A CompletableFuture that, when completed, will contain a List of {@link Version} instances which
     * represent the versions for the inserted/updated keys. The size of this list will be the same as the number of
     * items in entries and the versions will be in the same order as the entries.
     */
    CompletableFuture<List<Version>> putAll(@NonNull String keyFamily, @NonNull Iterable<Map.Entry<KeyT, ValueT>> entries);

    /**
     * Conditionally updates an existing Entry in the {@link KeyValueTable} if the given Key exists and its
     * version matches the given {@link Version}.
     *
     * @param keyFamily (Optional) The Key Family for the Entry. If null, this Entry will not be associated with any
     *                  Key Family.
     * @param key       The Key to update.
     * @param value     The new Value to be associated with the Key.
     * @param version   A {@link Version} representing the version that this Key must have in order to replace it.
     * @return A CompletableFuture that, when completed, will contain the {@link Version} associated with the
     * updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If the Key is not present present in the {@link KeyValueTable} for the
     * provided Key Family or it is and has a different {@link Version}.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Version> replace(@Nullable String keyFamily, @NonNull KeyT key, @NonNull ValueT value,
                                       @NonNull Version version);

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
     * @return A CompletableFuture that, when completed, will contain a List of {@link Version} instances which
     * represent the versions for the inserted/updated keys. The size of this list will be the same as entries.size()
     * and the versions will be in the same order as the entries. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<List<Version>> replaceAll(@NonNull String keyFamily, @NonNull Iterable<TableEntry<KeyT, ValueT>> entries);

    /**
     * Unconditionally removes a Key from this Table Segment. If the Key does not exist, no action will be taken.
     *
     * @param keyFamily (Optional) The Key Family for the Key to remove.
     * @param key       The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key);

    /**
     * Conditionally Removes a Key from this Table Segment.
     *
     * @param keyFamily (Optional) The Key Family for the Key to remove.
     * @param key       The Key to remove.
     * @param version   A {@link Version} representing the version that this Key must have in order to remove it.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Void> remove(@Nullable String keyFamily, @NonNull KeyT key, @NonNull Version version);

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
    CompletableFuture<Void> removeAll(@Nullable String keyFamily, @NonNull Iterable<TableKey<KeyT>> keys);

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

    /**
     * Exposes this {@link KeyValueTable} instance as a {@link Map}. Please refer to the {@link KeyValueTableMap}
     * documentation for special cases and limitations.
     * <p>
     * This is useful for integrating code that expects to deal with a {@link Map}. Not all the {@link KeyValueTable}
     * functionality can be implemented using a {@link Map}, however the {@link Map} interface is fully implemented.
     *
     * @param keyFamily (Optional) The Key Family to create the {@link KeyValueTableMap} for. Any operations on the
     *                  returned {@link KeyValueTableMap} will only affect this Key Family. If no Key Family is
     *                  provided (null), then certain {@link KeyValueTableMap} APIs may not be supported. Refer to
     *                  {@link KeyValueTableMap} documentation for more details.
     * @return A new {@link KeyValueTableMap} instance bound to this {@link KeyValueTable} and Key Family.
     */
    KeyValueTableMap<KeyT, ValueT> getMapFor(@Nullable String keyFamily);
}
