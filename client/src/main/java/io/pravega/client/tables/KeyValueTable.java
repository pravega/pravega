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
import java.util.concurrent.CompletableFuture;
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
 * {@link TableKey}s are made up of two components: Primary Key and Secondary Key.
 * <ul>
 * <li> The Primary Key is mandatory and is used to group all {@link TableKey}s with the same Primary Key together in the
 * same Table Partition; this allows multiple keys/entries sharing the same Primary Key to be updated/removed atomically.
 * The Primary Keys must all have the same length throughout the Key-Value Table. This needs to be configured when the
 * Key-Value Table is created (See {@link KeyValueTableConfiguration#getPrimaryKeyLength()} and must be greater than 0.
 *
 * <li> The Secondary Key is optional. The Secondary Keys must all have the same length throughout the Key-Value Table.
 * This needs to be configured when the Key-Value Table is created (See {@link KeyValueTableConfiguration#getSecondaryKeyLength()}
 * and must be at least 0. For Key-Value Tables with no Secondary Keys, set this value to 0. It is important to note that
 * if specifying a non-zero value, then all {@link TableKey}s must have both the Primary Key and Secondary Key set; so
 * we may not have {@link TableKey}s without Secondary Keys in this case. The Secondary Key is optional at the Table
 * level, not at the Key level.
 *
 * <li> Two {@link TableKey} instances are considered equal if they have the same Primary Key and same Secondary Key.
 * The same Secondary Key may be "associated" with different Primary Keys, in which case the resulting {@link TableKey}s
 * will be distinct. For example, if we consider Primary Keys PK1 != PK2 and Secondary Key SK1, then key PK1.SK1 is
 * different from key PK2.SK1.
 *
 * <li> Multiple {@link TableKey}/{@link TableEntry} instances with the same Primary Key can be updated or removed
 * atomically (either all changes will be applied or none will).
 *
 * <li> {@link TableKey}s that do not share the same Primary Key may be hashed to different Key-Value Table Partitions
 * and cannot be used for multi-key/entry atomic updates or removals.
 *
 * <li> {@link TableKey}s sharing the same Primary Key are grouped into the same Table Segment; as such, the choice of
 * Primary Keys can have performance implications. An ideally balanced Key-Value Table is one where there the load on each
 * Table Segment is approximately the same; this can be achieved by not having any Secondary Keys at all or where each
 * Primary Key has approximately the same number of Secondary Keys. To enable a uniform distribution of Keys over the
 * Key-Value Table, consider not using Secondary Keys at all. If this situation cannot be avoided (i.e., multi-entry
 * atomic updates are required), then it is recommended to use a sufficiently large number of Primary Keys compared to
 * the number of Partitions configured on the Key-Value Table (the higher the ratio of Primary Keys to Partitions, the
 * better). Such an approach will ensure that the Key-Value Table load will be spread across all its Table Segments.
 * An undesirable situation is an extreme case where all the Keys in the Key-Value Table are associated with a single
 * Primary Key; in this case the entire Key-Value Table load will be placed on a single backing Table Segment instead of
 * spreading it across many Table Segments, limiting the performance of the whole Key-Value Table to that of a single
 * Table Segment.
 * </ul>
 * <p>
 * Types of Updates:
 * <ul>
 * <li> Unconditional Updates will update a value associated with a {@link TableKey}, regardless of whether that
 * {@link TableKey} previously existed or not, and regardless of what that {@link TableKey}'s {@link Version} is. Note
 * that Unconditional Updates will either update an existing value or insert a new {@link TableEntry} into the table
 * if the specified {@link TableKey} does not already exist.
 *
 * <li> Conditional Updates will only overwrite an existing value if the specified {@link Version} matches the one that
 * is currently present on the server. Conditional updates can performed using {@link #put} or {@link #putAll} by passing
 * {@link TableEntry} instances that were either created using {@link TableEntry#versioned} or were retrieved from a
 * {@link #get}, {@link #getAll} or {@link #entryIterator} call. Conditional inserts can be performed using the same
 * methods by passing a {@link TableEntry} that was created using {@link TableEntry#absent} and will only succeed if
 * the associated {@link TableKey} does not already exist.
 *
 * <li> Unconditional Removals will remove a {@link TableKey} if it exists. Such removals can be performed using
 * {@link #remove} or {@link #removeAll} by passing a {@link TableKey} created using {@link TableKey#anyVersion}. The
 * operation will also succeed (albeit with no effect) if the {@link TableKey} does not exist.
 *
 * <li> Conditional Removals will remove a {@link TableKey} only if the specified {@link Version} matches the one that is
 * currently present on the server. Such removals can be performed using {@link #remove} or {@link #removeAll} by passing
 * a {@link TableKey} created using {@link TableKey#versioned} or retrieved from a {@link #get}, {@link #getAll},
 * {@link #keyIterator} or {@link #entryIterator} call.
 *
 * <li> Multi-key updates allow any number of conditions to be specified (including no conditions) in the same atomic
 * batch. All the conditions that are present in the update batch must be satisfied in order for the update batch to be
 * accepted - the condition checks and updates are performed atomically. Some entries may be conditioned on their
 * {@link TableKey}s not existing at all ({@link TableEntry#getKey()}{@link TableKey#getVersion()} equals
 * {@link Version#NOT_EXISTS}), some may be conditioned on specific versions and some may not have condition attached at
 * all ({@link TableEntry#getKey()}{@link TableKey#getVersion()} equals {@link Version#NO_VERSION}). Use
 * {@link #putAll(Iterable)} for such an update.
 *
 * <li> Multi-key removals allow any number of conditions to be specified (including no conditions) in the same atomic
 * batch. All the conditions that are present in the removal batch must be satisfied in order for the removal batch to
 * be accepted - the condition checks and updates are performed atomically. Some removals may be conditioned on their
 * affected {@link TableKey} having a specific version and some may not have a condition attached at all
 * ({@link TableKey#getVersion()} equals {@link Version#NO_VERSION}). Although unusual, it is possible to have a removal
 * conditioned on a {@link TableKey} not existing ({@link TableKey#getVersion()} equals {@link Version#NOT_EXISTS});
 * such an update will have no effect on that {@link TableKey} if it doesn't exist but it will prevent the rest of the
 * removals from the same batch from being applied - this can be used in scenarios where a set of {@link TableKey} must
 * be removed only if a particular {@link TableKey} is not present. Use {@link #removeAll(Iterable)} for such a removal.
 *
 * <li> It is not possible to mix updates (including inserts) with removals in the same atomic operation.
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

 */
@Beta
public interface KeyValueTable extends AutoCloseable {
    /**
     * The maximum length of a Table Segment Value.
     */
    int MAXIMUM_VALUE_LENGTH = TableSegment.MAXIMUM_VALUE_LENGTH;

    /**
     * Updates the value associated with a {@link TableKey} in the {@link KeyValueTable}. If the {@link TableKey} is not
     * already present, the {@link TableEntry} will be inserted into the Table, subject to the rules below.
     *
     * If {@link TableEntry#isVersioned()} returns true, this will be a Conditional Update.
     * <ul>
     * <li> If {@code entry} was created using {@link TableEntry#versioned} or retrieved via {@link #getAll}, {@link #getAll}
     * or {@link #entryIterator}, the {@link TableEntry} will only be <strong>updated</strong> if there exists a
     * {@link TableKey} in the {@link KeyValueTable} matching {@code entry}'s {@link TableEntry#getKey()} and both key
     * versions match. This update will be rejected if the Key Versions do not match or if there is no matching
     * {@link TableKey} in the Table.
     *
     * <li> If {@code entry} was created using {@link TableKey#absent}, the {@link TableEntry} will only be
     * <strong>inserted</strong> if there doesn't exist a {@link TableKey} in the {@link KeyValueTable} that matches the
     * {@code entry}'s {@link TableEntry#getKey()}. This is a conditional insert.
     * </ul>
     * <p>
     * All other invocations will be executed as Unconditional Updates (the entry was created using
     * {@link TableEntry#anyVersion}).
     *
     * @param entry The {@link TableEntry} to update.
     * @return A CompletableFuture that, when completed, will contain the {@link Version} associated with the newly
     * updated entry.
     */
    CompletableFuture<Version> put(@NonNull TableEntry entry);

    /**
     * Updates the values associated with multiple {@link TableKey}s that have the same {@link TableKey#getPrimaryKey()}
     * into this {@link KeyValueTable}. All changes are performed atomically (either all or none will be accepted).
     *
     * See {@link #put(TableEntry)} for details on Conditional/Unconditional Updates. Conditional and unconditional
     * updates may be mixed together in this call. Each individual {@link TableEntry} will be individually assessed
     * (from a conditional point of view) and will prevent the committal of the entire batch of updates if it fails to
     * meet the condition.
     *
     * @param entries An {@link Iterable} of {@link TableEntry} instances to update.
     * @return A CompletableFuture that, when completed, will contain a List of {@link Version} instances which
     * represent the versions for the updated keys. The size of this list will be the same as the number of items in
     * {@code entries} and the versions will be in the same order as the {@code entries}.
     */
    CompletableFuture<List<Version>> putAll(@NonNull Iterable<TableEntry> entries);

    /**
     * Removes a {@link TableKey} from the {@link KeyValueTable}.
     * <p>
     * If {@link TableKey#isVersioned()} returns true, this will be a Conditional Removal. The {@link TableKey} will only
     * be removed if there exists a {@link TableKey} in the {@link KeyValueTable} matching the given {@code key} and {@link Version}.
     * <p>
     * Otherwise, it will be executed as an Unconditional Removal ({@link TableKey#getVersion()} returns
     * {@link Version#NO_VERSION} - such as when the key was created using {@link TableKey#anyVersion}).
     * <p>
     * NOTE: it is possible to invoke this method with a {@link TableKey} created using {@link TableKey#absent}. This
     * operation will have no effect if the key does not exist, but it will fail if the key exists. While there doesn't
     * exist a practical use case for this call, it could be used to detect if a Key exists (call will fail) or not (no error).
     *
     * @param key The Key to remove.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed.
     */
    CompletableFuture<Void> remove(@NonNull TableKey key);

    /**
     * Removes one or more {@link TableKey} instances that have the same {@link TableKey#getPrimaryKey()} from this
     * {@link KeyValueTable}. All removals are performed atomically (either all keys or no key will be removed).
     * <p>
     * See {@link #remove(TableKey)} for details on Conditional/Unconditional Removals. Conditional and Unconditional
     * removals may be mixed together in this call. Each individual {@link TableKey} will be individually assessed (from
     * a conditional point of view) and will prevent the committal of the entire batch of removals if it fails.
     *
     * @param keys An {@link Iterable} of keys to remove. If for at least one such key, {@link TableKey#getVersion()}
     *             indicates a conditional update, this will perform an atomic Conditional Remove conditioned on the
     *             server-side versions matching the provided ones (for all {@link TableKey} instances that have one);
     *             otherwise an Unconditional Remove will be performed.
     *             See {@link KeyValueTable} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * See the {@link KeyValueTable} doc for more details on Conditional Update Responses.
     * </ul>
     */
    CompletableFuture<Void> removeAll(@NonNull Iterable<TableKey> keys);

    /**
     * Determines if the given {@link TableKey} exists or not.
     *
     * @param key The {@link TableKey} to check. Only the {@link TableKey#getPrimaryKey()} and {@link TableKey#getSecondaryKey()}
     *            are considered here; {@link TableKey#getVersion()} is ignored (i.e., this only verifies if the key
     *            exists, not that the key must be of a specific {@link Version}).
     * @return A CompletableFuture that, when completed, will contain a {@link Boolean} representing the result.
     */
    CompletableFuture<Boolean> exists(@NonNull TableKey key);

    /**
     * Gets the latest value for a {@link TableKey}.
     *
     * @param key The {@link TableKey} to get the value for. Only the {@link TableKey#getPrimaryKey()} and
     *            {@link TableKey#getSecondaryKey()} are considered here; {@link TableKey#getVersion()} is ignored
     *            (i.e., this retrieves the latest value for a key).
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such {@link TableKey}
     * exists, this will be completed with a null value.
     */
    CompletableFuture<TableEntry> get(@NonNull TableKey key);

    /**
     * Gets the latest values for a set of {@link TableKey}s.
     *
     * @param keys An {@link Iterable} of @link TableKey} to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableEntry} instances for the
     * requested keys. The size of the list will be the same as {@code keys.size()} and the results will be in the same
     * order as the requested keys. Any keys which do not have a value will have a null entry at their index.
     */
    CompletableFuture<List<TableEntry>> getAll(@NonNull Iterable<TableKey> keys);

    /**
     * Creates a new Iterator for {@link TableKey}s in this {@link KeyValueTable}. This is preferred to {@link #entryIterator}
     * if all that is needed is the {@link TableKey}s as less I/O is involved both server-side and between the server
     * and client.
     *
     * @param maxKeysAtOnce The maximum number of {@link TableKey}s to return with each call to {@link AsyncIterator#getNext()}.
     * @param args          (Optional) An {@link IteratorArgs} instance that represents the args to pass to the iterator.
     * @return An {@link AsyncIterator} that can be used to iterate over Keys in this {@link KeyValueTable}.
     */
    AsyncIterator<IteratorItem<TableKey>> keyIterator(int maxKeysAtOnce, @NonNull IteratorArgs args);

    /**
     * Creates a new Iterator over {@link TableEntry} instances in this {@link KeyValueTable}. This should be used if
     * both the Keys and their associated Values are needed and is preferred to using {@link #keyIterator} to get the Keys
     * and then issuing {@link #get}/{@link #getAll} to retrieve the Values.
     *
     * @param maxEntriesAtOnce The maximum number of {@link TableEntry} instances to return with each call to
     *                         {@link AsyncIterator#getNext()}.
     * @param args          (Optional) An {@link IteratorArgs} instance that represents the args to pass to the iterator.
     * @return An {@link AsyncIterator} that can be used to iterate over Entries in this {@link KeyValueTable}.
     */
    AsyncIterator<IteratorItem<TableEntry>> entryIterator(int maxEntriesAtOnce, @NonNull IteratorArgs args);

    /**
     * Closes the {@link KeyValueTable}. No more updates, removals, retrievals or iterators may be performed using it.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
