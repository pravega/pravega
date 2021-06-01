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
 * atomically (either all changes will be applied or none will). However, it is important to note that it is not possible
 * to mix updates  with removals in the same atomic operation.
 *
 * <li> {@link TableKey}s that do not share the same Primary Key may be hashed to different Key-Value Table Partitions
 * and cannot be used for multi-key/entry atomic updates or removals.
 *
 * <li> {@link TableKey}s sharing the same Primary Key are grouped into the same Table Segment; as such, the choice of
 * Table Key can have performance implications. As Primary Keys are hashed to Table Segments, a large number of Primary
 * Keys (larger than the number of Table Segments) and a uniform distribution of operations across keys leads to an even
 * distribution of load across Table Segments. Many realistic workloads are skewed, however, and can lead to an uneven
 * load distribution across Table Segments.
 *
 * The use of Secondary Keys also has implications to the load balance. All keys containing a given Primary Key map to
 * the same Table Segment independent of Secondary Key. In the presence of Secondary Keys, the total load for a given
 * Primary Key depends on the load aggregate across all of its Secondary Keys. When the load distribution is highly
 * skewed, one option to consider is eliminating Secondary Keys and only using Primary Keys. The application must be
 * able to encode the key into a single one rather than split into two parts. To illustrate, if we have two Table Keys
 * "A.B" and "A.C", "." representing the separation between Primary and Secondary Keys, then they map to the same Table
 * Segment because they have the same Primary Key "A". If instead, we use "AB" and "AC" as keys, eliminating the Secondary
 * Keys but retaining the necessary information in the Primary Key, then we should be able to map those keys to different
 * Table Segments depending on the hashing.
 * </ul>
 * <p>
 * Types of Updates:
 * <ul>
 * <li> Unconditional Updates will update a value associated with a {@link TableKey}, regardless of whether that
 * {@link TableKey} previously existed or not, or what what its {@link Version} is. Note that Unconditional Updates will
 * either update an existing value or insert a new {@link TableEntry} into the table if the specified {@link TableKey}
 * does not already exist. Unconditional Updates can be performed using {@link #update} by passing {@link Put} instances
 * with a null ({@link Put#getVersion()}.
 *
 * <li> Conditional Updates will only overwrite an existing value if the specified {@link Put#getVersion()} matches the
 * one that is currently present on the server. Conditional updates can performed using {@link #update} by passing
 * {@link Put} instances with a non-null {@link Put#getVersion()}. Conditional inserts can be performed using the same
 * methods by passing an {@link Insert} instance and will only succeed if the associated {@link TableKey} does not already
 * exist.
 *
 * <li> Unconditional Removals will remove a {@link TableKey} if it exists. Such removals can be performed using
 * {@link #update} by passing a {@link Remove} having {@link Remove#getVersion()} be null. The operation will also succeed
 * (albeit with no effect) if the {@link Remove#getKey()} does not exist.
 *
 * <li> Conditional Removals will remove a {@link TableKey} only if the specified {@link Remove#getVersion()} matches the
 * one that is currently present on the server. Such removals can be performed using {@link #update} by passing a
 * {@link Remove} having {@link Remove#getVersion()} non-null.
 *
 * <li> Multi-key updates allow any number of conditions to be specified (including no conditions) in the same atomic
 * batch. All the conditions that are present in the update batch must be satisfied in order for the update batch to be
 * accepted - the condition checks and updates are performed atomically. {@link Insert}s may be mixed with {@link Put}
 * having {@link Put#getVersion()} either null or non-null. {@link Remove}s may not be mixed with anything else (i.e.,
 * {@link Insert} or {@link Remove}). Use {@link #update(Iterable)} for such an update.
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
     * Performs a specific {@link TableModification} to the {@link KeyValueTable}, as described below:
     * <ul>
     * <li> If {@code update} is a {@link Insert}, this will be interpreted as a Conditional Insert, so the
     * {@link Insert#getValue()} will only be inserted (and associated with {@link Insert#getKey()} if there doesn't
     * already exist a {@link TableKey} in the {@link KeyValueTable} that matches {@link Insert#getKey()}.
     *
     * <li> If {@code update} is a {@link Put}, this will be an update. If {@link Put#getVersion()} is null, this will
     * be interpreted as an Unconditional Update, and the {@link Put#getValue()} will be associated with {@link Put#getKey()}
     * in the {@link KeyValueTable} regardless of whether the Key existed before or not. If {@link Put#getVersion()} is
     * non-null, this will be interpreted as a Conditional Update, and the {@link Put#getValue()} will only be associated
     * with {@link Put#getKey()} if there exists a {@link TableKey} in the {@link KeyValueTable} whose {@link Version}
     * matches {@link Put#getVersion()}. NOTE: if {@link Put#getVersion()} equals {@link Version#NOT_EXISTS}, this is
     * equivalent to {@link Insert} (i.e., it will be a Conditional Insert).
     *
     * <li> If {@code update} is a {@link Remove}, this will remove the {@link Remove#getKey()} from the {@link KeyValueTable}.
     * If {@link Remove#getVersion()} is null, this will be interpreted as an Unconditional Removal, so the Key will be
     * removed regardless of its {@link Version} (and the operation will also be successful if the {@link Remove#getKey()}
     * does not exist). If {@link Remove#getVersion()} is non-null, this will be interpreted as a Conditional Removal,
     * and the Key will only be removed if it exists and its {@link Version} matches {@link Remove#getVersion()}.
     * </ul>
     *
     * @param update The {@link TableModification} to apply to the {@link KeyValueTable}.
     * @return A CompletableFuture that, when completed, will contain the {@link Version} associated with any newly
     * updated or inserted entry (for {@link Insert} or {@link Put}) or {@code null} for {@link Remove}.
     */
    CompletableFuture<Version> update(@NonNull TableModification update);

    /**
     * Performs a batch of {@link TableModification}s to the {@link KeyValueTable} for {@link TableKey}s that have the
     * same {@link TableKey#getPrimaryKey()}. All changes are performed atomically (either all or none will be accepted).
     * <p>
     * See {@link #update(TableModification)} for details on Conditional/Unconditional Updates. Conditional and unconditional
     * updates may be mixed together in this call. Each individual {@link TableModification} will be individually assessed
     * (from a conditional point of view) and will prevent the entire batch from committing if it fails to meet the condition.
     *
     * @param updates An {@link Iterable} of {@link TableModification}s to apply to the {@link KeyValueTable}. Note that
     *                {@link Remove} instances may not be mixed together with {@link Insert} or {@link Put} instances,
     *                but {@link Insert}s and {@link Put}s may be mixed together.
     * @return A CompletableFuture that, when completed, will contain a List of {@link Version} instances which
     * represent the versions for the updated or inserted keys. The size of this list will be the same as the number of
     * items in {@code updates} and the versions will be in the same order as the {@code updates}. This list will be
     * empty if {@code updates} contains only {@link Remove} instances.
     * @throws IllegalArgumentException If there exist two or more {@link TableModification}s in {@code updates} that
     *                                  refer to {@link TableKey}s with different {@link TableKey#getPrimaryKey()}.
     * @throws IllegalArgumentException If {@code updates} contains at least one {@link Remove} instance and at least one
     *                                  {@link TableModification} that is not {@link Remove}.
     */
    CompletableFuture<List<Version>> update(@NonNull Iterable<TableModification> updates);

    /**
     * Determines if the given {@link TableKey} exists or not.
     *
     * @param key The {@link TableKey} to check.
     * @return A CompletableFuture that, when completed, will contain a {@link Boolean} representing the result.
     */
    CompletableFuture<Boolean> exists(@NonNull TableKey key);

    /**
     * Gets the latest value for a {@link TableKey}.
     *
     * @param key The {@link TableKey} to get the value for.
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
     * Creates a new {@link KeyValueTableIterator.Builder} that can be used to construct and execute an Iterator over
     * the {@link TableKey}/{@link TableEntry} instances in this {@link KeyValueTable}.
     *
     * @return A new instance of a {@link KeyValueTableIterator.Builder}.
     */
    KeyValueTableIterator.Builder iterator();

    /**
     * Closes the {@link KeyValueTable}. No more updates, removals, retrievals or iterators may be performed using it.
     *
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
