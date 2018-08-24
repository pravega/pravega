/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import com.google.common.annotations.Beta;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.BadSegmentTypeException;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that are supported on Table Segments.
 *
 * Conditional Updates vs Blind Updates
 * * The {@link #put(String, List, Duration)} and {@link #remove(String, Collection, Duration)} methods support conditional
 * updates.
 * * If at least one of the {@link TableEntry} instances in the put() call has {@link TableEntry#hasVersion()} true, or
 * if at least one of the {@link TableKey} instances in the remove() call has {@link TableKey#hasVersion()} true, then
 * the operation will perform a Conditional Update. If the "hasVersion()" method returns false for ALL the items in the
 * respective collections, then the operation will perform a Blind Update.
 * * Conditional Updates compare the provided {@link TableKey#getVersion()} or {@link TableEntry#getVersion()} against the
 * version of the Key in the specified Table Segment. If the versions match, the Update (insert, update, remove) will be
 * allowed, otherwise it will be rejected. For a batched update (a Collection of {@link TableKey} or {@link TableEntry}),
 * all the items in the collection must meet the version comparison criteria in order for the entire batch to be accepted
 * (all items in the Collection will either all be accepted or all be rejected). Any Conditional Update (batch or not)
 * will be atomically checked-and-applied.
 * * Blind Updates (insert, update, remove) will take effect regardless of what the current Key version exists in the
 * Table Segment.
 */
@Beta
public interface TableStore {
    /**
     * Gets a value indicating the maximum length of any Table Entry Key supported by this TableStore implementation.
     *
     * @return The maximum length of any key, in bytes.
     */
    int maximumKeyLength();

    /**
     * Gets a value indicating the maximum length of any Table Entry Value supported by this TableStore Implementation.
     *
     * @return The maximum length of any value, in bytes.
     */
    int maximumValueLength();

    /**
     * Creates a new Segment and marks it as a Table Segment.
     * This segment may not be used for Streaming purposes (i.e., it cannot be used with {@link StreamSegmentStore}).
     *
     * @param segmentName The name of the Table Segment to create.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentExistsException} If the Segment does exist (whether as a Table Segment or Stream Segment).
     * </ul>
     */
    CompletableFuture<Void> createSegment(String segmentName, Duration timeout);

    /**
     * Deletes an existing Table Segment.
     *
     * @param segmentName The name of the Table Segment to delete.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<Void> deleteSegment(String segmentName, Duration timeout);

    /**
     * Merges a Table Segment into another Table Segment.
     *
     * @param targetSegmentName The name of the Table Segment to merge into.
     * @param sourceSegmentName The name of the Table Segment to merge.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If either the Source or Target Table Segment do not exist.
     * <li>{@link BadSegmentTypeException} If sourceSegmentName or targetSegmentName refer to non-Table Segments.
     * </ul>
     */
    CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout);

    /**
     * Seals a Table Segment for modifications.
     *
     * @param segmentName The name of the Table Segment to seal.
     * @param timeout     Timeout for the operation
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<Void> seal(String segmentName, Duration timeout);

    /**
     * Inserts a new or updates an existing Table Entry into the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to insert/update the Table Entry.
     * @param entries     A List of {@link TableEntry} instances to insert or update. If {@link TableEntry#hasVersion()}
     *                    returns true for at least one of the items in the list, then this will perform an atomic Conditional
     *                    Update. If {@link TableEntry#hasVersion()} returns false for ALL items in the list, then this
     *                    will perform a Blind update.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the current version of the each TableEntry
     * Key provided. The versions will be in the same order as the TableEntry instances provided. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If {@link TableEntry#getKey()} exceeds {@link #maximumKeyLength()}.
     * <li>{@link TableValueTooLongException} If {@link TableEntry#getValue()} exceeds {@link #maximumValueLength()}.
     * <li>{@link ConditionalTableUpdateException} If {@link TableEntry#hasVersion()} is true and {@link TableEntry#getVersion()}
     * does not match the Table Entry's Key current Table Version.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout);

    /**
     * Removes a Table Key from the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to remove the Key from.
     * @param keys        A Collection of {@link TableKey} instances to remove. If {@link TableKey#hasVersion()} returns
     *                    true for at least one of the TableKeys in this collection, then this will perform an atomic
     *                    Conditional Update (Removal). If {@link TableKey#hasVersion()} returns false for ALL items in
     *                    the collection, then this will perform a Blind Update (Removal).
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If {@link TableKey#getKey()} exceeds {@link #maximumKeyLength()}.
     * <li>{@link ConditionalTableUpdateException} If {@link TableKey#hasVersion()} is true and {@link TableKey#getVersion()}
     * does not match the Table Entry's Key current Table Version.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout);

    /**
     * Looks up a List of Keys in the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to look up into.
     * @param keys        A List of {@link ArrayView} instances representing the Keys to look up.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought result. This will be organized as a List
     * of {@link TableEntry} instances in the same order as their Keys in the provided input list. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If any of the items in "keys" exceeds {@link #maximumKeyLength()}.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<List<TableEntry>> get(String segmentName, List<ArrayView> keys, Duration timeout);

    /**
     * Creates a new Iterator over all the {@link TableEntry} in the given Table Segment.
     *
     * @param segmentName       The name of the Table Segment to iterate over.
     * @param continuationToken A continuation token that can be used to resume a previously interrupted iteration. This
     *                          can be obtained by invoking {@link IteratorItem#getContinuationToken()}.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableEntry} instances in the given Table Segment. If the operation failed, the Future will be
     * failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<AsyncIterator<IteratorItem>> iterator(String segmentName, Object continuationToken, Duration timeout);

    /**
     * Registers an existing {@link UpdateListener} for particular Key on a Table Segment.
     *
     * @param segmentName The name of the Table Segment to register the {@link UpdateListener} for.
     * @param key         An {@link ArrayView} representing the Key to register the {@link UpdateListener} for.
     * @param listener    An {@link UpdateListener} instance to register.
     * @return A CompletableFuture that, when completed, will indicate the operation completed. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If any of the items in "keys" exceeds {@link #maximumKeyLength()}.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<Void> registerListener(String segmentName, ArrayView key, UpdateListener listener);

    /**
     * Unregisters an {@link UpdateListener} for a particular Key on a Table Segment.
     *
     * @param segmentName The name of the Table Segment to unregister the {@link UpdateListener} from.
     * @param key         An {@link ArrayView} representing the Key to unregister from.
     * @param listener    The {@link UpdateListener} to unregister.
     * @return True if the {@link UpdateListener} was registered before this call, false otherwise.
     */
    boolean unregisterListener(String segmentName, ArrayView key, UpdateListener listener);

    /**
     * Defines iteration result that is returned by the {@link AsyncIterator} when invoking
     * {@link #iterator(String, Object, Duration)}.
     */
    interface IteratorItem {
        /**
         * Gets an object that can be used to reinvoke {@link TableStore#iterator(String, Object, Duration)} if a previous
         * iteration has been interrupted (by losing the pointer to the {@link AsyncIterator}), system restart, etc.
         * TODO: this will be properly defined when the implementation is ready, but it will be a serializable object (String, byte[], etc.).
         */
        Object getContinuationToken();

        /**
         * Gets a List of {@link TableEntry} instances that are contained in this instance. For efficiency reasons, entries
         * may be batched together. The entries in this list are not necessarily related to each other, nor are they
         * guaranteed to be in any particular order.
         *
         * @return A List of {@link TableEntry} instances.
         */
        List<TableEntry> getEntries();
    }

    /**
     * Defines a listener for updates for a particular Key.
     * Note about consistency and concurrency:
     * * These callbacks will always be invoked after a successful Table Update and may be invoked in parallel with the
     * completion of the Future that is returned with every invocation.
     * * For updates in quick succession, it may be possible that not all updates may be captured. If the same key is
     * updated multiple times and those updates are batched together (externally or internally), only the latest change
     * will be reflected in one of these callbacks.
     */
    interface UpdateListener extends AutoCloseable {
        /**
         * This will be invoked by the Table Store when the Update Listener is unregistered. It may be unregistered by
         * one of the following cases:
         * * When {@link TableStore#unregisterListener(String, ArrayView, UpdateListener)} is invoked with the correct arguments.
         * * When {@link TableStore#deleteSegment(String, Duration)} is invoked for the Table Segment for which this is registered.
         * * When {@link TableStore#seal(String, Duration)} is invoked for the Table Segment for which this is registered.
         * * When {@link TableStore#merge(String, String, Duration)} is invoked for the Source Table Segment for which
         * this is registered.
         * * When the Segment Container on which the segment for which this is registered for is shutting down.
         *
         * This is not invoked when the Key associated with this instance is removed.
         */
        @Override
        void close();

        /**
         * This will be invoked every time the requested Key is updated or inserted.
         *
         * @param currentEntry A {@link TableEntry} with the current value.
         */
        void entryUpdated(TableEntry currentEntry);

        /**
         * This will be invoked every time the requested Key is removed.
         *
         * @param removedKey A {@link TableKey} that was removed.
         */
        void keyRemoved(TableKey removedKey);
    }
}
