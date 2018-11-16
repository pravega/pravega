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
 * Conditional vs Unconditional Updates
 * * The {@link #put(String, List, Duration)} and {@link #remove(String, Collection, Duration)} methods support conditional
 * updates.
 * * If at least one of the {@link TableEntry} instances in the put() call has {@link TableEntry#getKey()#hasVersion()} true,
 * or if at least one of the {@link TableKey} instances in the remove() call has {@link TableKey#hasVersion()} true, then
 * the operation will perform a Conditional Update. If the "hasVersion()" method returns false for ALL the items in the
 * respective collections, then the operation will perform an Unconditional Update.
 * * Conditional Updates compare the provided {@link TableKey#getVersion()} or {@link TableEntry#getKey()#getVersion()}
 * against the version of the Key in the specified Table Segment. If the versions match, the Update (insert, update, remove)
 * will be allowed, otherwise it will be rejected. For a batched update (a Collection of {@link TableKey} or {@link TableEntry}),
 * all the items in the collection must meet the version comparison criteria in order for the entire batch to be accepted
 * (all items in the Collection will either all be accepted or all be rejected). Any Conditional Update (batch or not)
 * will be atomically checked-and-applied.
 * * Unconditional Updates (insert, update, remove) will take effect regardless of what the current Key version exists in
 * the Table Segment.
 */
@Beta
public interface TableStore {
    /**
     * Gets a value indicating the maximum length of any Table Entry Key supported by this TableStore implementation.
     *
     * @return The maximum length of any key, in bytes.
     */
    default int maximumKeyLength() {
        return 8192;
    }

    /**
     * Gets a value indicating the maximum length of any Table Entry Value supported by this TableStore Implementation.
     *
     * @return The maximum length of any value, in bytes.
     */
    default int maximumValueLength() {
        return 1040384; // 1MB - maximumKeyLength();
    }

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
     * @param mustBeEmpty If true, the Table Segment will only be deleted if it is empty (contains no keys).
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * <li>{@link TableSegmentNotEmptyException} If mustBeEmpty is true and the Table Segment contains at least one key.
     * </ul>
     */
    CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout);

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
     * Inserts new or updates existing Table Entries into the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to insert/update the Table Entries.
     * @param entries     A List of {@link TableEntry} instances to insert or update. If {@link TableEntry#getKey()#hasVersion()}
     *                    returns true for at least one of the items in the list, then this will perform an atomic Conditional
     *                    Update. If {@link TableEntry#getKey()#hasVersion()} returns false for ALL items in the list, then this
     *                    will perform an Unconditional update.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the current version of the each TableEntry
     * Key provided. The versions will be in the same order as the TableEntry instances provided. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If {@link TableEntry#getKey()} exceeds {@link #maximumKeyLength()}.
     * <li>{@link TableValueTooLongException} If {@link TableEntry#getValue()} exceeds {@link #maximumValueLength()}.
     * <li>{@link ConditionalTableUpdateException} If {@link TableEntry#getKey()#hasVersion()} is true and
     * {@link TableEntry#getKey()#getVersion()} does not match the Table Entry's Key current Table Version.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout);

    /**
     * Removes one or more Table Keys from the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to remove the Keys from.
     * @param keys        A Collection of {@link TableKey} instances to remove. If {@link TableKey#hasVersion()} returns
     *                    true for at least one of the TableKeys in this collection, then this will perform an atomic
     *                    Conditional Update (Removal). If {@link TableKey#hasVersion()} returns false for ALL items in
     *                    the collection, then this will perform an Unconditional Update (Removal).
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
     * Creates a new Iterator over all the {@link TableKey} instances the given Table Segment.
     *
     * @param segmentName     The name of the Table Segment to iterate over.
     * @param serializedState (Optional) A byte array representing the serialized form of the State. This can be obtained
     *                        from {@link IteratorItem#getState()}. If provided, the iteration will resume from where it
     *                        left off, otherwise it will start from the beginning.
     * @param fetchTimeout    Timeout for each invocation to {@link AsyncIterator#getNext()}.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableKey} instances in the Table. If the operation failed, the Future will be failed with the
     * causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * <li>{@link java.io.IOException} If serializedState is non-null but it cannot be deserialized.
     * </ul>
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, byte[] serializedState, Duration fetchTimeout);

    /**
     * Creates a new Iterator over all the {@link TableEntry} instances the given Table Segment.
     *
     * @param segmentName     The name of the Table Segment to iterate over.
     * @param serializedState (Optional) A byte array representing the serialized form of the State. This can be obtained
     *                        from {@link IteratorItem#getState()}. If provided, the iteration will resume from where it
     *                        left off, otherwise it will start from the beginning.
     * @param fetchTimeout    Timeout for each invocation to {@link AsyncIterator#getNext()}.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableEntry} instances in the Table. If the operation failed, the Future will be failed with the
     * causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * <li>{@link java.io.IOException} If serializedState is non-null but it cannot be deserialized.
     * </ul>
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, byte[] serializedState, Duration fetchTimeout);
}
