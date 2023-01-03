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
package io.pravega.segmentstore.contracts.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.BadSegmentTypeException;
import io.pravega.segmentstore.contracts.SegmentType;
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
 * * If at least one of the {@link TableEntry} instances in the {@link #put} call has {@link TableEntry#getKey()}.{@link TableKey#hasVersion} true,
 * or if at least one of the {@link TableKey} instances in the remove() call has {@link TableKey#hasVersion()} true, then
 * the operation will perform a Conditional Update. If the "hasVersion()" method returns false for ALL the items in the
 * respective collections, then the operation will perform an Unconditional Update.
 * * Conditional Updates compare the provided {@link TableKey#getVersion()} or {@link TableEntry#getKey()} {@link TableKey#getVersion()}
 * against the version of the Key in the specified Table Segment. If the versions match, the Update (insert, update, remove)
 * will be allowed, otherwise it will be rejected. For a batched update (a Collection of {@link TableKey} or {@link TableEntry}),
 * all the items in the collection must meet the version comparison criteria in order for the entire batch to be accepted
 * (all items in the Collection will either all be accepted or all be rejected). Any Conditional Update (batch or not)
 * will be atomically checked-and-applied.
 * * Unconditional Updates (insert, update, remove) will take effect regardless of what the current Key version exists in
 * the Table Segment.
 *
 * Sorted vs Non-Sorted Table Segments:
 * * All Table Segments are a Hash-Table-like data structure, where Keys are mapped to Values.
 * * Non-Sorted Table Segments provide no ordering guarantees for {@link #keyIterator} or {@link #entryIterator}.
 * * Sorted Table Segments store additional information about the Keys and will return results for {@link #keyIterator}
 * or {@link #entryIterator} in lexicographic bitwise order. All other contracts are identical to the Non-Sorted variant.
 * * Sorted Table Segments will require additional storage space to store the ordered Keys and may require additional
 * processing time to maintain the said data structure. This extra storage space is proportional to the number and length
 * of Keys, but not the size of the values.
 * * Sorted Table Segments do not support conditional Table Segment Deletion {@link #deleteSegment(String, boolean, Duration)}
 * with `true` passed as the second argument.
 * * Sorted Table Segments Key Iterators ({@link #keyIterator}) will not return Key Versions. Versions can still be
 * retrieved for individual Keys using {@link #get} or using the Entry Iterator {@link #entryIterator}.
 */
public interface TableStore {
    /**
     * Gets a value indicating the maximum length of any Table Entry Key supported by the TableStore.
     */
    int MAXIMUM_KEY_LENGTH = 8192;

    /**
     * Gets a value indicating the maximum length of any Table Entry Value supported by the TableStore.
     */
    int MAXIMUM_VALUE_LENGTH = 1024 * 1024 - MAXIMUM_KEY_LENGTH;

    /**
     * Creates a new Segment and marks it as a Table Segment.
     * This segment may not be used for Streaming purposes (i.e., it cannot be used with {@link StreamSegmentStore}).
     *
     * This cannot be used to create Fixed-Key Table Segments. Use {@link #createSegment(String, SegmentType, TableSegmentConfig, Duration)} instead.
     *
     * @param segmentName The name of the Table Segment to create.
     * @param segmentType Type of Segment to Create. If not already specified, this will automatically set the
     *                    {@link  SegmentType#isTableSegment()} to true.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * failed, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentExistsException} If the Segment does exist (whether as a Table Segment or Stream Segment).
     * </ul>
     */
    default CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, Duration timeout) {
        Preconditions.checkArgument(!segmentType.isFixedKeyLengthTableSegment(), "Cannot create Fixed-Key-Length Table Segments using this API.");
        return createSegment(segmentName, segmentType, TableSegmentConfig.NO_CONFIG, timeout);
    }

    /**
     * Creates a Table Segment.
     * This segment may not be used for Streaming purposes (i.e., it cannot be used with {@link StreamSegmentStore}).
     *
     * @param segmentName The name of the Table Segment to create.
     * @param segmentType Type of Segment to Create. If not already specified, this will automatically set the
     *                    {@link  SegmentType#isTableSegment()} to true. If {@link  SegmentType#isFixedKeyLengthTableSegment()}
     *                    is true, this will create a Fixed-Key-Length Table Segment (EXPERIMENTAL), otherwise it will be
     *                    a plain Hash Table. See {@link TableStore} Javadoc for difference between the two.
     * @param config      A {@link TableSegmentConfig} to use.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate the operation completed. If the operation
     * led, the future will be failed with the causing exception. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentExistsException} If the Segment does exist (whether as a Table Segment or Stream Segment).
     * </ul>
     */
    CompletableFuture<Void> createSegment(String segmentName, SegmentType segmentType, TableSegmentConfig config, Duration timeout);

    /**
     * Deletes an existing Table Segment.
     *
     * @param segmentName The name of the Table Segment to delete.
     * @param mustBeEmpty If true, the Table Segment will only be deleted if it is empty (contains no keys). This is not
     *                    supported on Sorted Table Segments.
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
     * Inserts new or updates existing Table Entries into the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to insert/update the Table Entries.
     * @param entries     A List of {@link TableEntry} instances to insert or update. If {@link TableEntry#getKey()} {@link TableKey#hasVersion}
     *                    returns true for at least one of the items in the list, then this will perform an atomic Conditional
     *                    Update. If {@link TableEntry#getKey()} {@link TableKey#getVersion()} hasVersion} returns false for ALL items in the list, then this
     *                    will perform an Unconditional update.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the current version of the each TableEntry
     * Key provided. The versions will be in the same order as the TableEntry instances provided. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.</li>
     * <li>{@link TableKeyTooLongException} If {@link TableEntry#getKey()} exceeds {@link #MAXIMUM_KEY_LENGTH}.</li>
     * <li>{@link TableValueTooLongException} If {@link TableEntry#getValue()} exceeds {@link #MAXIMUM_VALUE_LENGTH}.</li>
     * <li>{@link ConditionalTableUpdateException} If {@link TableEntry#getKey()} {@link TableKey#hasVersion() hasVersion() } is true and
     * {@link TableEntry#getKey()} {@link TableKey#getVersion()} does not match the Table Entry's Key current Table Version. </li>
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment. </li>
     * </ul>
     */
    CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout);

    /**
     * Inserts new or updates existing Table Entries (conditioned on the Segment's length matching an expected value)
     * into the given Segment.
     *
     * @param segmentName        The name of the Table Segment to insert/update the Table Entries.
     * @param entries            A List of {@link TableEntry} instances to insert or update. If {@link TableEntry#getKey()}
     *                           {@link TableKey#getVersion()}  hasVersion} returns true for at least one of the items in the list,
     *                           then this will perform an atomic Conditional Update. If {@link TableEntry#getKey()}
     *                           {@link TableKey#getVersion()} hasVersion} returns false for ALL items in the list, then this
     *                           will just be conditioned on the tableSegmentOffset value.
     * @param tableSegmentOffset The expected offset of the TableSegment used for conditional (expected matches actual) appends.
     * @param timeout            Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a List with the current version of the each TableEntry
     * Key provided. The versions will be in the same order as the TableEntry instances provided. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.</li>
     * <li>{@link TableKeyTooLongException} If {@link TableEntry#getKey()} exceeds {@link #MAXIMUM_KEY_LENGTH}.</li>
     * <li>{@link TableValueTooLongException} If {@link TableEntry#getValue()} exceeds {@link #MAXIMUM_VALUE_LENGTH}.</li>
     * <li>{@link ConditionalTableUpdateException} If {@link TableEntry#getKey()} {@link TableKey#hasVersion() hasVersion() } is true and
     * {@link TableEntry#getKey()} {@link TableKey#getVersion()} does not match the Table Entry's Key current Table Version. </li>
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment. </li>
     * <li>{@link BadOffsetException} If there is a mismatch between the provided {@code tableSegmentOffset} and the actual segment length.</li>
     * </ul>
     */
    CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, long tableSegmentOffset, Duration timeout);

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
     * <li>{@link TableKeyTooLongException} If {@link TableKey#getKey()} exceeds {@link #MAXIMUM_KEY_LENGTH}.
     * <li>{@link ConditionalTableUpdateException} If {@link TableKey#hasVersion()} is true and {@link TableKey#getVersion()}.
     * does not match the Table Entry's Key current Table Version.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout);

    /**
     * Removes one or more Table Keys from the given Table Segment.
     *
     * @param segmentName        The name of the Table Segment to remove the Keys from.
     * @param keys               A Collection of {@link TableKey} instances to remove. If {@link TableKey#hasVersion()} returns
     *                           true for at least one of the TableKeys in this collection, then this will perform an atomic
     *                           Conditional Update (Removal). If {@link TableKey#hasVersion()} returns false for ALL items in
     *                           the collection, then this just be conditioned on the tableSegmentOffset value.
     * @param tableSegmentOffset The expected offset of the TableSegment used for conditional (expected matches actual) appends.
     * @param timeout            Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation completed. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If {@link TableKey#getKey()} exceeds {@link #MAXIMUM_KEY_LENGTH}.
     * <li>{@link ConditionalTableUpdateException} If {@link TableKey#hasVersion()} is true and {@link TableKey#getVersion()}.
     * does not match the Table Entry's Key current Table Version.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * <li>{@link BadOffsetException} If there is a mismatch between the provided {@code tableSegmentOffset} and the actual segment length.
     * </ul>
     */
    CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, long tableSegmentOffset, Duration timeout);

    /**
     * Looks up a List of Keys in the given Table Segment.
     *
     * @param segmentName The name of the Table Segment to look up into.
     * @param keys        A List of {@link BufferView} instances representing the Keys to look up.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the sought result. This will be organized as a List
     * of {@link TableEntry} instances in the same order as their Keys in the provided input list. If the operation failed,
     * the future will be failed with the causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link TableKeyTooLongException} If any of the items in "keys" exceeds {@link #MAXIMUM_KEY_LENGTH}.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<List<TableEntry>> get(String segmentName, List<BufferView> keys, Duration timeout);

    /**
     * Creates a new {@link AsyncIterator} over all the {@link TableKey} instances in the given Table Segment. This is a resumable
     * iterator; this method can be reinvoked using the {@link IteratorItem#getState()} from the last processed item
     * and the resulting iterator will continue from where the previous one left off.
     *
     * It is important to note that this iterator may not provide a consistent view of the Table Segment. Due to its async
     * nature, it is expected that the resulting {@link AsyncIterator} may be long lived (especially for large tables).
     * Since it does not lock the Table Segment for updates or compactions while iterating, it is possible that it will
     * include changes made to the Table Segment after the initial invocation to this method (this is because, during
     * compactions, portions of the index may be truncated and rewritten using newer information). Whether this happens
     * or not, it is completely transparent to the caller and it will still iterate through all the {@link TableKey}s in
     * the table.
     *
     * @param segmentName The name of the Table Segment to iterate over.
     * @param args        Arguments for the Iterator.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableKey} instances in the Table. If the operation failed, the Future will be failed with the
     * causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     * @throws IllegalDataFormatException If serializedState is not null and cannot be deserialized.
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args);

    /**
     * Creates a new {@link AsyncIterator} over all the {@link TableEntry} instances in the given Table Segment.
     * <p>
     * Please refer to {@link #keyIterator} for notes about consistency and the ability to resume.
     *
     * @param segmentName The name of the Table Segment to iterate over.
     * @param args        Arguments for the Iterator.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableEntry} instances in the Table. If the operation failed, the Future will be failed with the
     * causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     * @throws IllegalDataFormatException If serializedState is not null and cannot be deserialized.
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args);

    /**
     * Creates a new {@link AsyncIterator} over all the {@link TableEntry} instances in the given Table Segment starting from a given position.
     *
     * The entryDeltaIterator directly traverses the underlying contents of the TableSegment and deserializes the read bytes into
     * {@link TableEntry}s. The bounds of the iteration (start and end positions) are determined prior to instantiation of the underlying
     * iterator, and are fixed for the duration of the iteration. The 'actual' startPosition is determined by the max
     * of the fromPosition and the observed {@link TableAttributes#COMPACTION_OFFSET}. Every TableEntry appended to the TableSegment
     * will be returned (including deletions).
     *
     * Please refer to {@link #keyIterator} for notes about consistency and the ability to resume.
     *
     * @param segmentName       The name of the Table Segment to iterate over.
     * @param fromPosition      The position to begin iteration at.
     * @param fetchTimeout      Timeout for each invocation to {@link AsyncIterator#getNext()}.
     * @return A CompletableFuture that, when completed, will return an {@link AsyncIterator} that can be used to iterate
     * over all the {@link TableEntry} instances starting at {@code fromPosition}. If the operation failed, the Future will be failed with the
     * causing exception. Notable exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     * @throws IllegalDataFormatException If serializedState is not null and cannot be deserialized.
     */
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout);

    /**
     * Gets information about a Table Segment.
     *
     * @param segmentName The name of the Table Segment.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the result. If the operation failed, the
     * future will be failed with the causing exception. Note that this result will only contain the Core Attributes
     * for this Segment. Notable Exceptions:
     * <ul>
     * <li>{@link StreamSegmentNotExistsException} If the Table Segment does not exist.
     * <li>{@link BadSegmentTypeException} If segmentName refers to a non-Table Segment.
     * </ul>
     */
    CompletableFuture<TableSegmentInfo> getInfo(String segmentName, Duration timeout);
}
