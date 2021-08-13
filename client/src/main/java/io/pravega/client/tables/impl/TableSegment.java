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
package io.pravega.client.tables.impl;

import com.google.common.collect.Iterators;
import io.netty.buffer.ByteBuf;
import io.pravega.client.tables.ConditionalTableUpdateException;
import io.pravega.client.tables.IteratorItem;
import io.pravega.common.util.AsyncIterator;
import io.pravega.shared.protocol.netty.WireCommands;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that are supported on a Table Segment.
 *
 * Types of updates:
 * * Unconditional Updates will insert and/or overwrite any existing values for the given Key, regardless of whether that Key
 * previously existed or not, and regardless of what that Key's version is.
 * * Conditional Updates will only overwrite an existing value if the specified version matches that Key's version. If
 * the key does not exist, the {@link TableSegmentKey} or {@link TableSegmentEntry} must have been created with
 * {@link TableSegmentKeyVersion#NOT_EXISTS} in order for the update to succeed.
 * * Unconditional Removals will remove a Key regardless of what that Key's version is. The operation will also succeed (albeit
 * with no effect) if the Key does not exist.
 * * Conditional Removals will remove a Key only if the specified {@link TableSegmentKey#getVersion()} matches that Key's version.
 * It will also fail (with no effect) if the Key does not exist and Version is not set to
 * {@link TableSegmentKeyVersion#NOT_EXISTS}.
 *
 * A note about {@link ByteBuf}s. All the methods defined in this interface make use of {@link ByteBuf} either directly
 * or via {@link TableSegmentKey}/{@link TableSegmentEntry}. It is expected that no implementation of the {@link TableSegment}
 * interface will either retain ({@link ByteBuf#retain()}) or release ({@link ByteBuf#release()}) these buffers during
 * execution. The lifecycle of these buffers should be maintained externally by the calling code, using the following
 * guidelines:
 * * For methods that accept externally-provided {@link ByteBuf}s, the calling code should call {@link ByteBuf#retain()}
 * prior to invoking the method on {@link TableSegment} and should invoke {@link ByteBuf#release()} afterwards.
 * * For methods that return internally-generated {@link ByteBuf}s (such as {@link #get}), the calling code should
 * invoke {@link ByteBuf#release()} as soon as it is done with processing the result. If the result needs to be held onto
 * for a longer duration, the caller should make a copy of it and release the {@link ByteBuf} that was provided from the
 * call.
 */
public interface TableSegment extends AutoCloseable {
    /**
     * The maximum length of a Table Segment Key.
     * Synchronized with io.pravega.segmentstore.contracts.tables.TableStore.MAXIMUM_KEY_LENGTH.
     */
    int MAXIMUM_KEY_LENGTH = 8192;

    /**
     * The maximum length of a Table Segment Value.
     * Synchronized with io.pravega.segmentstore.contracts.tables.TableStore.MAXIMUM_VALUE_LENGTH.
     */
    int MAXIMUM_VALUE_LENGTH = 1024 * 1024 - MAXIMUM_KEY_LENGTH;
    /**
     * Maximum number of Entries that can be updated, removed or retrieved with a single request.
     */
    int MAXIMUM_BATCH_KEY_COUNT = 256;
    /**
     * Maximum total serialization length of all keys and values for any given request.
     */
    int MAXIMUM_BATCH_LENGTH = WireCommands.MAX_WIRECOMMAND_SIZE - 8192;

    /**
     * Inserts a new or updates an existing Table Entry into this Table Segment.
     *
     * @param entry The Entry to insert or update. If {@link TableSegmentEntry#getKey()}{@link TableSegmentKey#getVersion()}
     *              indicates a conditional update this will perform a Conditional Update conditioned on the server-side
     *              version matching the provided one. See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain the {@link TableSegmentKeyVersion} associated with
     * the newly inserted or updated entry. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<TableSegmentKeyVersion> put(TableSegmentEntry entry) {
        return put(Iterators.singletonIterator(entry)).thenApply(result -> result.get(0));
    }

    /**
     * Inserts new or updates existing Table Entries into this Table Segment.
     * All changes are performed atomically (either all or none will be accepted).
     *
     * @param entries An Iterator containing the entries to insert or update. If for at least one such entry,
     *                {@link TableSegmentEntry#getKey()}{@link TableSegmentKey#getVersion()} indicates a conditional
     *                update, this will perform an atomic Conditional Update conditioned on the server-side version for
     *                each such entry matching the provided one.
     *                See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableSegmentKeyVersion} instances
     * which represent the versions for the inserted/updated keys. The returned list will contain the same number of
     * elements as are remaining in the entries iterator and the resulting versions will be in the same order.
     * Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Update and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<List<TableSegmentKeyVersion>> put(Iterator<TableSegmentEntry> entries);

    /**
     * Removes the given key from this Table Segment.
     *
     * @param key The Key to remove. If {@link TableSegmentKey#getVersion()} indicates a conditional update, this will
     *            perform an atomic removal conditioned on the server-side version matching the provided one.
     *            See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate the Key has been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    default CompletableFuture<Void> remove(TableSegmentKey key) {
        return remove(Iterators.singletonIterator(key));
    }

    /**
     * Removes one or more keys from this Table Segment.
     * All removals are performed atomically (either all keys or no key will be removed).
     *
     * @param keys An Iterator containing the keys to remove. If for at least one such key, {@link TableSegmentKey#getVersion()}
     *             indicates a conditional update, this will perform an atomic removal conditioned on the server-side
     *             version matching the provided one.
     *             See {@link TableSegment} doc for more details on Types of Updates.
     * @return A CompletableFuture that, when completed, will indicate that the keys have been removed. Notable exceptions:
     * <ul>
     * <li>{@link ConditionalTableUpdateException} If this is a Conditional Removal and the condition was not satisfied.
     * </ul>
     */
    CompletableFuture<Void> remove(Iterator<TableSegmentKey> keys);

    /**
     * Gets the latest value for the given Key.
     *
     * @param key A {@link ByteBuf} representing the Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    default CompletableFuture<TableSegmentEntry> get(ByteBuf key) {
        return get(Iterators.singletonIterator(key)).thenApply(result -> result.get(0));
    }

    /**
     * Gets the latest values for the given Keys.
     *
     * @param keys An Iterator of {@link ByteBuf} instances representing the Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a List of {@link TableSegmentEntry} instances for
     * the requested keys. The returned list will contain the same number of elements as are remaining in the keys iterator
     * and the results will be in the same order as the requested keys. Any keys which do not have a value will have a
     * null entry at their index.
     */
    CompletableFuture<List<TableSegmentEntry>> get(Iterator<ByteBuf> keys);

    /**
     * Creates a new Iterator over all the Keys in the Table Segment.
     *
     * @param args A {@link SegmentIteratorArgs} that can be used to configure the iterator.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Keys in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableSegmentKey>> keyIterator(SegmentIteratorArgs args);

    /**
     * Creates a new Iterator over all the Entries in the Table Segment.
     *
     * @param args A {@link SegmentIteratorArgs} that can be used to configure the iterator.
     * @return An {@link AsyncIterator} that can be used to iterate over all the Entries in this Table Segment.
     */
    AsyncIterator<IteratorItem<TableSegmentEntry>> entryIterator(SegmentIteratorArgs args);

    /**
     * Gets the number of entries in the Table Segment.
     *
     * NOTE: this is an "eventually consistent" value:
     * <ul>
     * <li> In-flight (not yet acknowledged) updates and removals are not included.
     * <li> Recently acknowledged updates and removals may or may not be included (depending on whether they were
     * conditional or not). As the index is updated (in the background), this value will eventually converge towards the
     * actual number of entries in the Table Segment.
     * </ul>
     *
     * @return A CompletableFuture that, when completed, will contain the number of entries in the Table Segment.
     */
    CompletableFuture<Long> getEntryCount();

    /**
     * Gets a value indicating the internal Id of the Table Segment, as assigned by the Controller.
     *
     * @return The Table Segment Id.
     */
    long getSegmentId();

    @Override
    void close();
}
