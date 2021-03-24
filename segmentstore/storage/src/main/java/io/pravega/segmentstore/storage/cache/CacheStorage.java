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
package io.pravega.segmentstore.storage.cache;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.storage.CacheException;
import java.util.function.Supplier;
import lombok.NonNull;

/**
 * Defines a generic, non-associative, block-based Cache Storage.
 */
public interface CacheStorage extends AutoCloseable {
    /**
     * Gets a value representing a "null" address.
     */
    int NO_ADDRESS = CacheLayout.NO_ADDRESS;

    /**
     * Gets a value representing the size of one block. For efficiency purposes, it is highly recommended that all inserted
     * data align to this value (i.e., have a length that is a multiple of this value).
     *
     * @return The block alignment, in bytes.
     */
    int getBlockAlignment();

    /**
     * Gets a value representing the maximum size of an entry that can be stored in this {@link CacheStorage}.
     *
     * @return The maximum size, in bytes.
     */
    int getMaxEntryLength();

    /**
     * Inserts a new entry into this {@link CacheStorage}.
     *
     * @param data A {@link BufferView} representing the entry to insert. May be empty. Must have {@link BufferView#getLength()}
     *             less than or equal to {@link #getMaxEntryLength()}.
     * @return An integer representing the address where this data was inserted at. This can be used to invoke {@link #append},
     * {@link #get}, {@link #delete} or {@link #replace}.
     * @throws CacheFullException If there is no more capacity in the {@link CacheStorage} to accomodate this entry.
     */
    int insert(@NonNull BufferView data);

    /**
     * Replaces the entry at the given address with new content.
     *
     * @param address The address to replace. May be obtained via {@link #insert} or {@link #replace}. If there is no
     *                entry with this address, this method will act like {@link #insert}.
     * @param data    A {@link BufferView} representing the new contents of the entry. May be empty. Must have
     *                {@link BufferView#getLength()} less than or equal to {@link #getMaxEntryLength()}.
     * @return An integer representing the address where this data now resides. Depending on the implementation, this may
     * or may not be equal to `address`. This can be used to invoke {@link #append}, {@link #get} or {@link #delete}.
     * @throws CacheFullException If there is no more capacity in the {@link CacheStorage} to accommodate this entry.
     * @throws CacheException     If another unexpected exception occurred.
     */
    int replace(int address, @NonNull BufferView data);

    /**
     * Gets the number of bytes that can be appended to an entry that has the given length.
     *
     * @param currentLength The current length of the entry.
     * @return An non-negative integer representing the number of bytes that can be appended.
     */
    int getAppendableLength(int currentLength);

    /**
     * Performs an atomic compare-and-appends of data at the end of an entry. This operation will only take effect if
     * the current length of the entry matches the argument `expectedLength` and both the entry's length and contents
     * will be updated atomically.
     *
     * @param address        An integer representing the address to append to.
     * @param expectedLength The currently known length of this entry.
     * @param data           A {@link BufferView} representing the data to append.
     * @return An integer representing the number of bytes that were appended. If 0, then the entry at the given address
     * is already at its max capacity (its length equals {@link #getMaxEntryLength()}.
     * @throws IncorrectCacheEntryLengthException If `expectedLength` does not match the current length of the entry.
     * @throws IllegalArgumentException If address does not point to a valid entry or the given {@link BufferView} is
     * too long to fit into the given entry (use {@link #getAppendableLength} to determine maximum length).
     */
    int append(int address, int expectedLength, @NonNull BufferView data);

    /**
     * Deletes the entry at the given address. If this address does not currently map to an entry, this operation will have
     * no effect.
     *
     * @param address An integer representing the address to delete.
     */
    void delete(int address);

    /**
     * Retrieves the contents of an entry with the given address.
     *
     * @param address An integer representing the address to retrieve.
     * @return A read-only {@link BufferView} that can be used to access the data, or null if no entry is mapped to this
     * address.
     */
    BufferView get(int address);

    /**
     * Returns a {@link CacheState} representing the current state of the {@link CacheStorage}.
     *
     * @return A new {@link CacheState} instance.
     */
    CacheState getState();

    /**
     * Sets a callback that will be invoked during {@link #insert} if there is insufficient capacity to add more entries.
     *
     * @param cacheFullCallback    The callback to invoke. This should return `true` if a cache cleanup was performed, and
     *                             `false` otherwise.
     * @param retryDelayBaseMillis The amount of time to wait between retries if cacheFullCallback returns `false`.
     */
    void setCacheFullCallback(@NonNull Supplier<Boolean> cacheFullCallback, int retryDelayBaseMillis);

    /**
     * Closes this {@link CacheStorage} instance and releases all resources used by it.
     */
    @Override
    void close();
}
