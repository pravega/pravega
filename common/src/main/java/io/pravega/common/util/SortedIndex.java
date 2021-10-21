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
package io.pravega.common.util;

import java.util.function.Consumer;

/**
 * Defines an Index that orders its IndexEntries by an Int64 (long) Key.
 * <p>
 * Notes:
 * <ul>
 * <li> Implementations of this interface are not necessarily thread-safe and no assumptions should be made about
 * multi-thread consistency.
 * <li> No implementation of this class should be able to index null values. As such, for all the retrieval methods,
 * a null value can be safely interpreted as no result found.
 * </ul>
 *
 * @param <V> The type of the IndexEntries.
 */
public interface SortedIndex<V extends SortedIndex.IndexEntry> {
    /**
     * Clears the contents of the Index.
     */
    void clear();

    /**
     * Inserts the given item into the Index. If there already exists an item with the same key, it will be overridden.
     *
     * @param item The item to insert.
     * @return The displaced item, if any.
     */
    V put(V item);

    /**
     * Removes any item with the given key from the Index.
     *
     * @param key The key of the item to remove.
     * @return The removed item, or null if nothing was removed.
     */
    V remove(long key);

    /**
     * Gets a value indicating the number of items in the Index.
     *
     * @return Integer indicating the size or number of items in the Index.
     */
    int size();

    /**
     * Gets an item with the given key.
     *
     * @param key The key to search by.
     * @return The requested item, if it exists, or null if it doesn't.
     */
    V get(long key);

    /**
     * Gets the smallest item whose key is greater than or equal to the given key.
     *
     * @param key the Key to search by.
     * @return The sought item, or null if it doesn't exist.
     */
    V getCeiling(long key);

    /**
     * Gets the largest item whose key is smaller than or equal to the given key.
     *
     * @param key the Key to search by.
     * @return The sought item, or null if it doesn't exist.
     */
    V getFloor(long key);

    /**
     * Gets the smallest item in the index.
     *
     * @return The sought item, or null (if no items in the index).
     */
    V getFirst();

    /**
     * Gets the largest item in the index.
     *
     * @return The sought item, or null (if no items in the index).
     */
    V getLast();

    /**
     * Iterates through each item in the Index, in natural order, and calls the given consumer on all of them.
     *
     * @param consumer The consumer to invoke.
     * @throws java.util.ConcurrentModificationException If the Index is modified while this method is executing.
     */
    void forEach(Consumer<V> consumer);

    /**
     * Defines a generic entry into an Index.
     */
    interface IndexEntry {
        /**
         * Gets a value representing the key of the entry. The Key should not change for the lifetime of the entry and
         * should be very cheap to return (as it is used very frequently).
         *
         * @return The entry key.
         */
        long key();
    }
}
