/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.util;

import java.util.Optional;
import java.util.function.Consumer;

/**
 * Defines an Index that orders its IndexEntries by a Key.
 *
 * @param <K> The type of the Key.
 * @param <V> The type of the IndexEntries.
 */
public interface SortedIndex<K, V extends IndexEntry<K>> {
    /**
     * Clears the contents of the Index.
     */
    void clear();

    /**
     * Inserts the given item into the Index. If there already exists an item with the same key, it will be overridden.
     *
     * @param item The item to insert.
     * @return An Optional containing the displaced item, if any.
     */
    Optional<V> put(V item);

    /**
     * Removes any item with the given key from the Index.
     *
     * @param key The key of the item to remove.
     * @return An Optional containing the removed item, or empty if nothing was removed.
     */
    Optional<V> remove(K key);

    /**
     * Gets a value indicating the number of items in the Index.
     */
    int size();

    /**
     * Gets an item with the given key.
     *
     * @param key The key to search by.
     * @return An Optional containing the requested item, if it exists, or empty if it doesn't.
     */
    Optional<V> get(K key);

    /**
     * Gets the smallest item whose key is greater than or equal to the given key.
     *
     * @param key the Key to search by.
     * @return An Optional containing the sought item, or empty if it doesn't exist.
     */
    Optional<V> getCeiling(K key);

    /**
     * Gets the smallest item in the index.
     *
     * @return An Optional containing the sought item, or empty (if no items in the index).
     */
    Optional<V> getFirst();

    /**
     * Gets the largest item in the index.
     *
     * @return An Optional containing the sought item, or empty (if no items in the index).
     */
    Optional<V> getLast();

    /**
     * Iterates through each item in the Index, in natural order, and calls the given consumer on all of them.
     *
     * @param consumer The consumer to invoke.
     */
    void forEach(Consumer<V> consumer);
}
