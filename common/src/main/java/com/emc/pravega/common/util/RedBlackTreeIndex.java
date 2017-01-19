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

import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;

/**
 * SortedIndex backed by a Red-Black Tree (java.util.TreeMap).
 *
 * @param <K> The type of the Key.
 * @param <V> The type of the IndexEntries.
 */
public class RedBlackTreeIndex<K, V extends IndexEntry<K>> implements SortedIndex<K, V> {
    // region Members

    private final TreeMap<K, V> map;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedBlackTreeIndex class.
     *
     * @param comparator A Key comparator to use as orderer within the index.
     */
    public RedBlackTreeIndex(Comparator<K> comparator) {
        this.map = new TreeMap<>(comparator);
    }

    //endregion

    //region SortedIndex implementation

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public Optional<V> put(V item) {
        return Optional.ofNullable(this.map.put(item.key(), item));
    }

    @Override
    public Optional<V> remove(K key) {
        return Optional.ofNullable(this.map.remove(key));
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public Optional<V> get(K key) {
        return Optional.ofNullable(this.map.get(key));
    }

    @Override
    public Optional<V> getCeiling(K key) {
        return getValue(this.map.ceilingEntry(key));
    }

    @Override
    public Optional<V> getFloor(K key) {
        return getValue(this.map.floorEntry(key));
    }

    @Override
    public Optional<V> getFirst() {
        return getValue(this.map.firstEntry());
    }

    @Override
    public Optional<V> getLast() {
        return getValue(this.map.lastEntry());
    }

    @Override
    public void forEach(Consumer<V> consumer) {
        this.map.values().forEach(consumer);
    }

    private Optional<V> getValue(Map.Entry<K, V> e) {
        return e == null ? Optional.empty() : Optional.of(e.getValue());
    }

    //endregion
}
