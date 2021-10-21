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

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * SortedIndex backed by a Red-Black Tree (java.util.TreeMap).
 * <p>
 * Note: This class is not thread-safe and requires external synchronization when in a multi-threaded environment.
 *
 * @param <V> The type of the IndexEntries.
 */
@NotThreadSafe
public class RedBlackTreeIndex<V extends SortedIndex.IndexEntry> implements SortedIndex<V> {
    // region Members

    private final TreeMap<Long, V> map;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RedBlackTreeIndex class.
     */
    public RedBlackTreeIndex() {
        this.map = new TreeMap<>(Long::compare);
    }

    //endregion

    //region SortedIndex implementation

    @Override
    public void clear() {
        this.map.clear();
    }

    @Override
    public V put(V item) {
        return this.map.put(item.key(), item);
    }

    @Override
    public V remove(long key) {
        return this.map.remove(key);
    }

    @Override
    public int size() {
        return this.map.size();
    }

    @Override
    public V get(long key) {
        return this.map.get(key);
    }

    @Override
    public V getCeiling(long key) {
        return getValue(this.map.ceilingEntry(key));
    }

    @Override
    public V getFloor(long key) {
        return getValue(this.map.floorEntry(key));
    }

    @Override
    public V getFirst() {
        return getValue(this.map.firstEntry());
    }

    @Override
    public V getLast() {
        return getValue(this.map.lastEntry());
    }

    @Override
    public void forEach(Consumer<V> consumer) {
        this.map.values().forEach(consumer);
    }

    private V getValue(Map.Entry<Long, V> e) {
        return e == null ? null : e.getValue();
    }

    //endregion
}
