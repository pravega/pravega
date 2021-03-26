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

import java.util.HashMap;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Wrapper class for {@link HashMap} that performs synchronized copy-on-write operations for any modification of its
 * content. It also provides a method to access the inner {@link HashMap} with the actual contents.
 */
public class CopyOnWriteHashMap<K, V> implements Map<K, V> {

    private volatile Map<K, V> contents;

    public CopyOnWriteHashMap() {
        this.contents = new HashMap<>();
    }

    public CopyOnWriteHashMap(Map<K, V> map) {
        this.contents = Collections.unmodifiableMap(map);
    }

    // Provide access to  the inner map that this class is wrapping.

    public Map<K, V> getInnerMap() {
        return this.contents;
    }

    // End region.

    // Methods mutating internal state.

    @Override
    public synchronized V put(K k, V v) {
        Map<K, V> newMap = new HashMap<>(this.contents);
        V result = newMap.put(k, v);
        this.contents = Collections.unmodifiableMap(newMap);
        return result;
    }

    @Override
    public synchronized void putAll(Map<? extends K, ? extends V> entries) {
        Map<K, V> newMap = new HashMap<>(this.contents);
        newMap.putAll(entries);
        this.contents = Collections.unmodifiableMap(newMap);
    }

    @Override
    public synchronized V putIfAbsent(K k, V v) {
        return (!containsKey(k)) ? put(k, v) : get(k);
    }

    @Override
    public synchronized V remove(Object key) {
        Map<K, V> newMap = new HashMap<>(this.contents);
        V result = newMap.remove(key);
        this.contents = Collections.unmodifiableMap(newMap);
        return result;
    }

    @Override
    public synchronized boolean remove(Object k, Object v) {
        if (containsKey(k) && get(k).equals(v)) {
            remove(k);
            return true;
        }
        return false;
    }

    @Override
    public synchronized boolean replace(K k, V original, V replacement) {
        if (containsKey(k) && get(k).equals(original)) {
            put(k, replacement);
            return true;
        }
        return false;
    }

    @Override
    public synchronized V replace(K k, V v) {
        return (containsKey(k)) ? put(k, v) : null;
    }

    // End region

    // Read-only methods.

    @Override
    public boolean containsKey(Object k) {
        return contents.containsKey(k);
    }

    @Override
    public boolean containsValue(Object v) {
        return contents.containsValue(v);
    }

    @Override
    public Set<java.util.Map.Entry<K, V>> entrySet() {
        return contents.entrySet();
    }

    @Override
    public V get(Object k) {
        return contents.get(k);
    }

    @Override
    public boolean isEmpty() {
        return contents.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return contents.keySet();
    }

    @Override
    public int size() {
        return contents.size();
    }

    @Override
    public Collection<V> values() {
        return contents.values();
    }

    @Override
    public synchronized void clear() {
        this.contents = new HashMap<>();
    }

    // End region

}