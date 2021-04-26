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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Lightweight, thread-safe, key-value pair cache built on top of Java {@link HashMap} that supports eviction based on
 * a maximum size or last-access-time.
 *
 * Eviction of items is triggered by one of the following:
 * - Invoking {@link #cleanUp()}.
 * - Invoking {@link #putIfAbsent}, {@link #put} or {@link #get} for a Key that has expired or if the {@link #size()}
 * of the cache has exceeded {@link #getMaxSize()}.
 *
 * Every item access (upon insertion, updating or retrieval) will update the "last-access-time" of that item to the current
 * time. Upon eviction, items will be evicted beginning with the least-accessed one (i.e, the ones that have not been used
 * recently), in order of access time (oldest items first). Every eviction event will remove all expired items and any
 * unexpired items as needed if {@link #size()} exceeds {@link #getMaxSize()}.
 *
 * @param <KeyT>   Key Type.
 * @param <ValueT> Value Type.
 */
@ThreadSafe
@Slf4j
public class SimpleCache<KeyT, ValueT> {
    //region Members

    @GuardedBy("lock")
    private final HashMap<KeyT, Entry<KeyT, ValueT>> map;
    @Getter
    private final int maxSize;
    private final long expirationTimeNanos;
    private final BiConsumer<KeyT, ValueT> onExpiration;
    private final Supplier<Long> currentTime;
    @GuardedBy("lock")
    private Entry<KeyT, ValueT> leastRecent;
    @GuardedBy("lock")
    private Entry<KeyT, ValueT> mostRecent;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link SimpleCache} class.
     *
     * @param maxSize        Maximum number of elements in the cache. If {@link #size()} exceeds this value as a result
     *                       of an insertion, the oldest entries will be evicted, in order, until {@link #size()} falls
     *                       below this value.
     * @param expirationTime Maximum amount of time, since the last access of an entry, until such an entry is "expired"
     *                       and will be evicted. Entries will not be evicted exactly when they expire, rather upon calls
     *                       to {@link #cleanUp()} or any other accessor or mutator invocations.
     * @param onExpiration   (Optional) A {@link Consumer} that will be invoked when an Entry is evicted. This will not be
     *                       invoked when the entry is replaced (i.e., via {@link #put} or removed (via {@link #remove}.
     */
    public SimpleCache(int maxSize, @NonNull Duration expirationTime, @Nullable BiConsumer<KeyT, ValueT> onExpiration) {
        this(maxSize, expirationTime, onExpiration, System::nanoTime);
    }

    /**
     * Creates a new instance of the {@link SimpleCache} class for testing purposes.
     *
     * @param maxSize        See {@link #SimpleCache(int, Duration, BiConsumer)}.
     * @param expirationTime See {@link #SimpleCache(int, Duration, BiConsumer)}.
     * @param onExpiration   See {@link #SimpleCache(int, Duration, BiConsumer)}.
     * @param currentTime    A {@link Supplier} that will return the current time, expressed in nanoseconds.
     */
    @VisibleForTesting
    SimpleCache(int maxSize, @NonNull Duration expirationTime, @Nullable BiConsumer<KeyT, ValueT> onExpiration,
                @NonNull Supplier<Long> currentTime) {
        Preconditions.checkArgument(maxSize > 0, "maxSize must be a positive number.");
        this.expirationTimeNanos = expirationTime.toNanos();
        Preconditions.checkArgument(this.expirationTimeNanos > 0, "expirationTime must be a positive duration.");
        this.map = new HashMap<>();
        this.onExpiration = onExpiration;
        this.currentTime = currentTime;
        this.maxSize = maxSize;
        this.leastRecent = null;
        this.mostRecent = null;
    }

    //endregion

    //region Methods

    /**
     * Gets a value representing the number of entries in the cache. This may include expired entries as well (expired
     * entries will be removed upon invoking {@link #cleanUp()}, which will also update this value).
     *
     * @return The number of entries in the cache.
     */
    public int size() {
        synchronized (this.lock) {
            return this.map.size();
        }
    }

    /**
     * Inserts a new Key-Value pair into the cache.
     *
     * @param key   The key to insert.
     * @param value The value to insert.
     * @return The previous value associated with this key, or null if no such value exists or if it is expired.
     */
    public ValueT put(KeyT key, ValueT value) {
        val e = new Entry<KeyT, ValueT>(key, value);
        e.lastAccessTime = this.currentTime.get();
        Entry<KeyT, ValueT> prevValue;
        synchronized (this.lock) {
            prevValue = this.map.put(key, e);
            if (prevValue != null) {
                // Replacement.
                if (isExpired(prevValue, e.lastAccessTime)) {
                    // Expired previous value is equivalent to it not having existed in the first place
                    prevValue.replaced = true; // cleanup will take care of expired entries.
                    prevValue = null;
                } else {
                    // Not expired. We need to manually unlink it.
                    unregister(prevValue);
                }
            }

            // Insertion.
            register(e);
        }

        if (prevValue == null) {
            // We have made an insertion. Clean up if necessary.
            cleanUp();
            return null;
        }

        return prevValue.value;
    }

    /**
     * Inserts a Key-Value pair into the cache, but only if the Key is not already present.
     *
     * @param key   The key to insert.
     * @param value The value to insert.
     * @return The value that was already associated with this key. If no such value, or if the value is expired, then
     * null will be returned. If null is returned, then the insertion can be considered successful.
     */
    public ValueT putIfAbsent(KeyT key, ValueT value) {
        val e = new Entry<KeyT, ValueT>(key, value);
        e.lastAccessTime = this.currentTime.get();
        Entry<KeyT, ValueT> prevValue;
        synchronized (this.lock) {
            prevValue = this.map.putIfAbsent(key, e);
            if (prevValue != null && isExpired(prevValue, e.lastAccessTime)) {
                // Key exists, but entry is expired; we are eligible for insertion.
                this.map.put(key, e);
                prevValue.replaced = true;
                prevValue = null;
            }

            if (prevValue == null) {
                // Insertion successful. Update mostRecent and leastRecent.
                register(e);
            }
        }

        if (prevValue == null) {
            // We have made an insertion. Clean up if necessary.
            cleanUp();
            return null;
        }

        return prevValue.value;
    }

    /**
     * Removes any value that is associated with the given key.
     *
     * @param key The key to remove.
     * @return The value associated with the given key, or null if no such value exists or if the value is expired.
     */
    public ValueT remove(KeyT key) {
        final Entry<KeyT, ValueT> e;
        final long currentTime = this.currentTime.get();
        synchronized (this.lock) {
            e = this.map.remove(key);
            if (e != null) {
                // Removal successful. Remove this entry from our chain (a cleanup may not catch it until it actually was
                // set to expire, so we'll have to do it this way).
                unregister(e);
                if (isExpired(e, currentTime)) {
                    return null;
                }
            }
        }

        return e == null ? null : e.value;
    }

    /**
     * Gets a value associated with the given key.
     *
     * @param key The key to lookup.
     * @return The value associated with the given key, or null if no such value exists or if the value is expired. If
     * the value is expired, it will be automatically removed from the cache.
     */
    public ValueT get(KeyT key) {
        Entry<KeyT, ValueT> e;
        boolean needsEviction = false;
        final long currentTime = this.currentTime.get();
        synchronized (this.lock) {
            e = this.map.get(key);
            if (e != null) {
                if (isExpired(e, currentTime)) {
                    // No need to do anything. We'll run cleanUp(), which will remove it anyway.
                    needsEviction = true;
                    e = null;
                } else {
                    // Not expired. Update its last access time and set it as most recent.
                    e.lastAccessTime = currentTime;
                    unregister(e);
                    register(e);
                }
            }
        }

        if (needsEviction) {
            cleanUp();
            return null;
        }

        return e == null ? null : e.value;
    }

    /**
     * Performs a cleanup of this class. After this method completes, the following will be true:
     * - {@link #size()} will be less than or equal to {@link #getMaxSize()}.
     * - All expired entries will be removed.
     */
    public void cleanUp() {
        final long currentTime = this.currentTime.get();
        Entry<KeyT, ValueT> lastEvicted;
        synchronized (this.lock) {
            Entry<KeyT, ValueT> current = this.leastRecent;
            while (current != null && (isExpired(current, currentTime) || this.map.size() > this.maxSize)) {
                if (!current.replaced) {
                    this.map.remove(current.key);
                }
                current = current.next;
            }

            this.leastRecent = current;
            if (current == null) {
                // Evict all.
                lastEvicted = this.mostRecent;
                this.mostRecent = null;
            } else {
                // current points to the first Value that remains.
                lastEvicted = current.prev;
                if (lastEvicted != null) {
                    lastEvicted.next = null;
                    current.prev = null;
                }
            }
        }

        // Run eviction callbacks (outside of the sync block).
        if (this.onExpiration != null) {
            while (lastEvicted != null) {
                try {
                    this.onExpiration.accept(lastEvicted.key, lastEvicted.value);
                } catch (Throwable ex) {
                    // Log and move on. There is no way we can handle this anyway here, and this shouldn't prevent us
                    // from invoking it for subsequent entries or fail whatever called us anyway.
                    log.error("Eviction callback for {} failed.", lastEvicted.key, ex);
                }

                lastEvicted = lastEvicted.prev;
            }
        }
    }

    /**
     * Gets the unexpired Entries within the cache, in order (from least used to most recent used).
     *
     * @return A list.
     */
    @VisibleForTesting
    List<Map.Entry<KeyT, ValueT>> getUnexpiredEntriesInOrder() {
        val result = new ArrayList<Map.Entry<KeyT, ValueT>>();
        synchronized (this.lock) {
            Entry<KeyT, ValueT> current = this.leastRecent;
            val currentTime = this.currentTime.get();
            while (current != null) {
                if (!isExpired(current, currentTime)) {
                    result.add(new AbstractMap.SimpleImmutableEntry<>(current.key, current.value));
                }
                current = current.next;
            }
        }
        return result;
    }

    //endregion

    //region Helpers

    /**
     * Unlinks the given Entry from the chain and updates {@link #leastRecent} and {@link #mostRecent} if necessary.
     *
     * @param v The Entry to unlink.
     */
    private void unregister(Entry<KeyT, ValueT> v) {
        // Update leastRecent and mostRecent.
        if (this.leastRecent == v) {
            this.leastRecent = v.next;
        }
        if (this.mostRecent == v) {
            this.mostRecent = v.prev;
        }

        // Unlink the Entry from its adjacent neighbors.
        if (v.prev != null) {
            v.prev.next = v.next;
        }

        if (v.next != null) {
            v.next.prev = v.prev;
        }

        v.prev = null;
        v.next = null;

        // Sanity checks.
        assert this.leastRecent == null || this.leastRecent.prev == null;
        assert this.mostRecent == null || this.mostRecent.next == null;
    }

    /**
     * Updates {@link #mostRecent} to the given Entry and creates the appropriate links.
     * May also update {@link #leastRecent} if previously null.
     *
     * @param e The Entry.
     */
    private void register(Entry<KeyT, ValueT> e) {
        e.prev = this.mostRecent;
        if (this.mostRecent != null) {
            this.mostRecent.next = e;
        }

        this.mostRecent = e;
        if (this.leastRecent == null) {
            this.leastRecent = e;
        }

        // Sanity checks.
        assert this.map.size() > 0 && this.leastRecent.prev == null && this.mostRecent.next == null;
    }

    /**
     * Determines whether the given entry is expired.
     *
     * @param e           The Entry.
     * @param currentTime The current time.
     * @return True if expired, false otherwise.
     */
    private boolean isExpired(Entry<KeyT, ValueT> e, long currentTime) {
        return currentTime - e.lastAccessTime > this.expirationTimeNanos;
    }

    //endregion

    //region Entry

    @RequiredArgsConstructor
    @NotThreadSafe
    private static class Entry<KeyT, ValueT> {
        final KeyT key;
        final ValueT value;
        long lastAccessTime;
        Entry<KeyT, ValueT> prev;
        Entry<KeyT, ValueT> next;
        boolean replaced = false;

        @Override
        public String toString() {
            return String.format("%s -> %s", this.key, this.value);
        }
    }

    //endregion
}
