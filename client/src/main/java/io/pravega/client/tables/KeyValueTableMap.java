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
package io.pravega.client.tables;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;

/**
 * {@link Map} implementation for a {@link KeyValueTable}'s Key Family.
 * <p>
 * All methods inherited from {@link Map} respect the contracts defined in that interface, except as noted below.
 * <p>
 * Performance considerations:
 * <ul>
 * <li> The {@link Map} interface defines synchronous operations, however {@link KeyValueTable} defines async operations.
 * All method implementations defined in this interface or inherited from {@link Map} invoke
 * {@link CompletableFuture#join()} on any {@link KeyValueTable} methods invoked. This means that two threads are used
 * to fulfill these calls: one from the Executor passed in to the {@link KeyValueTable} factory, and one on which this
 * request is executing.
 *
 * <li> The following operations result in a single call to the wrapped {@link KeyValueTable}:
 * <ul>
 * <li> {@link #containsKey}.
 * <li> {@link #get}, {@link #getOrDefault}.
 * <li> {@link #putDirect}, {@link #putAll}, {@link #putIfAbsent}.
 * <li> {@link #removeDirect}.
 * <li> {@link #isEmpty()} (Invokes {@link KeyValueTable#keyIterator} and requests a single item).
 * <li> {@link #keySet()}: {@link Set#removeAll}, {@link Set#contains}, {@link Set#containsAll}, {@link Set#isEmpty()}.
 * <li> {@link #entrySet()}: {@link Set#contains}, {@link Set#containsAll}, {@link Set#isEmpty()}, {@link Set#add}, {@link Set#addAll}.
 * </ul>
 *
 * <li> The following operations result up to two calls to the wrapped {@link KeyValueTable}:
 * <ul>
 * <li> {@link #put} (refer to {{@link #putDirect} if the old value is not needed).
 * <li> {@link #replace}.
 * <li> {@link #remove} (refer to {@link #removeDirect} if the old value is not needed).
 * <li> {@link #computeIfPresent}, {@link #compute}}, {@link #computeIfAbsent}.
 * <li> {@link #keySet()}{@link Set#remove}.
 * <li> {@link #entrySet()}: {@link Set#removeAll}, {@link Set#isEmpty()}.
 * </ul>
 *
 * <li> The following operations may result in iterating through all the Keys using {@link KeyValueTable#keyIterator}:
 * <ul>
 * <li> {@link #keySet()}{@link Set#iterator()} (Iterates as {@link Iterator#hasNext()} or {@link Iterator#next()} are
 * invoked. Calls to {@link Iterator#remove()} will result in a single call to {@link KeyValueTable}).
 * <li> {@link #size()} (Iterates through the entire iterator).
 * <li> {@link #clear()} (Iterates through the entire iterator and makes repeated calls to {@link KeyValueTable#removeAll}).
 * <li> {@link #keySet()}: {@link Set#clear()}, {@link Set#removeIf}, {@link Set#retainAll}.
 * </ul>
 *
 * <li>The following operations may result in iterating through all the Entries using {@link KeyValueTable#entryIterator}:
 * <ul>
 * <li> {@link #containsValue}.
 * <li> {@link #replaceAll} (which makes multiple calls to {@link KeyValueTable#putAll} as well).
 * <li> {@link #entrySet()}{@link Set#iterator()} (Iterates as {@link Iterator#hasNext()} or {@link Iterator#next()} are
 * invoked. Calls to {@link Iterator#remove()} will result in a single call to {@link KeyValueTable}).
 * <li> {@link #entrySet()}: {@link Set#removeIf}, {@link Set#retainAll}
 * <li> All operations on {@link #values()}.
 * </ul>
 * <li>Invocations of {@link Collection#stream()}}, {@link Collection#spliterator()} or {@link Collection#toArray()} on
 * {@link #keySet()}, {@link #entrySet()} or {@link #values()} will invoke those collections' iterators (see above).
 * </ul>
 * <p>
 * If {@link #getKeyFamily()} is null), the following operations are not supported and will throw throw
 * {@link UnsupportedOperationException}).
 * <ul>
 * <li>{@link #containsValue}.
 * <li>{@link #putAll}.
 * <li>{@link #replaceAll}.
 * <li>{@link #keySet()}.
 * <li>{@link #values()}.
 * <li>{@link #entrySet()}.
 * <li>{@link #size()}.
 * <li>{@link #isEmpty()}.
 * <li>{@link #clear()}.
 * </ul>
 *
 * @param <KeyT>   Table Key Type.
 * @param <ValueT> Table Value Type.
 */
public interface KeyValueTableMap<KeyT, ValueT> extends Map<KeyT, ValueT> {
    /**
     * Gets the Key Family over which this {@link KeyValueTableMap} applies.
     *
     * @return The Key Family, or null if operating over Keys with no Key Family.
     */
    String getKeyFamily();

    /**
     * Same as {@link #put}, but does not attempt to retrieve the existing value. Results in a single call to the wrapped
     * {@link KeyValueTable}.
     *
     * @param key   The Key to insert or update.
     * @param value The value to associate with the key.
     */
    void putDirect(@NonNull KeyT key, @NonNull ValueT value);

    /**
     * Same as {@link #remove}, but does not attempt to retrieve the existing value. Results in a single call to the
     * wrapped {@link KeyValueTable}.
     *
     * @param key The key to remove.
     */
    void removeDirect(@NonNull KeyT key);
}
