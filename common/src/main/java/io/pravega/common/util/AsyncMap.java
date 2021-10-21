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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a basic asynchronous Key-Value map which allows adding, getting and removing items. The purpose of this is to
 * abstract a non-memory backed store that may take a while to perform operations (for example, if disk or a network
 * resource is used as a store, significant latency may be part of every operation).
 * <p>
 * This contract definition does not make any guarantees as to the contents' consistency or concurrent access behavior.
 * Please refer to the actual implementation's documentation for such behavior.
 *
 * @param <K> Type of the Keys.
 * @param <V> Type of the Values.
 */
public interface AsyncMap<K, V> {
    /**
     * Inserts a new Key-Value pair into the map.
     *
     * @param key     The Key to insert/update.
     * @param value   The Value to insert/update.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the Key-Value pair has been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> put(K key, V value, Duration timeout);

    /**
     * Retrieves a value from the Map.
     *
     * @param key     The key to retrieve the value for.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the requested value. If the operation fails, this
     * will complete with the appropriate exception.
     */
    CompletableFuture<V> get(K key, Duration timeout);

    /**
     * Deletes a Key-Value pair from the Map, if any.
     *
     * @param key     The Key of the pair to remove.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the Key has been successfully removed. If the
     * operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> remove(K key, Duration timeout);
}
