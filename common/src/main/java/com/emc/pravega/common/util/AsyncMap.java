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

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a basic asynchronous Key-Value map.
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
