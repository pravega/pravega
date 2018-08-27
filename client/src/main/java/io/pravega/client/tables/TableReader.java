/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that can be used to access a Table for reading purposes.
 *
 * @param <KeyT>   Table Key type.
 * @param <ValueT> Table Value type.
 */
public interface TableReader<KeyT, ValueT> extends AutoCloseable {
    /**
     * Gets the latest value for the given Key.
     *
     * @param key The Key to get the value for.
     * @return A CompletableFuture that, when completed, will contain the requested result. If no such Key exists, this
     * will be completed with a null value.
     */
    CompletableFuture<GetResult> get(KeyT key);

    /**
     * Gets the latest values for the given Keys.
     *
     * @param keys A list of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a list of results for the given Keys. The results
     * will be in the same order as the keys. For any key that does not exist, a null value will be at its corresponding
     * index.
     */
    CompletableFuture<List<GetResult<ValueT>>> get(List<KeyT> keys);

    /**
     * Registers a listener for all updates to a single key.
     *
     * @param listener The listener to register.
     * @return A CompletableFuture that, when completed, will indicate the listener has been registered.
     */
    CompletableFuture<Void> registerListener(KeyUpdateListener<KeyT, ValueT> listener);

    /**
     * Unregisters a registered listener.
     *
     * @param listener THe listener to unregister.
     */
    void unregisterListener(KeyUpdateListener<KeyT, ValueT> listener);
}
