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

import com.google.common.annotations.Beta;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Defines all operations that can be used to access a Table for reading purposes.
 *
 * @param <KeyT>   Table Key type.
 * @param <ValueT> Table Value type.
 */
@Beta
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
     * @param keys A Collection of Keys to get values for.
     * @return A CompletableFuture that, when completed, will contain a map of {@link KeyT} to {@link GetResult} for those
     * keys that have a value in the index. All other keys will not be included.
     */
    CompletableFuture<Map<KeyT, GetResult<ValueT>>> get(Collection<KeyT> keys);
}
