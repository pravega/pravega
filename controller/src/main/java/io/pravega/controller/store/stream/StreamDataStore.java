/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import java.util.concurrent.CompletableFuture;

/**
 * Stream Metadata.
 */
public interface StreamDataStore extends AutoCloseable {

    /**
     * Gets the event with the segment number.
     *
     * @param routingKey routing Key
     * @param scopeName Scope name
     * @param streamName Stream name
     * @param segmentNumber segment number
     * @return data on success and exception on failure.
     */
    CompletableFuture<String> getEvent(String routingKey,
                                       String scopeName,
                                       String streamName,
                                       Long segmentNumber);

    /**
     * Writes the data to stream as event.
     *
     * @param routingKey routing Key
     * @param scopeName Scope name
     * @param streamName Stream name
     * @param message String name
     * @return null on success and exception on failure.
     */
    CompletableFuture<Void> createEvent(String routingKey,
                                        String scopeName,
                                        String streamName,
                                        String message);
}
