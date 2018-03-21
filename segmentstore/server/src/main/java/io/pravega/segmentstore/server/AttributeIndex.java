/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.AsyncMap;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an index for a Segment's Extended Attributes.
 */
public interface AttributeIndex extends AsyncMap<UUID, Long> {
    /**
     * Atomically inserts a collection of attributes into the index (either all Attributes are inserted or none are).
     *
     * @param values  The Attributes to insert.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> put(Map<UUID, Long> values, Duration timeout);

    /**
     * Atomically deletes a collection of Attributes from the index (either all Attributes are deleted or none are).
     *
     * @param keys    A Collection of Attribute Ids to remove.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that all the Attributes have been successfully inserted.
     * If the operation fails, this will complete with the appropriate exception.
     */
    CompletableFuture<Void> remove(Collection<UUID> keys, Duration timeout);

    /**
     * Compacts the Attribute Index into a final Snapshot and seals it, which means it will not accept any further changes.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that the operation completed successfully.
     */
    CompletableFuture<Void> seal(Duration timeout);
}
