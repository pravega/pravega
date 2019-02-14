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

/**
 * Defines a Factory for DurableLog Components.
 */
public interface OperationLogFactory {

    /**
     * Creates a new instance of an OperationLog class.
     *
     * @param containerMetadata The Metadata for the create the DurableLog for.
     * @param readIndex         A ReadIndex that can be used to store new appends in.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If the metadata is already in recovery mode.
     * @return The Sequential Log object.
     */
    OperationLog createDurableLog(UpdateableContainerMetadata containerMetadata, ReadIndex readIndex);
}
