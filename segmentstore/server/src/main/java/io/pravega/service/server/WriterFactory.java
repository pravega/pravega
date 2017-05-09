/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server;

import io.pravega.service.storage.Storage;

/**
 * Defines a Factory for Writers.
 */
public interface WriterFactory {
    /**
     * Creates a new Writer with given arguments.
     *
     * @param containerMetadata Metadata for the container that this Writer will be for.
     * @param operationLog      The OperationLog to attach to.
     * @param readIndex         The ReadIndex to attach to (to provide feedback for mergers).
     * @param storage           The Storage adapter to use.
     * @return An instance of a class that implements the Writer interface.
     */
    Writer createWriter(UpdateableContainerMetadata containerMetadata, OperationLog operationLog, ReadIndex readIndex, Storage storage);
}
