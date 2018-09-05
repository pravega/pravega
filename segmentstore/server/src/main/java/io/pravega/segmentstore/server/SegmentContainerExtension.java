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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an Extension that can be associated with a {@link SegmentContainer}, which will be instantiated (and initialized)
 * when the {@link SegmentContainer} is initialized, and closed when the {@link SegmentContainer} is closed.
 */
public interface SegmentContainerExtension extends AutoCloseable {
    @Override
    void close();

    /**
     * Performs any initialization required for this SegmentContainerExtension instance.
     * @return A CompletableFuture that, when completed, will indicate that the initialization is done.
     */
    CompletableFuture<Void> initialize();

    /**
     * Creates a Collection of any additional WriterSegmentProcessors to pass to the StorageWriter.
     * @param metadata The Metadata of the Segment to create Writer Processors for.
     * @return A Collection containing the desired processors. If none are required, this should return an empty list.
     */
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata);
}
