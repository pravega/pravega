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
 *
 * Segment Container Extensions are useful when we want to add additional functionality to a particular Segment Container
 * without touching the core code or getting access to the internals of the Container. Additionally, by having the same
 * lifecycle as the Container itself, these Extensions can be automatically created and cleaned up when the Container
 * starts or is stopped, so no additional tracking is necessary.
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
     * Creates a Collection of any {@link WriterSegmentProcessor} to pass to the {@link SegmentContainer}'s StorageWriter.
     * These processors will be associated with a particular segment and will process all Operations that are queued up
     * for that particular Segment (similarly to the SegmentAggregator).
     *
     * @param metadata The Metadata of the Segment to create Writer Processors for.
     * @return A Collection containing the desired processors. If none are required, this should return an empty list.
     */
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata);
}
