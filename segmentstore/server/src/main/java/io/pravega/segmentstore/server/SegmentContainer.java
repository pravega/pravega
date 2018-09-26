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

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Container for StreamSegments.
 */
public interface SegmentContainer extends StreamSegmentStore, Container {
    /**
     * Gets a collection of SegmentProperties for all active Segments (Active Segment = a segment that is currently allocated
     * in the internal Container's Metadata (usually a segment with recent activity)).
     *
     * @return A Collection with the SegmentProperties for these segments.
     */
    Collection<SegmentProperties> getActiveSegments();

    /**
     * Returns a DirectSegmentAccess object that can be used for operating on a particular StreamSegment directly. The
     * result of this call should only be used for short periods of time (i.e., while processing a single request) and
     * not be cached for long-term access.
     *
     * @param streamSegmentName The name of the StreamSegment to get for.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the desired result. If the operation
     * failed, the future will be failed with the causing exception.
     */
    CompletableFuture<DirectSegmentAccess> forSegment(String streamSegmentName, Duration timeout);

    /**
     * Gets a registered {@link SegmentContainerExtension} of the given type.
     *
     * @param extensionClass Class of the {@link SegmentContainerExtension}.
     * @param <T>         Type of the {@link SegmentContainerExtension}.
     * @return A registered {@link SegmentContainerExtension} of the requested type.
     */
    <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass);
}
