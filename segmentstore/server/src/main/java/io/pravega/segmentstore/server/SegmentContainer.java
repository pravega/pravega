/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.segmentstore.server.logs.MetadataUpdateException;
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
     * Returns a {@link DirectSegmentAccess} object that can be used for operating on a particular StreamSegment directly.
     * The result of this call should only be used for processing a single external request (i.e., coming from over the
     * network) and be discarded afterwards. It should not be cached for long-term access or shared between multiple
     * external requests.
     *
     * If the resulting {@link DirectSegmentAccess} object is cached for more than one request, there is a chance that
     * the underlying Segment may have been evicted from memory. Attempting to use this object at that time will result
     * in a {@link MetadataUpdateException} being thrown for every method call on it. As such, employing such a pattern
     * is strongly discouraged; consider re-invoking this method to obtain a new {@link DirectSegmentAccess} instance
     * every time a new external request is received.
     *
     * Note that it is also possible, though highly unlikely, that the Segment may be evicted from memory from the time
     * this method is invoked until it is needed (even if the calls happen in rapid succession). Even if the initial call
     * does "touch" the Segment (and should prevent it from being evicted in the near future), it is possible that either
     * due to extreme memory pressure or other thread pool contention (in which case the DirectSegmentAccess consumers
     * may be invoked much later) that the Segment may be evicted from memory before it can be used.
     *
     * Example
     * <pre>
     * {@code
     * CompletableFuture<Long> truncateAppendSeal(String segmentName, BufferView data, Duration timeout) {
     *     SegmentContainer container = getSegmentContainer(segmentName);
     *     TimeoutTimer timer = new TimeoutTimer(timeout);
     *     // Get the reference to DirectSegmentAccess here.
     *     return container.forSegment(segmentName, timer.getRemaining())
     *                     .thenCompose(segment -> {
     *                         // Use the DirectSegmentAccess instance repeatedly for multiple Segment operations.
     *                         SegmentProperties properties = segment.getInfo();
     *                         return segment.truncate(properties.getLength(), timer.getRemaining())
     *                                       .thenCompose(ignored -> segment.append(data, null, timer.getRemaining()))
     *                                       .thenCompose(ignored -> segment.seal(timer.getRemaining()));
     *                      });
     *     // Discard the DirectSegmentAccess instance (i.e., release its references so the GC can reclaim it).
     *     }
     * }
     * </pre>
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
