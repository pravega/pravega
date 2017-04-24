/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.service.storage.mocks;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Storage that can register various segment-level event triggers.
 */
public interface ListenableStorage {
    /**
     * Registers a size trigger for the given Segment Name and Offset.
     *
     * @param segmentName The Name of the Segment.
     * @param offset      The offset in the segment at which to trigger.
     * @param timeout     The timeout for the wait.
     * @return A CompletableFuture that will complete when the given Segment reaches at least the given minimum size.
     * This Future will fail with a TimeoutException if the Segment did not reach the minimum size within the given timeout.
     */
    CompletableFuture<Void> registerSizeTrigger(String segmentName, long offset, Duration timeout);

    /**
     * Registers a seal trigger for the given Segment Name.
     *
     * @param segmentName The Name of the Segment.
     * @param timeout     The timeout for the wait.
     * @return A CompletableFuture that will complete when the given Segment is sealed. This Future will fail with a TimeoutException
     * if the Segment was not sealed within the given timeout.
     */
    CompletableFuture<Void> registerSealTrigger(String segmentName, Duration timeout);
}
