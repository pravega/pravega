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
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines debug segment container for stream segments.
 */
public interface DebugSegmentContainer extends SegmentContainer {

    /**
     * Updates container metadata table with the given details of a segment. This is used during the data recovery
     * process when a segment exists in the long term storage, but not in the durable data log.
     * @param streamSegmentName         Name of the segment to be registered.
     * @param length                    Length of the segment to be registered.
     * @param isSealed                  Sealed status of the segment to be registered.
     * @return                          A CompletableFuture that, when completed normally, will indicate the operation
     * completed. If the operation failed, the future will be failed with the causing exception.
     */
    CompletableFuture<Void> registerSegment(String streamSegmentName, long length, boolean isSealed);

    /**
     * Deletes a segment from segment container metadata along with its details.
     * @param segmentName               Name of the segment to be deleted.
     * @param timeout                   Timeout for the operation.
     * @return                          A CompletableFuture that, when completed normally, will contain a Boolean indicating
     * whether  the Segment has been deleted (true means there was a Segment to delete, false means there was no segment
     * to delete). If the operation failed, this will contain the exception that caused the failure.
     */
    CompletableFuture<Boolean> deleteSegment(String segmentName, Duration timeout);
}
