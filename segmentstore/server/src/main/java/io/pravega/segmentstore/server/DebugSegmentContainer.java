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
import java.util.concurrent.CompletableFuture;

/**
 * Defines debug segment container for stream segments.
 */
public interface DebugSegmentContainer extends SegmentContainer {

    /**
     * Updates container metadata table by creating a segment with the given details. This is used during the data recovery
     * process when a segment exists in the long term storage, but not in the durable data log.
     * @param streamSegmentName         Name of the segment to be created.
     * @param length                    Length of the segment to be created.
     * @param isSealed                  Sealed status of the segment to be created.
     * @return                          A newly created segment.
     */
    CompletableFuture<Void> registerExistingSegment(String streamSegmentName, long length, boolean isSealed);
}
