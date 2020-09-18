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
import io.pravega.segmentstore.storage.Storage;

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
     * Creates a segment with target segment name and copies the contents of the source segment to the target segment.
     * @param storage                   A storage instance to create the segment.
     * @param sourceSegment             The name of the source segment to copy the contents from.
     * @param targetSegment             The name of the segment to write the contents to.
     * @throws Exception                In case of an exception occurred while execution.
     */
    void copySegment(Storage storage, String sourceSegment, String targetSegment) throws Exception;
}
