/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.storage;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Defines a Storage that allows truncating Segments.
 */
public interface TruncateableStorage extends Storage {

    /**
     * Truncates all data in the given StreamSegment prior to the given offset. This does not fill the truncated data
     * in the segment with anything, nor does it "shift" the remaining data to the beginning. After this operation is
     * complete, any attempt to access the truncated data will result in an exception.
     *
     * @param streamSegmentName The full name of the StreamSegment.
     * @param offset            The offset in the StreamSegment to truncate to.
     * @param timeout           Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation succeeded. If the operation failed,
     * it will contain the cause of the failure. Notable exceptions:
     * <ul>
     * <li> StreamSegmentNotExistsException: When the given Segment does not exist in Storage.
     * </ul>
     */
    CompletableFuture<Void> truncate(String streamSegmentName, long offset, Duration timeout);
}
