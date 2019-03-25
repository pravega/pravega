/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class TableCompactor {
    @NonNull
    private final TableWriterConnector connector;

    public CompletableFuture<Void> compactIfNeeded(DirectSegmentAccess segment, Duration timeout) {
        // 1. Get Table Segment State and decide if compaction is needed.
        // 2. Start reading Table Entries from the beginning (StartOffset) of the segment)
        // 2.1. If Key is not in index or exists with higher version/offset, skip over. This will include removals.
        // 2.2. If Key exists in index at the current offset, collect (into update collection).
        // 3. When enough entries have been processed (config based):
        // 3.1. execute conditional update to re-apply old keys,
        // 3.2. Update Table Attributes and Truncate at the latest offset (atomically).
        return CompletableFuture.completedFuture(null);
    }
}
