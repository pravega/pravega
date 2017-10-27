/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import java.time.Duration;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;

public class StreamSegmentStorageReader {
    public static StreamSegmentReadResult read(SegmentProperties segmentInfo, long startOffset, int maxReadLength, int readBlockSize, ReadOnlyStorage storage) {
        Exceptions.checkArgument(startOffset >= 0, "startOffset", "startOffset must be a non-negative number.");
        Exceptions.checkArgument(maxReadLength >= 0, "maxReadLength", "maxReadLength must be a non-negative number.");
        Preconditions.checkNotNull(segmentInfo, "segmentInfo");
        Preconditions.checkNotNull(storage, "storage");
        String traceId = String.format("Read[%s]", segmentInfo.getName());
        return new StreamSegmentReadResult(startOffset, maxReadLength, new SegmentReader(segmentInfo, readBlockSize, storage), traceId);
    }

    @RequiredArgsConstructor
    private static class SegmentReader implements StreamSegmentReadResult.NextEntrySupplier {
        private final SegmentProperties segmentInfo;
        private final int readBlockSize;
        private final ReadOnlyStorage storage;

        @Override
        public CompletableReadResultEntry apply(Long readOffset, Integer readLength) {
            if (readOffset < this.segmentInfo.getStartOffset()) {
                // We attempted to read from a truncated portion of the Segment.
                return new TruncatedReadResultEntry(readOffset, readLength, this.segmentInfo.getStartOffset());
            }

            // Execute the read from Storage.
            return new StorageReadResultEntry(readOffset, readLength, this::fetchContents);
        }

        void fetchContents(long streamSegmentOffset, int requestedReadLength, Consumer<ReadResultEntryContents> successCallback,
                           Consumer<Throwable> failureCallback, Duration timeout) {
            // TODO: implement
        }
    }

}
