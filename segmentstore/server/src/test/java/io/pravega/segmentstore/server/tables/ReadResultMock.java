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

import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Mocks a {@link ReadResult} wrapping a {@link ByteArraySegment} as its source.
 */
@RequiredArgsConstructor
@Getter
class ReadResultMock implements ReadResult {
    //region Members

    private final ByteArraySegment data;
    private final int maxResultLength;
    private final int entryLength;
    private int consumedLength;
    private boolean closed;

    //endregion

    //region Constructor

    ReadResultMock(byte[] data, int maxResultLength, int entryLength) {
        this(new ByteArraySegment(data), maxResultLength, entryLength);
    }

    //endregion

    //region ReadResult Implementation

    @Override
    public long getStreamSegmentStartOffset() {
        return 0;
    }

    @Override
    public void close() {
        this.closed = true;
    }

    @Override
    public boolean hasNext() {
        return this.consumedLength < this.maxResultLength;
    }

    @Override
    public ReadResultEntry next() {
        if (!hasNext()) {
            return null;
        }

        int offset = this.consumedLength;
        int length = Math.min(this.entryLength, this.maxResultLength - offset);
        this.consumedLength += length;
        return new Entry(offset, length);
    }

    //endregion

    //region ReadResultEntry

    @RequiredArgsConstructor
    @Getter
    private class Entry implements ReadResultEntry {
        private final long streamSegmentOffset;
        private final int requestedReadLength;
        private final CompletableFuture<ReadResultEntryContents> content = new CompletableFuture<>();

        @Override
        public ReadResultEntryType getType() {
            return ReadResultEntryType.Cache;
        }

        @Override
        public void requestContent(Duration timeout) {
            this.content.complete(new ReadResultEntryContents(
                    data.getReader((int) this.streamSegmentOffset, this.requestedReadLength),
                    this.requestedReadLength));
        }
    }

    //endregion
}