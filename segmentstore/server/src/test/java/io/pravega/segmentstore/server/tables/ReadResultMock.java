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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
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

    @Getter
    private final long streamSegmentStartOffset;
    private final ArrayView data;
    private final int maxResultLength;
    private final int entryLength;
    private int consumedLength;
    private boolean closed;

    //endregion

    //region Constructor

    ReadResultMock(byte[] data, int maxResultLength, int entryLength) {
        this(0L, new ByteArraySegment(data), maxResultLength, entryLength);
    }

    //endregion

    //region ReadResult Implementation

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

        int relativeOffset = this.consumedLength;
        int length = Math.min(this.entryLength, Math.min(this.data.getLength(), this.maxResultLength )- relativeOffset);
        this.consumedLength += length;
        return new Entry(relativeOffset, length);
    }

    /**
     * When implemented in a derived class, this will return the Segment's Start Offset (where it is truncated at).
     *
     * @return The Segment's Start Offset.
     */
    protected long getSegmentStartOffset() {
        return 0;
    }

    //endregion

    //region ReadResultEntry

    @Getter
    private class Entry implements ReadResultEntry {
        private final int relativeOffset;
        private final int requestedReadLength;
        private final ReadResultEntryType type;
        private final CompletableFuture<ReadResultEntryContents> content = new CompletableFuture<>();

        Entry(int relativeOffset, int requestedReadLength) {
            this.relativeOffset = relativeOffset;
            this.requestedReadLength = requestedReadLength;
            if (getStreamSegmentOffset() < ReadResultMock.this.getSegmentStartOffset()) {
                this.type = ReadResultEntryType.Truncated;
                this.content.completeExceptionally(new StreamSegmentTruncatedException(ReadResultMock.this.getSegmentStartOffset()));
            } else {
                this.type = this.requestedReadLength == 0 ? ReadResultEntryType.EndOfStreamSegment : ReadResultEntryType.Cache;
            }
        }

        @Override
        public long getStreamSegmentOffset() {
            return streamSegmentStartOffset + relativeOffset;
        }

        @Override
        public void requestContent(Duration timeout) {
            if (this.type == ReadResultEntryType.Truncated) {
                this.content.completeExceptionally(new StreamSegmentTruncatedException(getStreamSegmentStartOffset()));
            } else {
                this.content.complete(new ReadResultEntryContents(
                        data.getReader(this.relativeOffset, this.requestedReadLength),
                        this.requestedReadLength));
            }
        }

        @Override
        public String toString() {
            return String.format("SegmentOffset = %s, RelativeOffset = %s, Length = %s", getStreamSegmentOffset(),
                    this.relativeOffset, this.requestedReadLength);
        }
    }

    //endregion
}