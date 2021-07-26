/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.server.reading.CompletableReadResultEntry;
import io.pravega.segmentstore.server.reading.StreamSegmentReadResult;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

/**
 * Mocks a {@link ReadResult} wrapping a {@link ByteArraySegment} as its source.
 */
@Getter
public class ReadResultMock extends StreamSegmentReadResult implements ReadResult {
    //region Members

    private final ArrayView data;
    private final int entryLength;
    private int consumedLength;

    //endregion

    //region Constructor

    public ReadResultMock(byte[] data, int maxResultLength, int entryLength) {
        this(0L, new ByteArraySegment(data), maxResultLength, entryLength);
    }

    public ReadResultMock(long streamSegmentStartOffset, ArrayView data, int maxResultLength, int entryLength) {
        super(streamSegmentStartOffset, maxResultLength, ReadResultMock::noopSupplier, "");
        this.data = data;
        this.entryLength = entryLength;
    }

    private static CompletableReadResultEntry noopSupplier(long startOffset, int remainingLength, boolean copy) {
        throw new UnsupportedOperationException();
    }

    //endregion

    //region ReadResult Implementation

    @Override
    public synchronized boolean hasNext() {
        return this.consumedLength < getMaxResultLength();
    }

    @Override
    public synchronized ReadResultEntry next() {
        if (!hasNext()) {
            return null;
        }

        Preconditions.checkState(isCopyOnRead(), "Copy-on-read required for all Table Segment read requests.");
        int relativeOffset = this.consumedLength;
        int length = Math.min(this.entryLength, Math.min(this.data.getLength(), getMaxResultLength()) - relativeOffset);
        length = Math.min(length, getMaxReadAtOnce());
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
        private final CompletableFuture<BufferView> content = new CompletableFuture<>();

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
            return getStreamSegmentStartOffset() + relativeOffset;
        }

        @Override
        public void requestContent(Duration timeout) {
            if (this.type == ReadResultEntryType.Truncated) {
                this.content.completeExceptionally(new StreamSegmentTruncatedException(getStreamSegmentStartOffset()));
            } else {
                BufferView result = data.slice(this.relativeOffset, this.requestedReadLength);
                if (isCopyOnRead()) {
                    result = new ByteArraySegment(result.getCopy());
                }
                this.content.complete(result);
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