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

import com.google.common.base.Preconditions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.io.EnhancedByteArrayOutputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;

/**
 * Asynchronously builds Table Entry Keys.
 */
class AsyncTableKeyBuilder implements AsyncReadResultHandler {
    //region Members

    private final TimeoutTimer timer;
    @Getter
    private final CompletableFuture<ArrayView> result;
    private final EnhancedByteArrayOutputStream readData;
    private EntrySerializer.Header header;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncTableKeyBuilder class.
     *
     * @param timer
     */
    AsyncTableKeyBuilder(TimeoutTimer timer) {
        this.timer = Preconditions.checkNotNull(timer, "timer");
        this.result = new CompletableFuture<>();
        this.readData = new EnhancedByteArrayOutputStream();
    }

    //endregion

    //region AsyncReadResultHandler implementation

    @Override
    public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
        return entryType == ReadResultEntryType.Cache || entryType == ReadResultEntryType.Storage;
    }

    @Override
    public boolean processEntry(ReadResultEntry entry) {
        Preconditions.checkState(!this.result.isDone(), "AsyncTableKeyBuilder has finished.");
        try {
            assert entry.getContent().isDone() : "received incomplete ReadResultEntry from reader";
            ReadResultEntryContents contents = entry.getContent().join();
            this.readData.write(StreamHelpers.readAll(contents.getData(), contents.getLength()));
            if (this.header == null && this.readData.size() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = EntrySerializer.readHeader(this.readData.getData());
            }

            if (this.header != null && this.readData.size() >= this.header.getKeyLength() + EntrySerializer.HEADER_LENGTH) {
                // We read the header, and enough data to load the key.
                ArrayView data = this.readData.getData().subSegment(this.header.getKeyOffset(), this.header.getKeyLength());
                this.result.complete(data);
                return false;
            }
        } catch (Throwable ex) {
            processError(ex);
            return false;
        }

        return true;
    }

    @Override
    public void processError(Throwable cause) {
        this.result.completeExceptionally(cause);
    }

    @Override
    public void processResultComplete() {
        // We've reached the end of the read but couldn't find anything.
        if (!this.result.isDone()) {
            this.result.completeExceptionally(new SerializationException("Reached the end of the ReadResult with nothing to show."));
        }
    }

    @Override
    public Duration getRequestContentTimeout() {
        return this.timer.getRemaining();
    }

    //endregion
}