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
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

/**
 * Helps builds Table Entry Keys asynchronously by processing successive {@link ReadResultEntry}. This can be used to
 * read serialized Keys from a Segment starting at the Entry serialization offset but without knowing the length of the
 * Key.
 *
 * NOTE: this class is meant to be used as an {@link AsyncReadResultHandler} by an {@link AsyncReadResultProcessor}, which
 * processes it in a specific sequence using its own synchronization primitives. As such this class should be considered
 * thread-safe when used in such a manner, but no guarantees are made if it is used otherwise.
 */
class AsyncTableKeyBuilder implements AsyncReadResultHandler {
    //region Members

    private final EntrySerializer serializer;
    private final TimeoutTimer timer;
    @Getter
    private final CompletableFuture<ArrayView> result;
    private final EnhancedByteArrayOutputStream readData;
    private EntrySerializer.Header header;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncTableKeyBuilder class.
     * @param serializer The {@link EntrySerializer} to use for deserializing the Keys.
     * @param timer Timer for the whole operation.
     */
    AsyncTableKeyBuilder(@NonNull EntrySerializer serializer, @NonNull TimeoutTimer timer) {
        this.serializer = serializer;
        this.timer = timer;
        this.result = new CompletableFuture<>();
        this.readData = new EnhancedByteArrayOutputStream();
    }

    //endregion

    //region AsyncReadResultHandler implementation

    @Override
    public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
        // We only care about actual data, and the data must have been written. So Cache and Storage are the only entry
        // types we process.
        return entryType == ReadResultEntryType.Cache || entryType == ReadResultEntryType.Storage;
    }

    @Override
    public boolean processEntry(ReadResultEntry entry) {
        Preconditions.checkState(!this.result.isDone(), "AsyncTableKeyBuilder has finished.");
        try {
            Preconditions.checkArgument(entry.getContent().isDone(), "Entry Contents is not yet fetched.");
            ReadResultEntryContents contents = entry.getContent().join();

            // TODO: most of these transfers are from memory to memory. It's a pity that we need an extra buffer to do the copy.
            // TODO: https://github.com/pravega/pravega/issues/2924
            this.readData.write(StreamHelpers.readAll(contents.getData(), contents.getLength()));

            if (this.header == null && this.readData.size() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = this.serializer.readHeader(this.readData.getData());
            }

            if (this.header != null && this.readData.size() >= this.header.getKeyLength() + EntrySerializer.HEADER_LENGTH) {
                // We read the header, and enough data to load the key. We are done.
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
        if (!this.result.isDone()) {
            // We've reached the end of the read but couldn't find anything.
            this.result.completeExceptionally(new SerializationException("Reached the end of the ReadResult but unable to load a key."));
        }
    }

    @Override
    public Duration getRequestContentTimeout() {
        return this.timer.getRemaining();
    }

    //endregion
}