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
import lombok.NonNull;

/**
 * Asynchronously matches read data against a predefined Key.
 */
class KeyMatcher implements AsyncReadResultHandler {
    //region Members

    private final byte[] soughtKey;
    private final EntrySerializer serializer;
    private final TimeoutTimer timer;
    private final EnhancedByteArrayOutputStream readData;
    @Getter
    private final CompletableFuture<EntrySerializer.Header> result;
    private EntrySerializer.Header header;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the KeyMatcher class.
     *
     * @param soughtKey  The Key to search.
     * @param serializer The {@link EntrySerializer} to use.
     * @param timer      Timer for the operation.
     */
    KeyMatcher(@NonNull byte[] soughtKey, @NonNull EntrySerializer serializer, @NonNull TimeoutTimer timer) {
        this.soughtKey = soughtKey;
        this.serializer = serializer;
        this.timer = timer;
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
        Preconditions.checkState(!this.result.isDone(), "KeyMatcher has finished.");
        try {
            assert entry.getContent().isDone() : "received incomplete ReadResultEntry from reader";
            ReadResultEntryContents contents = entry.getContent().join();
            this.readData.write(StreamHelpers.readAll(contents.getData(), contents.getLength()));
            if (this.header == null && this.readData.size() >= EntrySerializer.HEADER_LENGTH) {
                // We now have enough to read the header.
                this.header = this.serializer.readHeader(this.readData.getData());
            }

            if (this.header != null && this.readData.size() >= this.soughtKey.length + EntrySerializer.HEADER_LENGTH) {
                // We read the header, and enough data to verify the Key.
                // TODO: we may want to optimize to verify this as we go.
                ArrayView data = this.readData.getData().subSegment(this.header.getKeyOffset(), this.header.getKeyLength());
                for (int i = 0; i < this.soughtKey.length; i++) {
                    if (this.soughtKey[i] != data.get(i)) {
                        // Key mismatch; no point in continuing.
                        this.result.complete(null);
                        return false;
                    }
                }

                // This entry has the right key. We are done.
                this.result.complete(this.header);
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