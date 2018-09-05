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
import io.pravega.common.io.SerializationException;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.reading.AsyncReadResultHandler;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import lombok.NonNull;

/**
 * Asynchronously builds Table Entry Values.
 */
class AsyncTableEntryBuilder implements AsyncReadResultHandler {
    //region Members

    private final ArrayView key;
    private final byte[] data;
    private final long version;
    private final TimeoutTimer timer;
    /**
     * A Completable Future that, when completed, will contain the TableStore.KeyValue read.
     */
    @Getter
    private final CompletableFuture<TableEntry> result;
    private int readBytes;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AsyncTableEntryBuilder class.
     *
     * @param valueLength The length of the data to read.
     * @param version     The version of this entry.
     * @param timer       TimeoutTimer that indicates how much time is left for the operation to complete.
     */
    AsyncTableEntryBuilder(@NonNull ArrayView key, int valueLength, long version, @NonNull TimeoutTimer timer) {
        this.key = key;
        this.data = new byte[valueLength];
        this.version = version;
        this.timer = timer;
        this.result = new CompletableFuture<>();
        this.readBytes = 0;
    }

    //endregion

    //region AsyncReadResultHandler implementation

    @Override
    public boolean shouldRequestContents(ReadResultEntryType entryType, long streamSegmentOffset) {
        return entryType == ReadResultEntryType.Cache || entryType == ReadResultEntryType.Storage;
    }

    @Override
    public boolean processEntry(ReadResultEntry entry) {
        Preconditions.checkState(!this.result.isDone(), "TableEntry has been built.");
        try {
            assert entry.getContent().isDone() : "received incomplete ReadResultEntry from reader";
            ReadResultEntryContents contents = entry.getContent().join();
            int toCopy = Math.min(this.data.length - this.readBytes, contents.getLength());
            int copiedBytes = StreamHelpers.readAll(contents.getData(), this.data, this.readBytes, toCopy);
            assert copiedBytes == toCopy;
            this.readBytes += copiedBytes;
            if (this.readBytes >= this.data.length) {
                this.result.complete(TableEntry.versioned(this.key, new ByteArraySegment(this.data), this.version));
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