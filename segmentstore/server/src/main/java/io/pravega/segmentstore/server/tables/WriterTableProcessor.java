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

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link WriterSegmentProcessor} that handles the asynchronous indexing of Table Entries.
 * TODO: implement this: https://github.com/pravega/pravega/issues/2878
 */
public class WriterTableProcessor implements WriterSegmentProcessor {
    //region Members

    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    WriterTableProcessor() {
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
    }

    //endregion

    //region WriterSegmentProcessor Implementation

    @Override
    public void add(SegmentOperation operation) throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed.get(), this);
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public long getLowestUncommittedSequenceNumber() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean mustFlush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<WriterFlushResult> flush(Duration timeout) {
        throw new UnsupportedOperationException();

    }

    //endregion
}
