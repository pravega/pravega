/**
 * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.byteStream.impl;

import io.pravega.client.byteStream.ByteStreamReader;
import io.pravega.client.byteStream.InvalidOffsetException;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.common.Exceptions;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@RequiredArgsConstructor
@ToString
public class ByteStreamReaderImpl extends ByteStreamReader {

    private final SegmentInputStream input;
    private final AtomicBoolean closed = new AtomicBoolean(false); 

    @Override
    public boolean isOpen() {
        return !closed.get();
    }

    @Override
    public long getOffset() {
        return input.getOffset();
    }

    @Override
    public void jumpToOffset(long offset) throws InvalidOffsetException {
        Exceptions.checkNotClosed(closed.get(), this);
        input.setOffset(offset);
    }

    @Override
    public int available() {
        Exceptions.checkNotClosed(closed.get(), this);
        //Can't replace isSegmentReady because does not handle EOF correctly. Need to have a way to deal with that.
        return input.bytesInBuffer();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            input.close();
        }
    }

    @Override
    public long fetchTailOffset() {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        return input.read(dst, Long.MAX_VALUE);
    }

    @Override
    public int read() throws IOException {
        Exceptions.checkNotClosed(closed.get(), this);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        int read = input.read(buffer, Long.MAX_VALUE);
        if (read > 0) {
            return buffer.get() & 0xFF;
        } else {
            return read;
        }
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(ByteBuffer.wrap(b));
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return read(ByteBuffer.wrap(b, off, len));
    }

    @Override
    public long skip(long toSkip) {
        Exceptions.checkNotClosed(closed.get(), this);
        //TODO: Threadsafety...
        long offset = input.getOffset();
        long endOffset = fetchTailOffset();
        long newOffset = Math.min(offset+toSkip, endOffset);
        input.setOffset(newOffset);
        return newOffset - offset;
    }

    @Override
    public CompletableFuture<Integer> onDataAvailable() {
        Exceptions.checkNotClosed(closed.get(), this);
        return input.fillBuffer().thenApply(v -> available());
    }

}
