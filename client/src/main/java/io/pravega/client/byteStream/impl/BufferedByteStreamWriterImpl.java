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

import io.pravega.client.byteStream.ByteStreamWriter;
import io.pravega.common.concurrent.Futures;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BufferedByteStreamWriterImpl extends ByteStreamWriter {

    private final ByteStreamWriterImpl out;

    @GuardedBy("this") //The ref is not guarded but the buffer inside the ref is.
    private final AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);

    private ByteBuffer getBuffer() {
        ByteBuffer result = buffer.get();
        if (result == null) {
            synchronized (this) {
                result = buffer.get();
                if (result == null) {
                    result = ByteBuffer.allocate(4096);
                    buffer.set(result);
                }
            }
        }
        return result;
    }

    @Override
    public void write(int b) throws IOException {
        ByteBuffer localBuffer = getBuffer();
        synchronized (this) {
            if (!localBuffer.hasRemaining()) {
                flushBuffer();
            }
            localBuffer.put((byte) b);
        }
    }

    @Override
    public void write(ByteBuffer src) throws IOException {
        flushBuffer();
        out.write(src);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        flushBuffer();
        out.write(b, off, len);
    }

    private void flushBuffer() throws IOException {
        if (buffer.get() != null) {
            synchronized (this) {
                ByteBuffer toWrite = buffer.get();
                toWrite.flip();
                if (toWrite.hasRemaining()) {
                    out.write(toWrite);
                }
                buffer.set(ByteBuffer.allocate(4096));
            }
        }
    }

    
    @Override
    public void close() throws IOException {
        flushBuffer();
        out.close();
    }

    @Override
    public void flush() throws IOException {
        flushBuffer();
        out.flush();
    }

    @Override
    public void closeAndSeal() throws IOException {
        flushBuffer();
        out.closeAndSeal();
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        try {
            flushBuffer();
            return out.flushAsync();
        } catch (IOException e) {
            return Futures.failedFuture(e);
        }
    }

    @Override
    public long fetchOffset() {
        return out.fetchOffset();
    }

}
