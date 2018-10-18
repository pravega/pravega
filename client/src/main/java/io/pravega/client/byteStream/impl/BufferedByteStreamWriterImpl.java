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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.byteStream.ByteStreamWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class BufferedByteStreamWriterImpl extends ByteStreamWriter {

    @VisibleForTesting
    public static final int BUFFER_SIZE = 4096;
    private final ByteStreamWriterImpl out;

    @GuardedBy("this") //The ref is not guarded but the buffer inside the ref is.
    private final AtomicReference<ByteBuffer> buffer = new AtomicReference<>(null);

    @Override
    public void write(int b) throws IOException {
        synchronized (this) {
            ByteBuffer localBuffer = buffer.get();
            if (localBuffer == null) {
                localBuffer = ByteBuffer.allocate(BUFFER_SIZE);
                buffer.set(localBuffer);
            }
            localBuffer.put((byte) b);
            if (!localBuffer.hasRemaining()) {
                flushBuffer();
            }
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
                ByteBuffer toWrite = buffer.getAndSet(null);
                toWrite.flip();
                if (toWrite.hasRemaining()) {
                    out.write(toWrite);
                }
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
    public long fetchOffset() {
        return out.fetchOffset();
    }

}
