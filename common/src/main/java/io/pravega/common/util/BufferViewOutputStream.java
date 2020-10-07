/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.Exceptions;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class BufferViewOutputStream extends OutputStream {
    
    @VisibleForTesting
    static final int CHUNK_SIZE = 1024;
    
    private final List<BufferView> completed = new ArrayList<BufferView>();
    private byte[] currentChunk;
    private int bytesWrittenInChunk = 0;
    
    BufferViewOutputStream() {
        this.currentChunk = new byte[CHUNK_SIZE];
    }

    /**
     * Writes a single byte to this output stream. The general
     * contract for <code>write</code> is that one byte is written
     * to the output stream. The byte to be written is the eight
     * low-order bits of the argument <code>b</code>. The 24
     * high-order bits of <code>b</code> are ignored.
     * <p>
     * This exists only because subclasses of <code>OutputStream</code> must provide an
     * implementation for this method.
     *
     * @param      b   the <code>byte</code>.
     */
    @Override
    public void write(int b) {
        Exceptions.checkNotClosed(isClosed(), this);
        allocateNewChunkIfNeeded(1);
        currentChunk[bytesWrittenInChunk] = (byte) b;
        bytesWrittenInChunk++;
    }

    private final void allocateNewChunkIfNeeded(int i) {
        if (bytesWrittenInChunk + i > currentChunk.length) {
            completeChunk();
        }
    }

    private final void completeChunk() {
        completed.add(new ByteArraySegment(currentChunk, 0, bytesWrittenInChunk));
        currentChunk = new byte[CHUNK_SIZE];
        bytesWrittenInChunk = 0;
    }

    /**
     * Writes <code>b.length</code> bytes from the specified byte array
     * to this output stream. 
     *
     * @param      b   the data.
     * @see        java.io.OutputStream#write(byte[], int, int)
     */
    @Override
    public void write(byte b[]) {
        write(b, 0, b.length);
    }
    
    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this output stream.
     * 
     * This implementation will always write the whole of the array.
     * 
     * <p>
     * If <code>off</code> is negative, or <code>len</code> is negative, or
     * <code>off+len</code> is greater than the length of the array
     * {@code b}, then an {@code IndexOutOfBoundsException} is thrown.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     */
    @Override
    public void write(byte b[], int off, int len) {
        Exceptions.checkNotClosed(isClosed(), this);
        Objects.checkFromIndexSize(off, len, b.length);
        if (bytesWrittenInChunk + len > currentChunk.length) {
            if (bytesWrittenInChunk > 0) {
                completed.add(new ByteArraySegment(currentChunk, 0, bytesWrittenInChunk));
                currentChunk = new byte[CHUNK_SIZE];
                bytesWrittenInChunk = 0;
            }
            completed.add(new ByteArraySegment(b, off, len));
        } else {
            System.arraycopy(b, off, currentChunk, bytesWrittenInChunk, len);
            bytesWrittenInChunk += len;
        }
    }

    private boolean isClosed() {
        return currentChunk == null;
    }
    
    /**
     * Does nothing.
     */
    @Override
    public void flush() {
    }

    /**
     * Prevents further writes.
     */
    @Override
    public void close() {
        if (currentChunk != null) {
            if (bytesWrittenInChunk > 0) {
                completed.add(new ByteArraySegment(currentChunk, 0, bytesWrittenInChunk));
            }
            currentChunk = null;
            bytesWrittenInChunk = -1;
        }
    }
    
    public BufferView getView() {
        if (bytesWrittenInChunk > 0) {
            completeChunk();
        }
        return new CompositeBufferView(completed);
    }
}
