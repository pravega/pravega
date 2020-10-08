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
import java.io.DataOutput;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public final class BufferViewOutputStream extends OutputStream implements DataOutput {
    
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
            completeChunk(i);
        }
    }

    private final void completeChunk(int minSize) {
        completed.add(new ByteArraySegment(currentChunk, 0, bytesWrittenInChunk));
        currentChunk = new byte[Math.max(minSize, CHUNK_SIZE)];
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
            completeChunk(0);
        }
        return new CompositeBufferView(completed);
    }

    @Override
    public void writeBoolean(boolean v) {
        write(v ? 1 : 0);
    }

    @Override
    public void writeByte(int v) {
        write(v);
    }

    @Override
    public void writeShort(int v) {
        Exceptions.checkNotClosed(isClosed(), this);
        allocateNewChunkIfNeeded(2);
        currentChunk[bytesWrittenInChunk] = (byte) ((v >>> 8) & 0xFF);
        currentChunk[bytesWrittenInChunk + 1] = (byte) ((v >>> 0) & 0xFF);
        bytesWrittenInChunk += 2;
    }

    @Override
    public void writeChar(int v) {
        writeShort(v);
    }

    @Override
    public void writeInt(int v) {
        Exceptions.checkNotClosed(isClosed(), this);
        allocateNewChunkIfNeeded(4);
        currentChunk[bytesWrittenInChunk] = (byte) ((v >>> 24) & 0xFF);
        currentChunk[bytesWrittenInChunk + 1] = (byte) ((v >>> 16) & 0xFF);
        currentChunk[bytesWrittenInChunk + 2] = (byte) ((v >>> 8) & 0xFF);
        currentChunk[bytesWrittenInChunk + 3] = (byte) ((v >>> 0) & 0xFF);
        bytesWrittenInChunk += 4;
    }

    @Override
    public void writeLong(long v) {
        Exceptions.checkNotClosed(isClosed(), this);
        allocateNewChunkIfNeeded(8);
        currentChunk[bytesWrittenInChunk] = (byte) ((v >>> 56) & 0xFF);
        currentChunk[bytesWrittenInChunk + 1] = (byte) ((v >>> 48) & 0xFF);
        currentChunk[bytesWrittenInChunk + 2] = (byte) ((v >>> 40) & 0xFF);
        currentChunk[bytesWrittenInChunk + 3] = (byte) ((v >>> 32) & 0xFF);
        currentChunk[bytesWrittenInChunk + 4] = (byte) ((v >>> 24) & 0xFF);
        currentChunk[bytesWrittenInChunk + 5] = (byte) ((v >>> 16) & 0xFF);
        currentChunk[bytesWrittenInChunk + 6] = (byte) ((v >>> 8) & 0xFF);
        currentChunk[bytesWrittenInChunk + 7] = (byte) ((v >>> 0) & 0xFF);
        bytesWrittenInChunk += 8;
    }

    @Override
    public void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }

    @Override
    public void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }

    @Override
    public void writeBytes(String s) {
        int len = s.length();
        allocateNewChunkIfNeeded(len);
        for (int i = 0 ; i < len ; i++) {
            currentChunk[bytesWrittenInChunk] = (byte)s.charAt(i);
            bytesWrittenInChunk++;
        }
    }

    @Override
    public void writeChars(String s) {
        int len = s.length();
        allocateNewChunkIfNeeded(2 * len);
        for (int i = 0 ; i < len ; i++) {
            int v = s.charAt(i);
            currentChunk[bytesWrittenInChunk] = (byte) ((v >>> 8) & 0xFF);
            currentChunk[bytesWrittenInChunk + 1] = (byte) ((v >>> 0) & 0xFF);
            bytesWrittenInChunk += 2;
        }
    }

    /**
     * Copied from DataOutputStream.
     */
    @Override
    public void writeUTF(String s) {
        int strlen = s.length();
        int utflen = 0;
        int c = 0;

        /* use charAt instead of copying String to char array */
        for (int i = 0; i < strlen; i++) {
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535)
            throw new IllegalArgumentException(
                "encoded string too long: " + utflen + " bytes");

        allocateNewChunkIfNeeded((utflen*2) + 2);
        byte[] bytearr = currentChunk; //Aliasing
        int index = bytesWrittenInChunk;

        bytearr[index++] = (byte) ((utflen >>> 8) & 0xFF);
        bytearr[index++] = (byte) ((utflen >>> 0) & 0xFF);

        int i=0;
        for (i=0; i<strlen; i++) {
           c = s.charAt(i);
           if (!((c >= 0x0001) && (c <= 0x007F))) break;
           bytearr[index++] = (byte) c;
        }

        for (;i < strlen; i++){
            c = s.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                bytearr[index++] = (byte) c;

            } else if (c > 0x07FF) {
                bytearr[index++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                bytearr[index++] = (byte) (0x80 | ((c >>  6) & 0x3F));
                bytearr[index++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            } else {
                bytearr[index++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
                bytearr[index++] = (byte) (0x80 | ((c >>  0) & 0x3F));
            }
        }
        bytesWrittenInChunk = index;
    }
}
