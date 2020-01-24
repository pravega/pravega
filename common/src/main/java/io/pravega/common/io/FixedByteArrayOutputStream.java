/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.io.serialization.RandomAccessOutputStream;
import io.pravega.common.util.ByteArraySegment;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream that writes to a fixed-size buffer (this is needed because ByteArrayOutputStream auto-grows the buffer).
 */
public class FixedByteArrayOutputStream extends OutputStream implements RandomAccessOutputStream {
    //region Members

    private final byte[] array;
    private final int offset;
    private final int length;
    private int position;
    private boolean isClosed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the FixedByteArrayOutputStream class.
     *
     * @param array  The array to wrap.
     * @param offset The offset to start the OutputStream at.
     * @param length The maximum length of the OutputStream.
     * @throws NullPointerException           If array is null.
     * @throws ArrayIndexOutOfBoundsException If offset and/or length are invalid.
     */
    public FixedByteArrayOutputStream(byte[] array, int offset, int length) {
        Preconditions.checkNotNull(array, "array");
        Exceptions.checkArrayRange(offset, length, array.length, "offset", "length");

        this.array = array;
        this.offset = offset;
        this.length = length;
        this.position = 0;
    }

    //endregion

    //region OutputStream and RandomAccessOutputStream Implementation

    @Override
    public void write(int b) throws IOException {
        if (this.isClosed) {
            throw new IOException("OutputStream is closed.");
        }

        if (this.position >= this.length) {
            throw new IOException("Buffer capacity exceeded.");
        }

        this.array[this.offset + this.position] = (byte) b;
        this.position++;
    }

    @Override
    public void write(int byteValue, int streamPosition) throws IOException {
        if (this.isClosed) {
            throw new IOException("OutputStream is closed.");
        }
        streamPosition += this.offset;
        Preconditions.checkElementIndex(streamPosition, this.array.length, "streamPosition");
        this.array[streamPosition] = (byte) byteValue;
    }

    @Override
    public void write(byte[] buffer, int bufferOffset, int length, int streamPosition) throws IOException {
        if (this.isClosed) {
            throw new IOException("OutputStream is closed.");
        }

        if (bufferOffset < 0 || length < 0 || (length > 0 && bufferOffset + length > buffer.length)) {
            throw new ArrayIndexOutOfBoundsException("bufferOffset and length must refer to a range within buffer.");
        }

        streamPosition += this.offset;
        if (streamPosition + length > this.array.length) {
            throw new ArrayIndexOutOfBoundsException("streamPosition+length must refer to a position within the Stream array.");
        }

        System.arraycopy(buffer, bufferOffset, this.array, streamPosition, length);
    }

    @Override
    public OutputStream subStream(int streamPosition, int length) {
        return new FixedByteArrayOutputStream(this.array, this.offset + streamPosition, length);
    }

    @Override
    public int size() {
        return this.position;
    }

    /**
     * Returns a readonly ByteArraySegment wrapping this Stream's buffer.
     *
     * @return A readonly ByteArraySegment from the current buffer.
     */
    public ByteArraySegment getData() {
        return new ByteArraySegment(this.array, this.offset, this.length, true);
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    //endregion
}
