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
import io.pravega.common.io.serialization.RandomAccessOutputStream;
import io.pravega.common.util.ByteArraySegment;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

/**
 * A ByteArrayOutputStream that exposes the contents as a ByteArraySegment, without requiring a memory copy.
 */
public class EnhancedByteArrayOutputStream extends ByteArrayOutputStream implements RandomAccessOutputStream {
    /**
     * Returns a readonly ByteArraySegment wrapping the current buffer of the ByteArrayOutputStream.
     *
     * @return A readonly ByteArraySegment from the current buffer of the ByteArrayOutputStream.
     */
    public ByteArraySegment getData() {
        return new ByteArraySegment(this.buf, 0, this.count, true);
    }

    @Override
    public void write(byte[] array) {
        this.write(array, 0, array.length);
    }

    //region RandomAccessOutputStream Implementation

    @Override
    public void write(int byteValue, int streamPosition) {
        Preconditions.checkElementIndex(streamPosition, this.buf.length, "streamPosition");
        this.buf[streamPosition] = (byte) byteValue;
    }

    @Override
    public void write(byte[] buffer, int bufferOffset, int length, int streamPosition) {
        if (bufferOffset < 0 || length < 0 || (length > 0 && bufferOffset + length > buffer.length)) {
            throw new ArrayIndexOutOfBoundsException("bufferOffset and length must refer to a range within buffer.");
        }

        Preconditions.checkElementIndex(streamPosition, this.buf.length, "streamPosition");
        if (streamPosition + length <= this.buf.length) {
            // This fits entirely within our buffer.
            System.arraycopy(buffer, bufferOffset, this.buf, streamPosition, length);
        } else {
            // This fits partially within our buffer; as such this will result in an increase.
            int splitPos = this.buf.length - streamPosition;
            System.arraycopy(buffer, bufferOffset, this.buf, streamPosition, splitPos);
            write(buffer, bufferOffset + splitPos, length - splitPos);
        }
    }

    @Override
    public OutputStream subStream(int streamPosition, int length) {
        return new FixedByteArrayOutputStream(this.buf, streamPosition, length);
    }

    //endregion
}
