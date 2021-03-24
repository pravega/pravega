/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common.io;

import io.pravega.common.io.serialization.RandomAccessOutputStream;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * An {@link OutputStream} that is backed by a {@link ByteBuffer} which can be resized as needed. Prefer using this
 * instead of {@link java.io.ByteArrayOutputStream} since this makes direct use of {@link ByteBuffer} intrinsic methods
 * for writing primitive types (see {@link DirectDataOutput} for example).
 */
@NotThreadSafe
public class ByteBufferOutputStream extends OutputStream implements RandomAccessOutputStream {
    //region Private

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - Long.BYTES;
    private ByteBuffer buf;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link ByteBufferOutputStream}.
     */
    public ByteBufferOutputStream() {
        this(128);
    }

    /**
     * Creates a new instance of the {@link ByteBufferOutputStream} with given initial size.
     *
     * @param initialSize The initial size to create with.
     */
    public ByteBufferOutputStream(int initialSize) {
        this.buf = ByteBuffer.allocate(initialSize);
    }

    //endregion

    //region Implementation

    @Override
    public void write(int b) {
        ensureExtraCapacity(1);
        this.buf.put((byte) b);
    }

    @Override
    public void write(int byteValue, int streamPosition) {
        this.buf.put(streamPosition, (byte) byteValue);
    }

    @Override
    public void write(byte[] array) {
        write(array, 0, array.length);
    }

    @Override
    public void write(byte[] b, int offset, int length) {
        ensureExtraCapacity(length);
        this.buf.put(b, offset, length);
    }

    @Override
    public void write(byte[] array, int arrayOffset, int length, int streamPosition) {
        if (arrayOffset < 0 || length < 0 || (length > 0 && arrayOffset + length > array.length)) {
            throw new ArrayIndexOutOfBoundsException("bufferOffset and length must refer to a range within buffer.");
        }

        final int extra = streamPosition + length - this.buf.position();
        if (extra > 0) {
            ensureExtraCapacity(extra);
        }

        final int originalPos = this.buf.position();
        this.buf.position(streamPosition);
        this.buf.put(array, arrayOffset, length);
        this.buf.position(Math.max(originalPos, streamPosition + length));
    }


    @Override
    public int size() {
        return this.buf.position();
    }

    @Override
    public void flush() {
        // This method intentionally left blank.
    }

    @Override
    public void close() {
        // This method intentionally left blank.
    }

    //endregion

    //region RandomAccessOutputStream and DirectDataOutput Implementation

    @Override
    public void writeBuffer(BufferView buffer) {
        ensureExtraCapacity(buffer.getLength());
        int bytesCopied = buffer.copyTo(this.buf);
        assert bytesCopied == buffer.getLength();
    }

    @Override
    public void writeShort(int shortValue) {
        ensureExtraCapacity(Short.BYTES);
        this.buf.putShort((short) shortValue);
    }

    @Override
    public void writeInt(int intValue) {
        ensureExtraCapacity(Integer.BYTES);
        this.buf.putInt(intValue);
    }

    @Override
    public void writeInt(int intValue, int streamPosition) {
        this.buf.putInt(streamPosition, intValue);
    }

    @Override
    public void writeLong(long longValue) {
        ensureExtraCapacity(Long.BYTES);
        this.buf.putLong(longValue);
    }

    @Override
    public ByteArraySegment getData() {
        return new ByteArraySegment(this.buf.array(), this.buf.arrayOffset(), size());
    }

    //endregion

    //region Helpers

    private void ensureExtraCapacity(int count) {
        // This method is borrowed from ByteArrayOutputStream and adapted for use with ByteBuffers.
        final int minCapacity = this.buf.position() + count;
        if (minCapacity <= this.buf.limit()) {
            return;
        }

        final int oldCapacity = this.buf.limit();
        int newCapacity = oldCapacity << 1;
        if (newCapacity - minCapacity < 0) {
            newCapacity = minCapacity;
        }

        if (newCapacity - MAX_ARRAY_SIZE > 0) {
            if (minCapacity < 0) {
                throw new OutOfMemoryError();
            } else {
                newCapacity = minCapacity > MAX_ARRAY_SIZE ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
            }
        }

        final int oldPosition = this.buf.position();
        this.buf.position(0);
        ByteBuffer newBuf = ByteBuffer.allocate(newCapacity);
        newBuf.put(this.buf);
        newBuf.position(oldPosition);
        this.buf = newBuf;
    }

    //endregion
}
