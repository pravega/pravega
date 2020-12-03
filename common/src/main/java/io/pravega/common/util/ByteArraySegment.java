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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;

/**
 * Allows segmenting a byte array and operating only on that segment.
 */
public class ByteArraySegment extends AbstractBufferView implements ArrayView {
    //region Members

    private final ByteBuffer buffer;
    @Getter
    private final int length;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ByteArraySegment class that wraps the entire given array.
     *
     * @param array The array to wrap.
     * @throws NullPointerException If the array is null.
     */
    public ByteArraySegment(byte[] array) {
        this(array, 0, array.length);
    }

    /**
     * Creates a new instance of the ByteArraySegment class that wraps an array backed ByteBuffer.
     *
     * @param buff       The ByteBuffer to wrap.
     * @throws NullPointerException           If the array is null.
     * @throws UnsupportedOperationException  If buff is not backed by an array.
     */
    public ByteArraySegment(ByteBuffer buff) {
        this(buff.array(), buff.arrayOffset() + buff.position(), buff.remaining());
    }

    /**
     * Creates a new instance of the ByteArraySegment class that wraps the given array range.
     *
     * @param array       The array to wrap.
     * @param startOffset The offset within the array to start the segment at.
     * @param length      The length of the segment.
     * @throws NullPointerException           If the array is null.
     * @throws ArrayIndexOutOfBoundsException If StartOffset or Length have invalid values.
     */
    public ByteArraySegment(byte[] array, int startOffset, int length) {
        this.buffer = ByteBuffer.wrap(array, startOffset, length);
        this.buffer.position(startOffset);
        this.length = length;
    }

    //endregion

    //region ArrayView Implementation

    @Override
    public byte get(int index) {
        return this.buffer.get(this.buffer.position() + index);
    }

    @Override
    public short getShort(int index) {
        return this.buffer.getShort(this.buffer.position() + index);
    }

    @Override
    public int getInt(int index) {
        return this.buffer.getInt(this.buffer.position() + index);
    }

    @Override
    public long getLong(int index) {
        return this.buffer.getLong(this.buffer.position() + index);
    }

    @Override
    public byte[] array() {
        return this.buffer.array();
    }

    @Override
    public int arrayOffset() {
        return this.buffer.arrayOffset() + this.buffer.position();
    }

    @Override
    public Reader getBufferViewReader() {
        return new Reader();
    }

    @Override
    public InputStream getReader() {
        return new ByteArrayInputStream(array(), arrayOffset(), this.length);
    }

    @Override
    public InputStream getReader(int offset, int length) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        return new ByteArrayInputStream(array(), arrayOffset() + offset, length);
    }

    @Override
    public ByteArraySegment slice(int offset, int length) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        return new ByteArraySegment(array(), arrayOffset() + offset, length);
    }

    @Override
    public ByteBuffer asByteBuffer() {
        return this.buffer.duplicate(); // Duplicate to prevent anyone external from messing with our buffer.
    }

    @Override
    public byte[] getCopy() {
        byte[] buffer = new byte[this.length];
        System.arraycopy(array(), arrayOffset(), buffer, 0, this.length);
        return buffer;
    }

    @Override
    public void copyTo(byte[] target, int targetOffset, int length) {
        Preconditions.checkElementIndex(length, this.length + 1, "length");
        Exceptions.checkArrayRange(targetOffset, length, target.length, "index", "values.length");

        System.arraycopy(array(), arrayOffset(), target, targetOffset, length);
    }

    @Override
    public int copyTo(ByteBuffer target) {
        int length = Math.min(this.length, target.remaining());
        target.put(array(), arrayOffset(), length);
        return length;
    }

    /**
     * Writes the entire contents of this ByteArraySegment to the given OutputStream. Only copies the contents of the
     * ByteArraySegment, and writes no other data (such as the length of the Segment or any other info).
     *
     * @param stream The OutputStream to write to.
     * @throws IOException If the OutputStream threw one.
     */
    @Override
    public void copyTo(OutputStream stream) throws IOException {
        stream.write(array(), arrayOffset(), this.length);
    }

    @Override
    public boolean equals(BufferView other) {
        if (this.length != other.getLength()) {
            return false;
        } else if (other instanceof ByteArraySegment) {
            // ByteBuffer-optimized equality check.
            return this.buffer.equals(((ByteArraySegment) other).buffer);
        }

        // No good optimization available; default to AbstractBufferView.equals().
        return super.equals(other);
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT {
        bufferCollector.accept(asByteBuffer());
    }

    @Override
    public Iterator<ByteBuffer> iterateBuffers() {
        return Iterators.singletonIterator(asByteBuffer());
    }

    @Override
    public void set(int index, byte value) {
        this.buffer.put(this.buffer.position() + index, value);
    }

    @Override
    public void setShort(int index, short value) {
        this.buffer.putShort(this.buffer.position() + index, value);
    }

    @Override
    public void setInt(int index, int value) {
        this.buffer.putInt(this.buffer.position() + index, value);
    }

    @Override
    public void setLong(int index, long value) {
        this.buffer.putLong(this.buffer.position() + index, value);
    }

    //endregion

    //region Other Operations

    /**
     * Copies a specified number of bytes from the given {@link ArrayView} into this ByteArraySegment.
     *
     * @param source       The {@link ArrayView} to copy bytes from.
     * @param targetOffset The offset within this ByteArraySegment to start copying at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    public void copyFrom(ArrayView source, int targetOffset, int length) {
        Exceptions.checkArrayRange(targetOffset, length, this.length, "index", "values.length");
        Preconditions.checkElementIndex(length, source.getLength() + 1, "length");

        System.arraycopy(source.array(), source.arrayOffset(), this.array(), this.buffer.position() + targetOffset, length);
    }

    /**
     * Copies a specified number of bytes from the given {@link ArrayView} into this ByteArraySegment.
     *
     * @param source       The {@link ArrayView} to copy bytes from.
     * @param sourceOffset The offset within source to start copying from.
     * @param targetOffset The offset within this ByteArraySegment to start copying at.
     * @param length       The number of bytes to copy.
     * @throws ArrayIndexOutOfBoundsException If targetOffset or length are invalid.
     */
    public void copyFrom(ArrayView source, int sourceOffset, int targetOffset, int length) {
        Exceptions.checkArrayRange(sourceOffset, length, source.getLength(), "index", "values.length");
        Exceptions.checkArrayRange(targetOffset, length, this.length, "index", "values.length");
        Preconditions.checkElementIndex(length, source.getLength() + 1, "length");

        System.arraycopy(source.array(), source.arrayOffset() + sourceOffset, this.array(), this.buffer.position() + targetOffset, length);
    }

    @Override
    public String toString() {
        if (getLength() > 128) {
            return this.buffer.toString();
        } else {
            return String.format("{%s}", IntStream.range(0, this.length).boxed()
                    .map(i -> Byte.toString(get(i)))
                    .collect(Collectors.joining(",")));
        }
    }

    //endregion

    //region Reader

    private class Reader extends AbstractReader implements BufferView.Reader {
        private int position = 0;

        @Override
        public int available() {
            return ByteArraySegment.this.length - this.position;
        }

        @Override
        public int readBytes(ByteBuffer byteBuffer) {
            int len = Math.min(available(), byteBuffer.remaining());
            byteBuffer.put(array(), arrayOffset() + this.position, len);
            this.position += len;
            return len;
        }

        @Override
        public byte readByte() {
            if (this.position >= ByteArraySegment.this.length) {
                throw new OutOfBoundsException();
            }

            byte result = ByteArraySegment.this.get(this.position);
            this.position++;
            return result;
        }

        @Override
        public int readInt() {
            int nextPos = this.position + Integer.BYTES;
            if (nextPos > ByteArraySegment.this.length) {
                throw new OutOfBoundsException();
            }

            int r = ByteArraySegment.this.getInt(this.position);
            this.position = nextPos;
            return r;
        }

        @Override
        public long readLong() {
            int nextPos = this.position + Long.BYTES;
            if (nextPos > ByteArraySegment.this.length) {
                throw new OutOfBoundsException();
            }

            long r = ByteArraySegment.this.getLong(this.position);
            this.position = nextPos;
            return r;
        }

        @Override
        public BufferView readSlice(int length) {
            try {
                BufferView result = ByteArraySegment.this.slice(this.position, length);
                this.position += length;
                return result;
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }
    }

    //endregion
}
