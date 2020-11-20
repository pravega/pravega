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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.NonNull;

/**
 * A composite, index-based array-like structure that is made up of one or more individual arrays of equal size. Each
 * component array maps to a contiguous offset range and is only allocated when the first index within its range needs
 * to be set (if unallocated, any index within its range will have a value of 0).
 */
public class CompositeByteArraySegment extends AbstractBufferView implements CompositeArrayView {
    //region Members
    /**
     * Default component array size. 4KB maps to the kernel's page size.
     */
    private static final int DEFAULT_ARRAY_SIZE = 4 * 1024;
    /**
     * The offset at which the {@link CompositeByteArraySegment} begins, counted from the first block. This is helpful
     * for slicing a {@link CompositeByteArraySegment}. See {@link #slice}.
     */
    private final int startOffset;
    /**
     * Size of each component array.
     */
    private final int arraySize;
    private final ByteBuffer[] buffers;
    @Getter
    private final int length;
    //endregion

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class with a default component array size.
     *
     * @param length The length of the {@link CompositeByteArraySegment}. This will determine the number of components
     *               to use, but doesn't allocate any of them yet.
     */
    public CompositeByteArraySegment(int length) {
        this(length, DEFAULT_ARRAY_SIZE);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class with the given component array size.
     *
     * @param length    The length of the {@link CompositeByteArraySegment}. This will determine the number of components
     *                  to use, but doesn't allocate any of them yet.
     * @param arraySize The component array size.
     */
    public CompositeByteArraySegment(int length, int arraySize) {
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");
        Preconditions.checkArgument(arraySize >= Long.BYTES, "arraySize must be a number greater than or equal to %s.", Long.BYTES);

        this.length = length;
        this.arraySize = Math.min(length, arraySize); // No point in allocating more memory if total length is smaller than arraySize.
        this.startOffset = 0;
        int count = length / arraySize + (length % arraySize == 0 ? 0 : 1);
        this.buffers = new ByteBuffer[count];
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class that wraps the given array. This instance
     * will have a single component array.
     *
     * @param source The byte array to wrap. Any changes made to this array will be reflected in this
     *               {@link CompositeByteArraySegment} instance and vice-versa.
     */
    @VisibleForTesting
    public CompositeByteArraySegment(@NonNull byte[] source) {
        this(new ByteBuffer[]{ByteBuffer.wrap(source)}, source.length, 0, source.length);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} that uses the given arguments. Useful for slicing.
     *
     * @param buffers     The array components to use.
     * @param arraySize   Size of each individual component.
     * @param startOffset Start offset.
     * @param length      Length of {@link CompositeByteArraySegment}.
     */
    private CompositeByteArraySegment(ByteBuffer[] buffers, int arraySize, int startOffset, int length) {
        this.buffers = buffers;
        this.arraySize = arraySize;
        this.startOffset = startOffset;
        this.length = length;
    }

    //region CompositeArrayView Implementation

    @Override
    public byte get(int offset) {
        ByteBuffer bb = getBuffer(getBufferId(offset), false); // No need to allocate array if not allocated yet.
        return bb == null ? 0 : bb.array()[getBufferOffset(offset)];
    }

    @Override
    public void set(int offset, byte value) {
        ByteBuffer bb = getBuffer(getBufferId(offset), true); // Need to allocate array if not allocated yet.
        bb.array()[getBufferOffset(offset)] = value;
    }

    @Override
    public void setShort(int offset, short value) {
        Exceptions.checkArrayRange(offset, Short.BYTES, this.length, "index", "length");

        int bufferId = getBufferId(offset);
        ByteBuffer bb = getBuffer(bufferId, true);
        int bufferOffset = getBufferOffset(offset);
        try {
            bb.putShort(bufferOffset, value);
        } catch (IndexOutOfBoundsException | BufferOverflowException ex) {
            bb.array()[bufferOffset] = (byte) (value >>> 8 & 255);
            getBuffer(bufferId + 1, true).array()[0] = (byte) (value & 255);
        }
    }

    @Override
    public void setInt(int offset, int value) {
        Exceptions.checkArrayRange(offset, Integer.BYTES, this.length, "index", "length");

        int bufferId = getBufferId(offset);
        ByteBuffer bb = getBuffer(bufferId, true);
        int bufferOffset = getBufferOffset(offset);

        try {
            bb.putInt(bufferOffset, value);
            return;
        } catch (IndexOutOfBoundsException | BufferOverflowException ex) {
            // Intentionally left blank.
        }

        int diff = bb.remaining() - bufferOffset;
        if (diff == 3) {
            // We can only fit 3 bytes. Write as 1 short + 2 bytes.
            bb.putShort(bufferOffset, (short) (value >>> 16));
            bb.array()[bufferOffset + Short.BYTES] = (byte) (value >>> 8);
            getBuffer(bufferId + 1, true).array()[0] = (byte) value;
        } else if (diff == 2) {
            // We can only fit 2 bytes. Write as 2 shorts
            bb.putShort(bufferOffset, (short) (value >>> 16));
            getBuffer(bufferId + 1, true).putShort(0, (short) value);
        } else {
            // We can only fit 1 byte. Write as 2 bytes + 1 short.
            bb.array()[bufferOffset] = (byte) (value >>> 24);
            bb = getBuffer(bufferId + 1, true);
            bb.array()[0] = (byte) (value >>> 16);
            bb.putShort(1, (short) value);
        }
    }

    @Override
    public void setLong(int offset, long value) {
        Exceptions.checkArrayRange(offset, Long.BYTES, this.length, "index", "length");

        int bufferId = getBufferId(offset);
        ByteBuffer bb = getBuffer(bufferId, true);
        int bufferOffset = getBufferOffset(offset);

        try {
            bb.putLong(bufferOffset, value);
            return;
        } catch (IndexOutOfBoundsException | BufferOverflowException ex) {
            // Intentionally left blank.
        }

        int diff = bb.remaining() - bufferOffset;
        if (diff == 7) {
            // Write as 1 int, 1 short, 2 bytes.
            bb.putInt(bufferOffset, (int) (value >>> 32));
            bb.putShort(bufferOffset + Integer.BYTES, (short) ((int) (value >>> 16)));
            bb.array()[bufferOffset + Integer.BYTES + Short.BYTES] = (byte) (value >>> 8);
            getBuffer(bufferId + 1, true).array()[0] = (byte) value;
        } else if (diff == 6) {
            // Write as 1 int, 2 shorts.
            bb.putInt(bufferOffset, (int) (value >>> 32));
            bb.putShort(bufferOffset + Integer.BYTES, (short) ((int) (value >>> 16)));
            bb = getBuffer(bufferId + 1, true);
            bb.putShort(0, (short) ((int) value));
        } else if (diff == 5) {
            // Write as 1 int, 1 byte, 1 short, 1 byte.
            bb.putInt(bufferOffset, (int) (value >>> 32));
            bb.array()[bufferOffset + Integer.BYTES] = (byte) (value >>> 24);
            bb = getBuffer(bufferId + 1, true);
            bb.putShort(0, (short) ((int) (value >>> 8)));
            bb.array()[Short.BYTES] = (byte) value;
        } else if (diff == 4) {
            // Write as 2 ints.
            bb.putInt(bufferOffset, (int) (value >>> 32));
            bb = getBuffer(bufferId + 1, true);
            bb.putInt(0, (int) value);
        } else if (diff == 3) {
            // Write as 1 short, 1 byte, 1 int, 1 byte.
            bb.putShort(bufferOffset, (short) ((int) (value >>> 48)));
            bb.array()[bufferOffset + Short.BYTES] = (byte) (value >>> 40);
            bb = getBuffer(bufferId + 1, true);
            bb.putInt(0, (int) (value >>> 8));
            bb.array()[Integer.BYTES] = (byte) value;
        } else if (diff == 2) {
            // Write as 1 short, 1 int, 1 short
            bb.putShort(bufferOffset, (short) ((int) (value >>> 48)));
            bb = getBuffer(bufferId + 1, true);
            bb.putInt(0, (int) (value >>> 16));
            bb.putShort(Integer.BYTES, (short) ((int) value));
        } else {
            // Write as 1 byte, 1 int, 1 short, 1 byte.
            bb.array()[bufferOffset] = (byte) (value >>> 56);
            bb = getBuffer(bufferId + 1, true);
            bb.putInt(0, (int) (value >>> 24));
            bb.putShort(Integer.BYTES, (short) ((int) (value >>> 8)));
            bb.array()[Integer.BYTES + Short.BYTES] = (byte) value;
        }
    }

    @Override
    public CompositeReader getBufferViewReader() {
        return new CompositeReader();
    }

    @Override
    public InputStream getReader() {
        // Use the collector to create a list of ByteArrayInputStreams and then return them as combined.
        ArrayList<ByteArrayInputStream> streams = new ArrayList<>();
        collect((array, offset, length) -> streams.add(new ByteArrayInputStream(array, offset, length)), this.length);
        return new SequenceInputStream(Iterators.asEnumeration(streams.iterator()));
    }

    @Override
    public InputStream getReader(int offset, int length) {
        return slice(offset, length).getReader();
    }

    @Override
    public CompositeArrayView slice(int offset, int length) {
        Exceptions.checkArrayRange(offset, length, this.length, "offset", "length");
        if (offset == 0 && length == this.length) {
            // Nothing to slice.
            return this;
        }

        return new CompositeByteArraySegment(this.buffers, this.arraySize, this.startOffset + offset, length);
    }

    @Override
    public <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT {
        collect((array, offset, len) -> bufferCollector.accept(ByteBuffer.wrap(array, offset, len)), this.length);
    }

    private <ExceptionT extends Exception> void collect(ArrayCollector<ExceptionT> collectArray, int length) throws ExceptionT {
        if (length == 0) {
            // Nothing to collect.
            return;
        }

        // We only need to process a subset of our arrays (since we may be sliced from the original array list).
        int startId = getBufferId(0);
        int endId = getBufferId(length - 1);

        int bufferOffset = getBufferOffset(0); // The first ByteBuffer may need an offset, if this.startOffset > 0.
        for (int bufferId = startId; bufferId <= endId; bufferId++) {
            int arrayLength = Math.min(length, this.arraySize - bufferOffset);
            ByteBuffer bb = getBuffer(bufferId, false); // Don't allocate array if not allocated yet.
            if (bb == null) {
                // Providing a dummy, empty array of the correct size is the easiest way to handle unallocated components
                // for all the cases this method is used for.
                collectArray.accept(new byte[arrayLength], 0, arrayLength);
            } else {
                collectArray.accept(bb.array(), bufferOffset, arrayLength);
            }

            length -= arrayLength;
            bufferOffset = 0; // After processing the first array (handling this.startOffset), all other array offsets are 0.
        }

        assert length == 0 : "Collection finished but " + length + " bytes remaining";
    }

    @Override
    public Iterator<ByteBuffer> iterateBuffers() {
        if (this.length == 0) {
            return Collections.emptyIterator();
        }

        AtomicInteger arrayOffset = new AtomicInteger(getBufferOffset(0));
        AtomicInteger length = new AtomicInteger(this.length);
        return Arrays.stream(this.buffers, getBufferId(0), getBufferId(this.length - 1) + 1)
                .map(o -> {
                    int arrayLength = Math.min(length.get(), this.arraySize - arrayOffset.get());
                    byte[] b;
                    if (o == null) {
                        b = new byte[arrayLength];
                    } else {
                        b = o.array();
                    }
                    ByteBuffer bb = ByteBuffer.wrap(b, arrayOffset.get(), arrayLength);
                    arrayOffset.set(0);
                    length.addAndGet(-arrayLength);
                    return bb;
                })
                .iterator();
    }

    @Override
    public int getComponentCount() {
        return this.length == 0 ? 0 : (this.startOffset + this.length - 1) / this.arraySize - this.startOffset / this.arraySize + 1;
    }

    @Override
    public byte[] getCopy() {
        byte[] result = new byte[this.length];
        copyTo(ByteBuffer.wrap(result));
        return result;
    }

    @Override
    public void copyFrom(BufferView.Reader source, int targetOffset, int length) {
        Preconditions.checkArgument(length <= source.available(), "length exceeds source input's length.");
        Exceptions.checkArrayRange(targetOffset, length, this.length, "offset", "length");

        int bufferOffset = getBufferOffset(targetOffset);
        int bufferId = getBufferId(targetOffset);
        while (length > 0) {
            ByteBuffer bb = getBuffer(bufferId, true); // Need to allocate if not already allocated.

            // Set the position and limit of our ByteBuffer. BufferView.Reader.readBytes() uses these to determine how
            // much to copy and where to copy the bytes to. We need to be especially careful with the last ByteBuffer in
            // our list as it may be larger than needed (all our buffers are equal in length, but our total requested size
            // may be short of their sum).
            bb.position(bufferOffset);
            bb.limit(Math.min(bb.limit(), bb.position() + length));

            int copyLength = source.readBytes(bb);

            // Reset position and limit after copy.
            bb.position(0).limit(this.arraySize);
            length -= copyLength;
            bufferOffset += copyLength;
            if (bufferOffset >= bb.array().length) {
                bufferId++;
                bufferOffset = 0;
            }
        }
    }

    @Override
    public void copyTo(OutputStream target) throws IOException {
        collect(target::write, this.length);
    }

    @Override
    public int copyTo(ByteBuffer target) {
        int length = Math.min(this.length, target.remaining());
        collect(target::put, length);
        return length;
    }

    //endregion

    //region Helpers

    /**
     * Gets the number of arrays allocated.
     *
     * @return The number of arrays allocated.
     */
    @VisibleForTesting
    int getAllocatedArrayCount() {
        return (int) Arrays.stream(this.buffers).filter(Objects::nonNull).count();
    }

    /**
     * Calculates the offset within an array that the given external offset maps to.
     * Use {@link #getBufferId} to identify which array the offset maps to and to validate offset is a valid offset within
     * this {@link #CompositeByteArraySegment}.
     *
     * @param offset The external offset to map.
     * @return The offset within a component array.
     */
    private int getBufferOffset(int offset) {
        return (this.startOffset + offset) % this.arraySize;
    }

    /**
     * Calculates the component array id (index within {@link #buffers} that the given external offset maps to.
     * Use {@link #getBufferOffset} to identify the offset within this array.
     *
     * @param offset The external offset to map.
     * @return The component array id.
     */
    private int getBufferId(int offset) {
        Preconditions.checkElementIndex(offset, this.length, "offset");
        return (this.startOffset + offset) / this.arraySize;
    }

    /**
     * Gets the component array with given id.
     *
     * @param arrayId  The array id (index within {@link #buffers} to return.
     * @param allocate If true, then the component array is allocated (if not already) before returning. If false, the
     *                 component array is not allocated.
     * @return The component array with given id. May return null if allocate == false.
     */
    private ByteBuffer getBuffer(int arrayId, boolean allocate) {
        ByteBuffer a = this.buffers[arrayId];
        if (a == null && allocate) {
            a = ByteBuffer.wrap(new byte[this.arraySize]);
            this.buffers[arrayId] = a;
        }

        return a;
    }

    //endregion

    //region Reader

    private class CompositeReader extends AbstractReader implements BufferView.Reader {
        private int position = 0;

        @Override
        public int available() {
            return CompositeByteArraySegment.this.length - this.position;
        }

        @Override
        public int readBytes(ByteBuffer byteBuffer) {
            int len = Math.min(available(), byteBuffer.remaining());
            if (len > 0) {
                slice(this.position, len).copyTo(byteBuffer);
                this.position += len;
            }
            return len;
        }

        @Override
        public byte readByte() {
            try {
                return get(this.position++);
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }

        @Override
        public int readInt() {
            int arrayPos = getBufferOffset(this.position);
            if (CompositeByteArraySegment.this.arraySize - arrayPos >= Integer.BYTES) {
                int nextPos = this.position + Integer.BYTES;
                if (nextPos > length) {
                    throw new OutOfBoundsException();
                }

                ByteBuffer bb = getBuffer(getBufferId(this.position), false);
                int r = bb == null ? 0 : bb.getInt(arrayPos);
                this.position = nextPos;
                return r;
            }

            return super.readInt();
        }

        @Override
        public long readLong() {
            int arrayPos = getBufferOffset(this.position);
            if (CompositeByteArraySegment.this.arraySize - arrayPos >= Long.BYTES) {
                int nextPos = this.position + Long.BYTES;
                if (nextPos > length) {
                    throw new OutOfBoundsException();
                }

                ByteBuffer bb = getBuffer(getBufferId(this.position), false);
                long r = bb == null ? 0 : bb.getLong(arrayPos);
                this.position = nextPos;
                return r;
            }

            return super.readLong();
        }

        @Override
        public BufferView readSlice(int length) {
            try {
                BufferView result = slice(this.position, length);
                this.position += length;
                return result;
            } catch (IndexOutOfBoundsException ex) {
                throw new OutOfBoundsException();
            }
        }
    }

    //endregion

    @FunctionalInterface
    private interface ArrayCollector<ExceptionT extends Exception> {
        /**
         * Processes an array range.
         *
         * @param array       The array.
         * @param arrayOffset The start offset within the array.
         * @param length      The number of bytes, beginning at startOffset, that need to be processed.
         * @throws ExceptionT (Optional) Any exception to throw.
         */
        void accept(byte[] array, int arrayOffset, int length) throws ExceptionT;
    }
}
