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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AccessLevel;
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
    private static final BufferLayout DEFAULT_BUFFER_LAYOUT = new BufferLayout(12); // 2^12 == 4KB
    /**
     * The offset at which the {@link CompositeByteArraySegment} begins, counted from the first block. This is helpful
     * for slicing a {@link CompositeByteArraySegment}. See {@link #slice}.
     */
    private final int startOffset;
    /**
     * Size of each component array.
     */
    private final BufferLayout bufferLayout;
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
    @VisibleForTesting
    public CompositeByteArraySegment(int length) {
        this(length, DEFAULT_BUFFER_LAYOUT);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} class with the given component array size.
     *
     * @param length       The length of the {@link CompositeByteArraySegment}. This will determine the number of
     *                     components to use, but doesn't allocate any of them yet.
     * @param bufferLayout The {@link BufferLayout} to use.
     */
    public CompositeByteArraySegment(int length, BufferLayout bufferLayout) {
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number.");

        this.length = length;
        this.bufferLayout = bufferLayout;
        this.startOffset = 0;
        int count = length / this.bufferLayout.bufferSize + (length % this.bufferLayout.bufferSize == 0 ? 0 : 1);
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
        this(new ByteBuffer[]{ByteBuffer.wrap(source)}, BufferLayout.fromLength(source.length), 0, source.length);
    }

    /**
     * Creates a new instance of the {@link CompositeByteArraySegment} that uses the given arguments. Useful for slicing.
     *
     * @param buffers      The array components to use.
     * @param bufferLayout The {@link BufferLayout} to use
     * @param startOffset  Start offset.
     * @param length       Length of {@link CompositeByteArraySegment}.
     */
    private CompositeByteArraySegment(ByteBuffer[] buffers, BufferLayout bufferLayout, int startOffset, int length) {
        this.buffers = buffers;
        this.bufferLayout = bufferLayout;
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

        final int bufferId = getBufferId(offset);
        final ByteBuffer bb = getBuffer(bufferId, true);
        final int bufferOffset = getBufferOffset(offset);
        if (bufferOffset <= this.bufferLayout.maxShortOffset) {
            bb.putShort(bufferOffset, value);
        } else {
            bb.array()[bufferOffset] = (byte) (value >>> 8 & 255);
            getBuffer(bufferId + 1, true).array()[0] = (byte) (value & 255);
        }
    }

    @Override
    public void setInt(int offset, int value) {
        Exceptions.checkArrayRange(offset, Integer.BYTES, this.length, "index", "length");

        final int bufferId = getBufferId(offset);
        final ByteBuffer bb = getBuffer(bufferId, true);
        int bufferOffset = getBufferOffset(offset);

        if (bufferOffset <= this.bufferLayout.maxIntOffset) {
            bb.putInt(bufferOffset, value);
        } else {
            setValue(value, Integer.SIZE, bb.array(), bufferId, bufferOffset);
        }
    }

    @Override
    public void setLong(int offset, long value) {
        Exceptions.checkArrayRange(offset, Long.BYTES, this.length, "index", "length");

        final int bufferId = getBufferId(offset);
        final ByteBuffer bb = getBuffer(bufferId, true);
        int bufferOffset = getBufferOffset(offset);

        if (bufferOffset <= this.bufferLayout.maxLongOffset) {
            bb.putLong(bufferOffset, value);
        } else {
            setValue(value, Long.SIZE, bb.array(), bufferId, bufferOffset);
        }
    }

    private void setValue(long value, int bits, byte[] array, int bufferId, int bufferOffset) {
        // Write each byte at a time, making sure we move over to the next buffer when needed.
        assert bits / 8 > 0 && bits % 8 == 0;
        while (bits > 0) {
            bits -= 8;
            array[bufferOffset] = (byte) (value >>> bits);
            bufferOffset++;
            if (bufferOffset >= this.bufferLayout.bufferSize) {
                bufferOffset = 0;
                array = getBuffer(bufferId + 1, true).array();
            }
        }
    }

    @Override
    public int getAllocatedLength() {
        return getAllocatedArrayCount() * this.bufferLayout.bufferSize;
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

        return new CompositeByteArraySegment(this.buffers, this.bufferLayout, this.startOffset + offset, length);
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
            int arrayLength = Math.min(length, this.bufferLayout.bufferSize - bufferOffset);
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
                    int arrayLength = Math.min(length.get(), this.bufferLayout.bufferSize - arrayOffset.get());
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
        return this.length == 0 ? 0 : (this.startOffset + this.length - 1) / this.bufferLayout.bufferSize - this.startOffset / this.bufferLayout.bufferSize + 1;
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
            bb.position(0).limit(this.bufferLayout.bufferSize);
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
        return (this.startOffset + offset) & this.bufferLayout.bufferOffsetMask;
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
        return (this.startOffset + offset) >> this.bufferLayout.bufferSizeBits;
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
            a = ByteBuffer.wrap(new byte[this.bufferLayout.bufferSize]);
            this.buffers[arrayId] = a;
        }

        return a;
    }

    //endregion

    //region Reader

    private final class CompositeReader extends AbstractReader implements BufferView.Reader {
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
            if (CompositeByteArraySegment.this.bufferLayout.bufferSize - arrayPos >= Integer.BYTES) {
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
            if (CompositeByteArraySegment.this.bufferLayout.bufferSize - arrayPos >= Long.BYTES) {
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

    /**
     * Defines the internal layout of a {@link CompositeByteArraySegment}.
     */
    @Getter(AccessLevel.PACKAGE)
    public static class BufferLayout {
        private static final int MIN_BIT_COUNT = 3;
        private static final int MAX_BIT_COUNT = Integer.SIZE - 1;
        /**
         * The size of an internal, component buffer.
         */
        private final int bufferSize;
        /**
         * The number of bits that are required to hold {@link #bufferSize}. In other words, {@link #bufferSize} is 2 to
         * the power of this value.
         */
        private final int bufferSizeBits;
        /**
         * A bit mask that can be used to extract component buffer offsets, given the offset within the {@link CompositeByteArraySegment}.
         */
        private final int bufferOffsetMask;
        /**
         * The highest offset within a component buffer where a {@link Long} can be written at.
         */
        private final int maxLongOffset;
        /**
         * The highest offset within a component buffer where an {@link Integer} can be written at.
         */
        private final int maxIntOffset;
        /**
         * The highest offset within a component buffer where a {@link Short} can be written at.
         */
        private final int maxShortOffset;

        /**
         * Creates a new instance of the {@link BufferLayout} class.
         *
         * @param bufferSizeBits The size of the buffer, expressed in bits. The size of the buffer will be equal to 2 to
         *                       the power of this value.
         * @throws IllegalArgumentException If bufferSizeBits is less than {@link #MIN_BIT_COUNT} or greater than or equal
         *                                  {@link #MAX_BIT_COUNT}.
         */
        public BufferLayout(int bufferSizeBits) {
            Preconditions.checkArgument(bufferSizeBits >= MIN_BIT_COUNT && bufferSizeBits < MAX_BIT_COUNT,
                    "bufferSizeBits must be a number in the range [%s, %s).", MIN_BIT_COUNT, MAX_BIT_COUNT);
            this.bufferSizeBits = bufferSizeBits;
            this.bufferSize = 1 << this.bufferSizeBits; // Array Size is always a power of 2.
            this.bufferOffsetMask = this.bufferSize - 1; // This will set the power-of-2 bit to 0 and all bits to the right to 1.
            this.maxLongOffset = this.bufferSize - Long.BYTES;
            this.maxIntOffset = this.bufferSize - Integer.BYTES;
            this.maxShortOffset = this.bufferSize - Short.BYTES;
        }

        /**
         * Creates a new {@link BufferLayout} instance with the correct number of bits that can hold a SINGLE component
         * buffers of the given size (i.e., the {@link CompositeByteArraySegment} has 1 component).
         *
         * @param bufferLength The size of the component buffer/{@link CompositeByteArraySegment}.
         * @return A {@link BufferLayout}.
         */
        @VisibleForTesting
        static BufferLayout fromLength(int bufferLength) {
            Preconditions.checkArgument(bufferLength >= 0, "arrayLength must be a non-negative number.");
            int bitCount = 0;
            while (bufferLength != 0) {
                bufferLength = bufferLength >>> 1;
                bitCount++;
            }

            if (bitCount < MIN_BIT_COUNT) {
                bitCount = MIN_BIT_COUNT;
            }
            return new BufferLayout(bitCount);
        }
    }
}
