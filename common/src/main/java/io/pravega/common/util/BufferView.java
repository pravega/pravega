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
import io.pravega.common.hash.HashHelper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 * Defines a generic read-only view of a readable memory buffer with a known length.
 *
 * For array-backed Buffers, see {@link ArrayView}.
 *
 * For any implementations that wrap direct memory (a {@link java.nio.ByteBuffer} or Netty ByteBuf and thus support
 * reference counting, consider using {@link #retain()} {@link #release()} to ensure the underlying buffer is correctly
 * managed. Invoke {@link #retain()} if this {@link BufferView} instance is to be needed for more than the buffer creator
 * anticipates (i.e., a background async task) and invoke {@link #release()} to notify that it is no longer needed. The
 * behavior of these two methods are dependent on the actual buffer implementation; for example, Netty ByteBufs only
 * release the memory once the internal reference count reaches 0 (refer to Netty ByteBuf documentation for more details).
 */
public interface BufferView {
    /**
     * Gets a value representing the length of this {@link BufferView}.
     *
     * @return The length.
     */
    int getLength();

    /**
     * Gets a value indicating the amount of memory (in bytes) allocated for this {@link BufferView}.
     *
     * @return The allocated memory size.
     */
    int getAllocatedLength();

    /**
     * Creates a new {@link BufferView.Reader} that can be used to read this {@link BufferView}. This reader is
     * preferable to {@link #getReader()} that returns an {@link InputStream} as it contains optimized methods for copying
     * directly into other {@link BufferView} instances, such as {@link ByteArraySegment}s.
     *
     * @return A new {@link BufferView.Reader}.
     */
    BufferView.Reader getBufferViewReader();

    /**
     * Creates an InputStream that can be used to read the contents of this {@link BufferView}. The InputStream returned
     * spans the entire {@link BufferView}.
     *
     * @return The InputStream.
     */
    InputStream getReader();

    /**
     * Creates an InputStream that can be used to read the contents of this {@link BufferView}.
     *
     * @param offset The starting offset of the section to read.
     * @param length The length of the section to read.
     * @return The InputStream.
     */
    InputStream getReader(int offset, int length);

    /**
     * Equivalent to invoking {@link #slice(int, int)} with offset 0 and getLength(). Depending on the implementation,
     * this may return this instance or a new instance that is a duplicate of this one but pointing to the same backing buffer.
     * No data copies are being made as part of invoking this method.
     *
     * @return A {@link BufferView}.
     */
    default BufferView slice() {
        return this;
    }
    
    /**
     * Creates a new {@link BufferView} that represents a sub-range of this {@link BufferView} instance. The new instance
     * will share the same backing buffer as this one, so a change to one will be reflected in the other.
     *
     * @param offset The starting offset to begin the slice at.
     * @param length The sliced length.
     * @return A new {@link BufferView}.
     */
    BufferView slice(int offset, int length);

    /**
     * Hashes the contents of the buffer in a fast and consistent way.
     *
     * @return A hash of the buffer view's data.
     */
    default long hash() {
        return HashHelper.hashBufferView(this);
    }

    /**
     * Returns a copy of the contents of this {@link BufferView}.
     *
     * @return A byte array with the same length as this {@link BufferView}, containing a copy of the data within it.
     */
    byte[] getCopy();

    /**
     * Copies the contents of this {@link BufferView} to the given {@link OutputStream}.
     *
     * @param target The {@link OutputStream} to write to.
     * @throws IOException If an exception occurred while writing to the target {@link OutputStream}.
     */
    void copyTo(OutputStream target) throws IOException;

    /**
     * Copies the contents of this {@link BufferView} to the given {@link ByteBuffer}.
     *
     * @param byteBuffer The {@link ByteBuffer} to copy to. This buffer must have sufficient capacity to allow the entire
     *                   contents of the {@link BufferView} to be written. If less needs to be copied, consider using
     *                   {@link BufferView#slice} to select a sub-range of this {@link BufferView}.
     * @return The number of bytes copied.
     */
    int copyTo(ByteBuffer byteBuffer);

    /**
     * When implemented in a derived class, notifies any wrapped buffer that this {@link BufferView} has a need for it.
     * Use {@link #release()} to do the opposite. See the main documentation on this interface for recommendations on how
     * to use these to methods. Also refer to the implementing class' documentation for any additional details.
     */
    default void retain() {
        // Default implementation intentionally left blank. Any derived class may implement if needed.
    }

    /**
     * When implemented in a derived class, notifies any wrapped buffer that this {@link BufferView} no longer has a
     * need for it. After invoking this method, this object should no longer be considered safe to access as the underlying
     * buffer may have been deallocated (the actual behavior may vary based on the wrapped buffer, please refer to the
     * implementing class' documentation for any additional details).
     */
    default void release() {
        // Default implementation intentionally left blank. Any derived class may implement if needed.
    }

    /**
     * Iterates through each of the buffers that make up this {@link BufferView}, in order, and invokes the given
     * {@link Collector} on each.
     *
     * @param bufferCollector A {@link Collector} function that will be invoked for each component. Each {@link ByteBuffer}
     *                        passed as an argument to this function is a direct pointer to the data contained within the
     *                        {@link BufferView} (i.e., they are not copies of the data).
     * @param <ExceptionT>    Type of exception that the {@link Collector} function throws, if any.
     * @throws ExceptionT If the {@link Collector} function throws an exception of this type, the iteration will end
     *                    and the exception will be bubbled up.
     */
    <ExceptionT extends Exception> void collect(Collector<ExceptionT> bufferCollector) throws ExceptionT;
    
    /**
     * Gets an {@link Iterator} through each of the {@link ByteBuffer}s that make up this {@link BufferView}, in order.
     *
     * @return A {@link Iterator}.
     */
    Iterator<ByteBuffer> iterateBuffers();

    /**
     * Wraps the given {@link BufferView} into a single instance.
     *
     * @param components The components to wrap. These components will be added by reference, without making any data
     *                   copies. Any modifications made to these components will be reflected in the returned
     *                   {@link BufferView} and vice-versa.
     * @return An empty {@link BufferView} (if the component list is empty), the first item in the list (if the component
     * list has 1 element) or a {@link CompositeBufferView} wrapping all the given instances otherwise.
     */
    static BufferView wrap(List<BufferView> components) {
        if (components.size() == 0) {
            return empty();
        } else if (components.size() == 1) {
            return components.get(0).slice();
        } else {
            return new CompositeBufferView(components);
        }
    }
    
    /**
     * Wraps a given byte array into a view.
     * 
     * @param array The array to wrap.
     * @return A buffer view backed by the provided array.
     */
    static BufferView wrap(byte[] array) {
        return new ByteArraySegment(array);
    }

    /**
     * Creates a new {@link BufferViewBuilder} that can be used to construct composite {@link BufferView} instances.
     *
     * @return A new {@link BufferViewBuilder} with default initial component count.
     */
    static BufferViewBuilder builder() {
        return builder(10);
    }

    /**
     * Creates a new {@link BufferViewBuilder} that can be used to construct composite {@link BufferView} instances.
     *
     * @param expectedComponentCount The initial component count. Knowing this value beforehand will avoid any list copies
     *                               that are done as the builder component list grows.
     * @return A new {@link BufferViewBuilder} with specified initial component count.
     */
    static BufferViewBuilder builder(int expectedComponentCount) {
        return new BufferViewBuilder(expectedComponentCount);
    }

    /**
     * Returns the empty {@link BufferView}.
     *
     * @return The empty {@link BufferView}.
     */
    static BufferView empty() {
        return AbstractBufferView.EMPTY;
    }

    /**
     * Defines a reader for a {@link BufferView}.
     */
    interface Reader {
        /**
         * Gets a value indicating the number of bytes available to read.
         *
         * @return The number of bytes available to read.
         */
        int available();

        /**
         * Reads a number of bytes into the given {@link ByteBuffer} using the most efficient method for the
         * implementation of this {@link BufferView}.
         *
         * @param byteBuffer The target {@link ByteBuffer}.
         * @return The number of bytes copied. This should be <pre><code>Math.min(available(), byteBuffer.remaining())</code></pre>.
         * If this returns 0, then either the given {@link ByteBuffer} has {@link ByteBuffer#remaining()} equal to 0,
         * or there are no more bytes to be read ({@link #available()} is 0).
         */
        int readBytes(ByteBuffer byteBuffer);

        /**
         * Reads one byte and advances the reader position by 1.
         *
         * @return The read byte.
         * @throws OutOfBoundsException If {@link #available()} is 0.
         */
        byte readByte();

        /**
         * Reads 4 bytes (and advances the reader position by 4) and composes a 32-bit Integer (Big-Endian).
         *
         * @return The read int.
         * @throws OutOfBoundsException If {@link #available()} is less than {@link Integer#BYTES}.
         */
        int readInt();

        /**
         * Reads 8 bytes (and advances the reader position by 4) and composes a 64-bit Long (Big-Endian).
         *
         * @return The read long.
         * @throws OutOfBoundsException If {@link #available()} is less than {@link Long#BYTES}.
         */
        long readLong();

        /**
         * Returns a {@link BufferView} that is a representation of the next bytes starting at the given position. The
         * reader position will be advanced by the requested number of bytes.
         *
         * @param length The number of bytes to slice out.
         * @return A {@link BufferView} that represents the given bytes. This {@link BufferView} represents a view into
         * the underlying {@link BufferView} and is not a copy of the given range.
         * @throws OutOfBoundsException If {@link #available()} is less than length.
         */
        BufferView readSlice(int length);

        /**
         * Copies all the remaining bytes from this {@link BufferView.Reader} into a new {@link ArrayView}. The reader
         * position will be set to the end of the {@link BufferView}.
         *
         * @param bufferSize The maximum number of bytes to copy at each iteration. Set to {@link Integer#MAX_VALUE}
         *                   to attempt to copy the whole buffer at once.
         * @return A new {@link ByteArraySegment} with the remaining contents of {@link BufferView.Reader}.
         */
        @VisibleForTesting
        ArrayView readFully(int bufferSize);

        /**
         * Exception that is thrown whenever an attempt is made to read beyond the bounds of a {@link BufferView}.
         */
        class OutOfBoundsException extends IndexOutOfBoundsException {
            public OutOfBoundsException() {
                super();
            }

            public OutOfBoundsException(String message) {
                super(message);
            }
        }
    }

    /**
     * Defines a collector function that can be applied to a ByteBuffer.
     *
     * @param <ExceptionT> Type of exception that this function can throw.
     */
    @FunctionalInterface
    interface Collector<ExceptionT extends Exception> {
        /**
         * Processes a ByteBuffer.
         *
         * @param buffer The ByteBuffer.
         * @throws ExceptionT (Optional) Any exception to throw.
         */
        void accept(ByteBuffer buffer) throws ExceptionT;
    }
}
