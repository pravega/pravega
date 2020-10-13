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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
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
     * Returns a copy of the contents of this {@link BufferView}.
     *
     * @return A byte array with the same length as this ArrayView, containing a copy of the data within it.
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
     * Gets a list of {@link ByteBuffer} that represent the contents of this {@link BufferView}. These buffer point
     * directly to the data contained within this buffer (i.e., they are not copies of the data).
     *
     * @return A List of {@link ByteBuffer}.
     */
    List<ByteBuffer> getContents();

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
     * Wraps the given {@link BufferView} into a single instance.
     *
     * @param components The components to wrap. These components will be added by reference, without making any data
     *                   copies. Any modifications made to these components will be reflected in the returned
     *                   {@link BufferView} and vice-versa.
     * @return An empty {@link BufferView} (if the component list is empty), the first item in the list (if the component
     * list has 1 element) or a {@link CompositeBufferView} wrapping all the given instances otherwise.
     */
    static BufferView wrap(BufferView... components) {
        if (components.length == 0) {
            return empty();
        } else if (components.length == 1) {
            return components[0].slice();
        } else {
            return new CompositeBufferView(components);
        }
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
         * Reads one input byte and returns
         * {@code true} if that byte is nonzero,
         * {@code false} if that byte is zero.
         * This method is suitable for reading
         * the byte written by the {@code writeBoolean}
         * method of interface {@code DataOutput}.
         *
         * @return     the {@code boolean} value read.
         * @exception  OutOfBoundsException  if this stream reaches the end before reading
         *               all the bytes.
         */
        boolean readBoolean();
        
        /**
         * Reads a number of bytes into the given {@link ByteArraySegment} using the most efficient method for the
         * implementation of this {@link BufferView}.
         *
         * @param segment The target {@link ByteArraySegment}.
         * @return The number of bytes copied. This should be <pre><code>Math.min(available(), segment.getLength())</code></pre>.
         * If this returns 0, then either the given {@link ByteArraySegment} has {@link ByteArraySegment#getLength()} equal
         * to 0, or there are no more bytes to be read ({@link #available()} is 0).
         */
        int readBytes(ByteArraySegment segment);

        /**
         * Reads one byte and advances the reader position by 1.
         *
         * @return The read byte.
         * @throws OutOfBoundsException If {@link #available()} is 0.
         */
        byte readByte();

        /**
         * Reads two input bytes and returns
         * a {@code short} value. Let {@code a}
         * be the first byte read and {@code b}
         * be the second byte. The value
         * returned
         * is:
         * <pre>{@code (short)((a << 8) | (b & 0xff))
         * }</pre>
         * This method
         * is suitable for reading the bytes written
         * by the {@code writeShort} method of
         * interface {@code DataOutput}.
         *
         * @return     the 16-bit value read.
         * @exception  OutOfBoundsException  if this stream reaches the end before reading
         *               all the bytes.
         */
        short readShort();
        
        /**
         * Reads two input bytes and returns a {@code char} value.
         * Let {@code a}
         * be the first byte read and {@code b}
         * be the second byte. The value
         * returned is:
         * <pre>{@code (char)((a << 8) | (b & 0xff))
         * }</pre>
         * This method
         * is suitable for reading bytes written by
         * the {@code writeChar} method of interface
         * {@code DataOutput}.
         *
         * @return     the {@code char} value read.
         * @exception  OutOfBoundsException  if this stream reaches the end before reading
         *               all the bytes.
         */
        char readChar();

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
         * Reads four input bytes and returns
         * a {@code float} value. It does this
         * by first constructing an {@code int}
         * value in exactly the manner
         * of the {@code readInt}
         * method, then converting this {@code int}
         * value to a {@code float} in
         * exactly the manner of the method {@code Float.intBitsToFloat}.
         * This method is suitable for reading
         * bytes written by the {@code writeFloat}
         * method of interface {@code DataOutput}.
         *
         * @return     the {@code float} value read.
         * @exception  OutOfBoundsException  if this stream reaches the end before reading
         *               all the bytes.
         */
        float readFloat();

        /**
         * Reads eight input bytes and returns
         * a {@code double} value. It does this
         * by first constructing a {@code long}
         * value in exactly the manner
         * of the {@code readLong}
         * method, then converting this {@code long}
         * value to a {@code double} in exactly
         * the manner of the method {@code Double.longBitsToDouble}.
         * This method is suitable for reading
         * bytes written by the {@code writeDouble}
         * method of interface {@code DataOutput}.
         *
         * @return     the {@code double} value read.
         * @exception  OutOfBoundsException  if this stream reaches the end before reading
         *               all the bytes.
         */
        double readDouble();

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
         * Reads in a string that has been encoded using a
         * <a href="#modified-utf-8">modified UTF-8</a>
         * format.
         * The general contract of {@code readUTF}
         * is that it reads a representation of a Unicode
         * character string encoded in modified
         * UTF-8 format; this string of characters
         * is then returned as a {@code String}.
         * <p>
         * First, two bytes are read and used to
         * construct an unsigned 16-bit integer in
         * exactly the manner of the {@code readUnsignedShort}
         * method . This integer value is called the
         * <i>UTF length</i> and specifies the number
         * of additional bytes to be read. These bytes
         * are then converted to characters by considering
         * them in groups. The length of each group
         * is computed from the value of the first
         * byte of the group. The byte following a
         * group, if any, is the first byte of the
         * next group.
         * <p>
         * If the first byte of a group
         * matches the bit pattern {@code 0xxxxxxx}
         * (where {@code x} means "may be {@code 0}
         * or {@code 1}"), then the group consists
         * of just that byte. The byte is zero-extended
         * to form a character.
         * <p>
         * If the first byte
         * of a group matches the bit pattern {@code 110xxxxx},
         * then the group consists of that byte {@code a}
         * and a second byte {@code b}. If there
         * is no byte {@code b} (because byte
         * {@code a} was the last of the bytes
         * to be read), or if byte {@code b} does
         * not match the bit pattern {@code 10xxxxxx},
         * then a {@code UTFDataFormatException}
         * is thrown. Otherwise, the group is converted
         * to the character:
         * <pre>{@code (char)(((a & 0x1F) << 6) | (b & 0x3F))
         * }</pre>
         * If the first byte of a group
         * matches the bit pattern {@code 1110xxxx},
         * then the group consists of that byte {@code a}
         * and two more bytes {@code b} and {@code c}.
         * If there is no byte {@code c} (because
         * byte {@code a} was one of the last
         * two of the bytes to be read), or either
         * byte {@code b} or byte {@code c}
         * does not match the bit pattern {@code 10xxxxxx},
         * then a {@code UTFDataFormatException}
         * is thrown. Otherwise, the group is converted
         * to the character:
         * <pre>{@code
         * (char)(((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F))
         * }</pre>
         * If the first byte of a group matches the
         * pattern {@code 1111xxxx} or the pattern
         * {@code 10xxxxxx}, then a {@code UTFDataFormatException}
         * is thrown.
         * <p>
         * If end of file is encountered
         * at any time during this entire process,
         * then an {@code EOFException} is thrown.
         * <p>
         * After every group has been converted to
         * a character by this process, the characters
         * are gathered, in the same order in which
         * their corresponding groups were read from
         * the input stream, to form a {@code String},
         * which is returned.
         * <p>
         * The {@code writeUTF}
         * method of interface {@code DataOutput}
         * may be used to write data that is suitable
         * for reading by this method.
         * @return     a Unicode string.
         * @exception  OutOfBoundsException            if this stream reaches the end
         *               before reading all the bytes.
         * @exception  UTFDataFormatException  if the bytes do not represent a
         *               valid modified UTF-8 encoding of a string.
         */
        String readUTF() throws UTFDataFormatException;

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
