/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Helper methods for various Number to Bit conversions.
 */
public final class BitConverter {
    /**
     * Writes the given 16-bit Short to the given ArrayView at the given offset.
     *
     * @param target The ArrayView to write to.
     * @param offset The offset within the ArrayView to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeShort(ArrayView target, int offset, short value) {
        return writeShort(target.array(), target.arrayOffset() + offset, value);
    }

    /**
     * Writes the given 16-bit Short to the given byte array at the given offset.
     *
     * @param target The byte array to write to.
     * @param offset The offset within the byte array to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeShort(byte[] target, int offset, short value) {
        target[offset] = (byte) (value >>> 8 & 255);
        target[offset + 1] = (byte) (value & 255);
        return Short.BYTES;
    }

    /**
     * Writes the given 32-bit Integer to the given ArrayView at the given offset.
     *
     * @param target The ArrayView to write to.
     * @param offset The offset within the ArrayView to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeInt(ArrayView target, int offset, int value) {
        return writeInt(target.array(), target.arrayOffset() + offset, value);
    }

    /**
     * Writes the given 32-bit Integer to the given byte array at the given offset.
     *
     * @param target The byte array to write to.
     * @param offset The offset within the byte array to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
        return Integer.BYTES;
    }

    /**
     * Writes the given 32-bit Integer to the given OutputStream.
     *
     * @param target The OutputStream to write to.
     * @param value  The value to write.
     * @return The number of bytes written.
     * @throws IOException If an exception got thrown.
     */
    public static int writeInt(OutputStream target, int value) throws IOException {
        target.write(value >>> 24);
        target.write(value >>> 16);
        target.write(value >>> 8);
        target.write(value);
        return Integer.BYTES;
    }

    /**
     * Reads a 16-bit Short from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static short readShort(byte[] source, int position) {
        return (short) ((source[position] & 0xFF) << 8
                | (source[position + 1] & 0xFF));
    }

    /**
     * Reads a 16-bit Short from the given ArrayView starting at the given position.
     *
     * @param source   The ArrayView to read from.
     * @param position The position in the ArrayView to start reading at.
     * @return The read number.
     */
    public static short readShort(ArrayView source, int position) {
        return (short) ((source.get(position) & 0xFF) << 8
                | (source.get(position + 1) & 0xFF));
    }

    /**
     * Reads a 32-bit integer from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static int readInt(byte[] source, int position) {
        return (source[position] & 0xFF) << 24
                | (source[position + 1] & 0xFF) << 16
                | (source[position + 2] & 0xFF) << 8
                | (source[position + 3] & 0xFF);
    }

    /**
     * Reads a 32-bit integer from the given ArrayView starting at the given position.
     *
     * @param source   The ArrayView to read from.
     * @param position The position in the ArrayView to start reading at.
     * @return The read number.
     */
    public static int readInt(ArrayView source, int position) {
        return (source.get(position) & 0xFF) << 24
                | (source.get(position + 1) & 0xFF) << 16
                | (source.get(position + 2) & 0xFF) << 8
                | (source.get(position + 3) & 0xFF);
    }

    /**
     * Reads a 32-bit integer from the given InputStream that was encoded using BitConverter.writeInt.
     *
     * @param source The InputStream to read from.
     * @return The read number.
     * @throws IOException If an exception got thrown.
     */
    public static int readInt(InputStream source) throws IOException {
        int b1 = source.read();
        int b2 = source.read();
        int b3 = source.read();
        int b4 = source.read();
        if ((b1 | b2 | b3 | b4) < 0) {
            throw new EOFException();
        } else {
            return (b1 << 24) + (b2 << 16) + (b3 << 8) + b4;
        }
    }

    /**
     * Writes the given 64-bit Long to the given ArrayView at the given offset.
     *
     * @param target The ArrayView to write to.
     * @param offset The offset within the ArrayView to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeLong(ArrayView target, int offset, long value) {
        return writeLong(target.array(), target.arrayOffset() + offset, value);
    }

    /**
     * Writes the given 64-bit Long to the given byte array at the given offset.
     *
     * @param target The byte array to write to.
     * @param offset The offset within the byte array to write at.
     * @param value  The value to write.
     * @return The number of bytes written.
     */
    public static int writeLong(byte[] target, int offset, long value) {
        target[offset] = (byte) (value >>> 56);
        target[offset + 1] = (byte) (value >>> 48);
        target[offset + 2] = (byte) (value >>> 40);
        target[offset + 3] = (byte) (value >>> 32);
        target[offset + 4] = (byte) (value >>> 24);
        target[offset + 5] = (byte) (value >>> 16);
        target[offset + 6] = (byte) (value >>> 8);
        target[offset + 7] = (byte) value;
        return Long.BYTES;
    }

    /**
     * Reads a 64-bit long from the given ArrayView starting at the given position.
     *
     * @param source   The ArrayView to read from.
     * @param position The position in the ArrayView to start reading at.
     * @return The read number.
     */
    public static long readLong(ArrayView source, int position) {
        return (long) (source.get(position) & 0xFF) << 56
                | (long) (source.get(position + 1) & 0xFF) << 48
                | (long) (source.get(position + 2) & 0xFF) << 40
                | (long) (source.get(position + 3) & 0xFF) << 32
                | (long) (source.get(position + 4) & 0xFF) << 24
                | (source.get(position + 5) & 0xFF) << 16
                | (source.get(position + 6) & 0xFF) << 8
                | (source.get(position + 7) & 0xFF);
    }

    /**
     * Reads a 64-bit long from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static long readLong(byte[] source, int position) {
        return (long) (source[position] & 0xFF) << 56
                | (long) (source[position + 1] & 0xFF) << 48
                | (long) (source[position + 2] & 0xFF) << 40
                | (long) (source[position + 3] & 0xFF) << 32
                | (long) (source[position + 4] & 0xFF) << 24
                | (source[position + 5] & 0xFF) << 16
                | (source[position + 6] & 0xFF) << 8
                | (source[position + 7] & 0xFF);
    }

    /**
     * Reads a 64-bit long from the given InputStream that was encoded using BitConverter.writeLong.
     *
     * @param source The InputStream to read from.
     * @return The read number.
     * @throws IOException If an exception got thrown.
     */
    public static long readLong(InputStream source) throws IOException {
        int b1 = source.read();
        int b2 = source.read();
        int b3 = source.read();
        int b4 = source.read();
        int b5 = source.read();
        int b6 = source.read();
        int b7 = source.read();
        int b8 = source.read();
        if ((b1 | b2 | b3 | b4 | b5 | b6 | b7 | b8) < 0) {
            throw new EOFException();
        } else {
            return ((long) b1 << 56) +
                    ((long) (b2 & 255) << 48) +
                    ((long) (b3 & 255) << 40) +
                    ((long) (b4 & 255) << 32) +
                    ((long) (b5 & 255) << 24) +
                    (long) ((b6 & 255) << 16) +
                    (long) ((b7 & 255) << 8) +
                    (long) ((b8 & 255));
        }
    }

    /**
     * Writes the given 64-bit Unsigned Long to the given byte array at the given offset. This value can then be
     * deserialized using {@link #readUnsignedLong}. This method is not interoperable with {@link #readLong}.
     *
     * The advantage of serializing as Unsigned Long (vs. a normal Signed Long) is that the serialization will have the
     * same natural order as the input value type (i.e., if compared using a lexicographic bitwise comparator such as
     * ByteArrayComparator, it will have the same ordering as the typical Long type).
     *
     * @param target The byte array to write to.
     * @param offset The offset within the byte array to write at.
     * @param value  The (signed) value to write. The value will be converted into the range [0, 2^64-1] before
     *               serialization by flipping the high order bit (so positive values will begin with 1 and negative values
     *               will begin with 0).
     * @return The number of bytes written.
     */
    public static int writeUnsignedLong(byte[] target, int offset, long value) {
        return writeLong(target, offset, value ^ Long.MIN_VALUE);
    }

    /**
     * Reads a 64-bit Unsigned Long from the given byte array starting at the given position. This value must have been
     * serialized using {@link #writeUnsignedLong} for proper results. This method is not interoperable with {@link #writeLong}.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static long readUnsignedLong(ArrayView source, int position) {
        return readLong(source, position) ^ Long.MIN_VALUE;
    }
}
