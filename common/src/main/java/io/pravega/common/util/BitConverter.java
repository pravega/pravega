/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

/**
 * Helper methods for various Number to Bit conversions.
 */
public final class BitConverter {
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
}
