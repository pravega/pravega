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

import io.pravega.common.io.ByteBufferOutputStream;
import io.pravega.common.io.DirectDataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Helper methods for various Number to Bit conversions.
 *
 * IMPORTANT: This class is closed to new additions. While not deprecated (it's good to have similar methods in one place),
 * consider using one of the following instead:
 * - {@link StructuredWritableBuffer} or {@link StructuredReadableBuffer} implementations, such as
 * {@link ByteArraySegment}). These provide the most efficient serialization implementations available.
 * - {@link DirectDataOutput} implementations, such as {@link ByteBufferOutputStream}). This provides an efficient
 * serialization implementation as well.
 * - {@link java.io.DataOutputStream} if none of the above are useful (this is the classic Java serializer).
 */
public final class BitConverter {
    /**
     * Writes the given 16-bit Short to the given OutputStream.
     *
     * @param target The OutputStream to write to.
     * @param value  The value to write.
     * @return The number of bytes written.
     * @throws IOException If an error occurred.
     */
    public static int writeShort(OutputStream target, short value) throws IOException {
        target.write((byte) (value >>> 8 & 255));
        target.write((byte) (value & 255));
        return Short.BYTES;
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
     * Reads a 32-bit integer from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static int readInt(byte[] source, int position) {
        return makeInt(source[position], source[position + 1], source[position + 2], source[position + 3]);
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
            return makeInt(b1, b2, b3, b4);
        }
    }

    /**
     * Composes a 32-bit integer from the given byte components (Big-Endian order).
     *
     * @param b1 Byte #1.
     * @param b2 Byte #2.
     * @param b3 Byte #3.
     * @param b4 Byte #4.
     * @return The composed number.
     */
    public static int makeInt(int b1, int b2, int b3, int b4) {
        return (b1 & 0xFF) << 24
                | (b2 & 0xFF) << 16
                | (b3 & 0xFF) << 8
                | (b4 & 0xFF);
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
     * Writes the given 64-bit Long to the given OutputStream.
     *
     * @param target The OutputStream to write to.
     * @param value  The value to write.
     * @return The number of bytes written.
     * @throws IOException If an exception got thrown.
     */
    public static int writeLong(OutputStream target, long value) throws IOException {
        target.write((byte) (value >>> 56));
        target.write((byte) (value >>> 48));
        target.write((byte) (value >>> 40));
        target.write((byte) (value >>> 32));
        target.write((byte) (value >>> 24));
        target.write((byte) (value >>> 16));
        target.write((byte) (value >>> 8));
        target.write((byte) value);
        return Long.BYTES;
    }

    /**
     * Writes the given 128-bit UUID to the given {@link StructuredWritableBuffer}.
     *
     * @param b     The {@link StructuredWritableBuffer} to write to.
     * @param value The value to write.
     */
    public static void writeUUID(StructuredWritableBuffer b, UUID value) {
        b.setLong(0, value.getMostSignificantBits());
        b.setLong(Long.BYTES, value.getLeastSignificantBits());
    }

    /**
     * Reads a 128-bit UUID from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read UUID.
     */
    public static UUID readUUID(byte[] source, int position) {
        long msb = readLong(source, position);
        long lsb = readLong(source, position + Long.BYTES);
        return new UUID(msb, lsb);
    }

    /**
     * Reads a 64-bit long from the given byte array starting at the given position.
     *
     * @param source   The byte array to read from.
     * @param position The position in the byte array to start reading at.
     * @return The read number.
     */
    public static long readLong(byte[] source, int position) {
        return makeLong(source[position], source[position + 1], source[position + 2], source[position + 3],
                source[position + 4], source[position + 5], source[position + 6], source[position + 7]);
    }

    /**
     * Composes 64-bit long from the given byte components (in Big Endian order).
     *
     * @param b1 Byte #1.
     * @param b2 Byte #2.
     * @param b3 Byte #3.
     * @param b4 Byte #4.
     * @param b5 Byte #5.
     * @param b6 Byte #6.
     * @param b7 Byte #7.
     * @param b8 Byte #8.
     * @return The composed number.
     */
    @SuppressWarnings("cast")
    public static long makeLong(int b1, int b2, int b3, int b4, int b5, int b6, int b7, int b8) {
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
