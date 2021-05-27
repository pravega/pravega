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
package io.pravega.common.io.serialization;

import com.google.common.base.Preconditions;
import io.pravega.common.io.DirectDataOutput;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToIntFunction;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;

/**
 * RevisionDataOutput implementation that mimics the encoding in {@link DataOutputStream}.
 */
@NotThreadSafe
abstract class RevisionDataOutputStream extends FilterOutputStream implements RevisionDataOutput {
    //region Members

    private DirectDataOutput structuredWriter;
    @Getter
    private int size;

    //endregion

    //region Constructor

    private RevisionDataOutputStream(OutputStream outputStream) {
        super(outputStream);
        this.structuredWriter = (out instanceof DirectDataOutput) ? (DirectDataOutput) out : new IndirectWriter();
        this.size = 0;
    }

    protected final void setOut(OutputStream out, int length) throws IOException {
        super.out = out;
        this.structuredWriter = (out instanceof DirectDataOutput) ? (DirectDataOutput) out : new IndirectWriter();
        this.structuredWriter.writeInt(length);
    }

    /**
     * Wraps the given OutputStream into a specific implementation of RevisionDataOutputStream.
     *
     * @param outputStream The OutputStream to wrap.
     * @return A new instance of a RevisionDataOutputStream sub-class, depending on whether the given OutputStream is a
     * {@link RandomAccessOutputStream} (supports seeking) or not.
     * @throws IOException If an IO Exception occurred. This is because if the given OutputStream is a {@link RandomAccessOutputStream},
     *                     this will pre-allocate 4 bytes for the length.
     */
    public static RevisionDataOutputStream wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomAccessOutputStream) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    //endregion

    //region DataOutput Implementation

    @Override
    public void flush() throws IOException {
        this.out.flush();
    }

    @Override
    public void write(int b) throws IOException {
        this.out.write(b);
        this.size++;
    }

    @Override
    public void write(byte[] array, int off, int len) throws IOException {
        this.out.write(array, off, len);
        this.size += len;
    }

    @Override
    public final void writeBoolean(boolean value) throws IOException {
        write(value ? 1 : 0);
    }

    @Override
    public final void writeByte(int b) throws IOException {
        write(b);
    }

    @Override
    public void writeChar(int c) throws IOException {
        writeShort(c);
    }

    @Override
    public void writeShort(int s) throws IOException {
        this.structuredWriter.writeShort(s);
        this.size += Short.BYTES;
    }

    @Override
    public void writeInt(int i) throws IOException {
        this.structuredWriter.writeInt(i);
        this.size += Integer.BYTES;
    }

    @Override
    public void writeLong(long l) throws IOException {
        this.structuredWriter.writeLong(l);
        this.size += Long.BYTES;
    }

    @Override
    public void writeFloat(float f) throws IOException {
        writeInt(Float.floatToIntBits(f));
    }

    @Override
    public void writeDouble(double d) throws IOException {
        writeLong(Double.doubleToLongBits(d));
    }

    @Override
    public void writeBytes(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; ++i) {
            this.out.write((byte) s.charAt(i));
        }

        this.size += len;
    }

    @Override
    public final void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; ++i) {
            writeChar(s.charAt(i));
        }
    }

    @Override
    public final void writeUTF(String str) throws IOException {
        // Note: this method was borrowed from DataOutputStream without any changes.
        final int stringLength = str.length();
        final int utfLength = getUTFLength(str) - 2;
        Preconditions.checkArgument(utfLength <= 65535, "Encoded string too long: %s bytes", utfLength);

        byte[] byteArray = new byte[utfLength + 2];
        int index = 0;
        byteArray[index++] = (byte) ((utfLength >>> 8) & 0xFF);
        byteArray[index++] = (byte) ((utfLength >>> 0) & 0xFF);

        int c;
        int i;
        for (i = 0; i < stringLength; i++) {
            c = str.charAt(i);
            if (c < 1 || c > 127) {
                break;
            }
            byteArray[index++] = (byte) c;
        }

        for (; i < stringLength; i++) {
            c = str.charAt(i);
            if (c >= 1 && c <= 127) {
                byteArray[index++] = (byte) c;
            } else if (c > 2047) {
                byteArray[index++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                byteArray[index++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                byteArray[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            } else {
                byteArray[index++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                byteArray[index++] = (byte) (0x80 | ((c >> 0) & 0x3F));
            }
        }

        write(byteArray);
    }

    //endregion

    //region RevisionDataOutput Implementation

    @Override
    public int getUTFLength(String s) {
        // This code is extracted out of DataOutputStream.writeUTF(). If we change the underlying implementation, this
        // needs to change as well.
        int charCount = s.length();
        int length = 2; // writeUTF() will also encode a 2-byte length.
        for (int i = 0; i < charCount; ++i) {
            char c = s.charAt(i);
            if (c >= 1 && c <= 127) {
                length++;
            } else if (c > 2047) {
                length += 3;
            } else {
                length += 2;
            }
        }
        return length;
    }

    @Override
    public int getCompactLongLength(long value) {
        if (value < COMPACT_LONG_MIN || value > COMPACT_LONG_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactLong", "longs", "[0, 2^62)", value));
        } else if (value > 0x3FFF_FFFF) {
            return 8;
        } else if (value > 0x3FFF) {
            return 4;
        } else if (value > 0x3F) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * {@inheritDoc}
     * Encodes the given value as a compact long, using the following scheme (MSB=Most Significant Bits):
     * * MSB = 00 for values in [0, 0x3F], with a 1-byte encoding.
     * * MSB = 01 for values in (0x3F, 0x3FFF], with a 2-byte encoding (2 MSB are reserved, leaving 14 bits usable).
     * * MSB = 10 for values in (0x3FFF, 0x3FFF_FFFF], with a 4-byte encoding (2 MSB are reserved, leaving 30 bits usable).
     * * MSB = 11 for values in (0x3FFF_FFFF, 0x3FFF_FFFF_FFFF_FFFFL], with an 8-byte encoding (2 MSB are reserved, leaving 62 bits usable).
     *
     * @param value The value to encode.
     */
    @Override
    public void writeCompactLong(long value) throws IOException {
        if (value < COMPACT_LONG_MIN || value > COMPACT_LONG_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactLong", "longs", "[0, 2^62)", value));
        } else if (value > 0x3FFF_FFFF) {
            // All 8 bytes
            writeInt((int) (value >>> 32 | 0xC000_0000));
            writeInt((int) value);
        } else if (value > 0x3FFF) {
            // Only 4 bytes.
            writeInt((int) (value | 0x8000_0000));
        } else if (value > 0x3F) {
            // Only 2 bytes.
            writeShort((short) (value | 0x4000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    @Override
    public int getCompactSignedLongLength(long value) {
        if (value < COMPACT_SIGNED_LONG_MIN || value > COMPACT_SIGNED_LONG_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactSignedLong", "longs", "[-2^61, 2^61)", value));
        }

        if (value < 0) {
            value = negateSignedNumber(value);
        }

        if (value > 0x1FFF_FFFF) {
            return 8;
        } else if (value > 0x1FFF) {
            return 4;
        } else if (value > 0x1F) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * {@inheritDoc}
     * Encodes the given value as a compact long, using the following scheme (MSB=Most Significant Bits).
     * * MSB[0] = 1 for negative values and 0 for positive values.
     * * MSB[1-2] = 00 if abs(value) in [0, 0x1F], with a 1-byte encoding.
     * * MSB[1-2] = 01 if abs(value) in (0x1F, 0x1FFF], with a 2-byte encoding (3 MSB are reserved, leaving 13 bits usable).
     * * MSB[1-2] = 10 if abs(value) in (0x1FFF, 0x1FFF_FFFF], with a 4-byte encoding (3 MSB are reserved, leaving 29 bits usable).
     * * MSB[1-2] = 11 if abs(value) in (0x1FFF_FFFF, 0x1FFF_FFFF_FFFF_FFFFL], with an 8-byte encoding (3 MSB are reserved, leaving 61 bits usable).
     *
     * @param value The value to encode.
     */
    @Override
    public void writeCompactSignedLong(long value) throws IOException {
        if (value < COMPACT_SIGNED_LONG_MIN || value > COMPACT_SIGNED_LONG_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactSignedLong", "longs", "[-2^61, 2^61)", value));
        } else {
            boolean negative = value < 0;
            if (negative) {
                // Transform the value into a positive one.
                value = negateSignedNumber(value);
            }

            if (value > 0x1FFF_FFFF) {
                // All 8 bytes
                writeInt((int) (value >>> 32 | (negative ? 0xE000_0000 : 0x6000_0000)));
                writeInt((int) value);
            } else if (value > 0x1FFF) {
                // Only 4 bytes.
                writeInt((int) (value | (negative ? 0xC000_0000 : 0x4000_0000)));
            } else if (value > 0x1F) {
                // Only 2 bytes.
                writeShort((short) (value | (negative ? 0xA000 : 0x2000)));
            } else if (negative) {
                // 1 byte.
                writeByte((byte) value | 0x80);
            } else {
                // 1 byte.
                writeByte((byte) value);
            }
        }
    }

    @Override
    public int getCompactIntLength(int value) {
        if (value < COMPACT_INT_MIN || value > COMPACT_INT_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactInt", "ints", "[0, 2^30)", value));
        } else if (value > 0x3FFF) {
            return 4;
        } else if (value > 0x7F) {
            return 2;
        } else {
            return 1;
        }
    }

    /**
     * {@inheritDoc}
     * Encodes the given value as a compact integer, using the following scheme (MSB=Most Significant Bits):
     * * MSB = 0 for values in [0, 0x7F], with a 1-byte encoding.
     * * MSB = 10 for values in (0x7F, 0x3FFF], with a 2-byte encoding (2 MSB are reserved, leaving 14 bits usable).
     * * MSB = 11 for values in (0x3FFF, 0x3FFF_FFFF], with a 4-byte encoding (2 MSB are reserved, leaving 30 bits usable).
     *
     * @param value The value to encode.
     */
    @Override
    public void writeCompactInt(int value) throws IOException {
        // MSB: 0  -> 1 byte with the remaining 7 bits
        // MSB: 10 -> 2 bytes with the remaining 6+8 bits
        // MSB: 11 -> 4 bytes with the remaining 6+8+8+8 bits
        if (value < COMPACT_INT_MIN || value > COMPACT_INT_MAX) {
            throw new IllegalArgumentException(badArgRange("writeCompactInt", "ints", "[0, 2^30)", value));
        } else if (value > 0x3FFF) {
            // All 4 bytes
            writeInt(value | 0xC000_0000);
        } else if (value > 0x7F) {
            // 2 Bytes.
            writeShort((short) (value | 0x8000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    /**
     * {@inheritDoc}
     * Encodes the given UUID as a sequence of 2 Longs, withe the Most Significant Bits first, followed by Least
     * Significant bits.
     *
     * @param uuid The value to encode.
     */
    @Override
    public void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    @Override
    public int getCollectionLength(int elementCount, int elementLength) {
        return getCompactIntLength(elementCount) + elementCount * elementLength;
    }

    @Override
    public <T> int getCollectionLength(Collection<T> collection, ToIntFunction<T> elementLengthProvider) {
        if (collection == null) {
            return getCompactIntLength(0);
        }

        return getCompactIntLength(collection.size()) + collection.stream().mapToInt(elementLengthProvider).sum();
    }

    @Override
    public <T> int getCollectionLength(T[] array, ToIntFunction<T> elementLengthProvider) {
        if (array == null) {
            return getCompactIntLength(0);
        }

        return getCompactIntLength(array.length) + Arrays.stream(array).mapToInt(elementLengthProvider).sum();
    }

    @Override
    public <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException {
        if (collection == null) {
            writeCompactInt(0);
            return;
        }

        writeCompactInt(collection.size());
        for (T e : collection) {
            elementSerializer.accept(this, e);
        }
    }

    @Override
    public <T> void writeArray(T[] array, ElementSerializer<T> elementSerializer) throws IOException {
        if (array == null) {
            writeCompactInt(0);
            return;
        }

        writeCompactInt(array.length);
        for (T e : array) {
            elementSerializer.accept(this, e);
        }
    }

    @Override
    public void writeArray(byte[] array, int offset, int length) throws IOException {
        if (array == null) {
            // We ignore offset and length in this case, as per the method's contract.
            writeCompactInt(0);
            return;
        } else if (offset < 0 || offset > array.length || length < 0 || offset + length > array.length) {
            throw new ArrayIndexOutOfBoundsException("offset and length must refer to a range within the given array.");
        }

        writeCompactInt(length);
        write(array, offset, length);
    }

    @Override
    public void writeBuffer(BufferView buf) throws IOException {
        if (buf == null) {
            // Null will be deserialized as an empty array, so write 0 as length.
            writeCompactInt(0);
            return;
        }

        // Write Length.
        writeCompactInt(buf.getLength());

        // Copy the buffer contents to this OutputStream. This will write all its bytes.
        this.structuredWriter.writeBuffer(buf);

        // Increase our size here regardless of what structuredWriter does. See IndirectWriter.writerBuffer for how
        // this is offset in certain cases.
        this.size += buf.getLength();
    }

    @Override
    public int getMapLength(int elementCount, int keyLength, int valueLength) {
        return getCompactIntLength(elementCount) + elementCount * (keyLength + valueLength);
    }

    @Override
    public <K, V> int getMapLength(Map<K, V> map, ToIntFunction<K> keyLengthProvider, ToIntFunction<V> valueLengthProvider) {
        if (map == null) {
            return getCompactIntLength(0);
        }

        return getCompactIntLength(map.size())
                + map.entrySet().stream()
                     .mapToInt(e -> keyLengthProvider.applyAsInt(e.getKey()) + valueLengthProvider.applyAsInt(e.getValue()))
                     .sum();
    }

    @Override
    public <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException {
        if (map == null) {
            writeCompactInt(0);
            return;
        }

        writeCompactInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet()) {
            keySerializer.accept(this, e.getKey());
            valueSerializer.accept(this, e.getValue());
        }
    }

    private <T> String badArgRange(String methodName, String type, String interval, T arg) {
        return String.format("%s can only serialize %s in the interval %s, given %s.", methodName, type, interval, arg);
    }

    /**
     * Transforms a number belonging to a range of [A, B] into an equivalent number in the range [-B-1, -A-1]. This
     * transformation is reversible (X=negate(negate(X)) and is useful for encoding negative compacted numbers and will
     * not work for {@link Long#MIN_VALUE}.
     *
     * @param value The value to encode.
     * @return The negated value.
     */
    static long negateSignedNumber(long value) {
        return -value - 1;
    }

    //endregion

    //region IndirectWriter

    /**
     * Structured data writer for cases when the underlying {@link OutputStream} does not support {@link DirectDataOutput}.
     */
    private class IndirectWriter implements DirectDataOutput {
        @Override
        public void writeShort(int shortValue) throws IOException {
            BitConverter.writeShort(out, (short) shortValue);
        }

        @Override
        public void writeInt(int intValue) throws IOException {
            BitConverter.writeInt(out, intValue);
        }

        @Override
        public void writeLong(long longValue) throws IOException {
            BitConverter.writeLong(out, longValue);
        }

        @Override
        public void writeBuffer(BufferView buffer) throws IOException {
            buffer.copyTo(RevisionDataOutputStream.this);
            // BufferView.copyTo will cause our "size" to increase, but an external DirectDataOutput will not do that.
            // To avoid wrapping the external DirectDataOutput with another wrapper, we increase the size in
            // RevisionDataOutputStream.writeBuffer() regardless of case, so we need to make an adjustment here to offset
            // for that.
            size -= buffer.getLength();
        }
    }

    //endregion

    //region Implementations

    /**
     * RevisionDataOutput implementation that writes to a RandomAccessOutputStream OutputStream. This does not force the caller to
     * explicitly declare the length prior to serialization as it can be back-filled upon closing.
     */
    private static class RandomRevisionDataOutput extends RevisionDataOutputStream {
        private final int initialPosition;

        /**
         * Creates a new instance of the RandomRevisionDataOutput class. Upon a successful call to this constructor, 4 bytes
         * will have been written to the OutputStream representing a placeholder for the length. These 4 bytes will be populated
         * upon closing this OutputStream.
         *
         * @param outputStream The OutputStream to wrap.
         * @throws IOException If an IO Exception occurred.
         */
        RandomRevisionDataOutput(OutputStream outputStream) throws IOException {
            super(outputStream);

            // Pre-allocate 4 bytes so we can write the length later, but remember this position.
            RandomAccessOutputStream ros = (RandomAccessOutputStream) outputStream;
            this.initialPosition = ros.size();
            ros.writeInt(0);
        }

        @Override
        public void close() throws IOException {
            // Calculate the number of bytes written, making sure to exclude the bytes for the length encoding.
            RandomAccessOutputStream ros = (RandomAccessOutputStream) this.out;
            int length = ros.size() - this.initialPosition - Integer.BYTES;

            // Write the length at the appropriate position.
            ros.writeInt(length, this.initialPosition);
        }

        @Override
        public OutputStream getBaseStream() {
            // We need to return an OutputStream that implements RandomAccessOutputStream, which is our underlying OutputStream (and not us).
            return this.out;
        }

        @Override
        public boolean requiresExplicitLength() {
            return false;
        }

        @Override
        public void length(int length) throws IOException {
            // Nothing to do.
        }
    }

    /**
     * RevisionDataOutput implementation that writes to a general OutputStream. This will force the caller to explicitly
     * calculate and declare the length prior to serialization as it cannot be back-filled upon closing.
     */
    @NotThreadSafe
    private static class NonSeekableRevisionDataOutput extends RevisionDataOutputStream {
        private final OutputStream realStream;
        private int length;

        NonSeekableRevisionDataOutput(OutputStream outputStream) {
            super(new LengthRequiredOutputStream());
            this.realStream = outputStream;
            this.length = 0;
        }

        @Override
        public void close() throws IOException {
            // We do not want to close the underlying Stream as it may be reused.
            if (this.length != getSize()) {
                // Check if we wrote the number of bytes we declared, otherwise we will have problems upon deserializing.
                throw new SerializationException(String.format("Unexpected number of bytes written. Declared: %d, written: %d.", this.length, getSize()));
            } else if (requiresExplicitLength()) {
                // We haven't written anything nor declared a length. Write the length prior to exiting.
                length(0);
            }
        }

        @Override
        public OutputStream getBaseStream() {
            return this;
        }

        @Override
        public boolean requiresExplicitLength() {
            // We only require the Length to be declared once; after it's been set there's no need to set it again.
            return this.out instanceof LengthRequiredOutputStream;
        }

        @Override
        public void length(int length) throws IOException {
            if (requiresExplicitLength()) {
                setOut(this.realStream, length);
                this.length = length;
            }
        }

        private static class LengthRequiredOutputStream extends OutputStream {
            @Override
            public void write(int i) {
                throw new IllegalStateException("Length must be declared prior to writing anything.");
            }

            @Override
            public void write(byte[] buffer, int index, int length) {
                throw new IllegalStateException("Length must be declared prior to writing anything.");
            }
        }
    }

    //endregion
}
