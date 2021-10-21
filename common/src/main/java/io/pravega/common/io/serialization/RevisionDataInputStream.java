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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import io.pravega.common.io.BoundedInputStream;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A [@link DataInputStream} that is used for deserializing Serialization Revisions. Instances of this class should be used to
 * read data that was serialized using an instance of {@link RevisionDataOutput} (i.e., NonSeekableRevisionDataOutput or
 * RandomRevisionDataOutput).
 */
@NotThreadSafe
class RevisionDataInputStream extends DataInputStream implements RevisionDataInput {
    //region Constructor

    /**
     * Creates a new instance of the RevisionDataInputStream class.
     *
     * @param inputStream The InputStream to wrap.
     */
    private RevisionDataInputStream(BoundedInputStream inputStream) {
        super(inputStream);
    }

    /**
     * Creates a new instance of the RevisionDataInputStream class. Upon a successful call to this method, 4 bytes
     * will have been read from the InputStream representing the expected length of the serialization.
     *
     * @param inputStream The InputStream to wrap.
     * @throws IOException If an IO Exception occurred.
     */
    static RevisionDataInputStream wrap(InputStream inputStream) throws IOException {
        int bound = BitConverter.readInt(inputStream);
        return new RevisionDataInputStream(new BoundedInputStream(inputStream, bound));
    }

    //endregion

    //region RevisionDataInput Implementation

    @Override
    public InputStream getBaseStream() {
        return this;
    }

    /**
     * Gets a value representing the length of this InputStream, in bytes, excluding the 4 bytes required for encoding
     * the length.
     */
    @VisibleForTesting
    int getLength() {
        return ((BoundedInputStream) this.in).getBound();
    }

    @Override
    public int getRemaining() {
        return ((BoundedInputStream) this.in).getRemaining();
    }

    @Override
    public long readCompactLong() throws IOException {
        // This uses the DataInput APIs, which will handle throwing EOFExceptions for us, so we don't need to do any more checking.
        // Read first byte and determine how many other bytes are used.
        long b1 = readUnsignedByte();
        int header = (byte) (b1 >>> 6);
        b1 &= 0x3F;

        switch (header) {
            case 0:
                // Only this byte.
                return b1;
            case 1:
                // 2 bytes
                return (b1 << 8) + readUnsignedByte();
            case 2:
                // 4 bytes
                return (b1 << 24)
                        + ((long) readUnsignedByte() << 16)
                        + readUnsignedShort();
            case 3:
                // All 8 bytes
                return (b1 << 56)
                        + ((long) readUnsignedByte() << 48)
                        + ((long) readUnsignedShort() << 32)
                        + (readInt() & 0xFFFF_FFFFL);
            default:
                throw new SerializationException(String.format(
                        "Unable to deserialize compact long. Unrecognized header value %d.", header));
        }
    }

    @Override
    public long readCompactSignedLong() throws IOException {
        // This uses the DataInput APIs, which will handle throwing EOFExceptions for us, so we don't need to do any more checking.
        // Read first byte and determine how many other bytes are used.
        long b1 = readUnsignedByte();
        int header = (byte) (b1 >>> 5);
        b1 &= 0x1F;

        // Determine if negative.
        boolean negative = (header & 0x4) == 0x4;
        if (negative) {
            // Clear the first bit.
            header &= 0x3;
        }

        long value;
        switch (header) {
            case 0:
                // Only this byte.
                value = b1;
                break;
            case 1:
                // 2 bytes
                value = (b1 << 8) + readUnsignedByte();
                break;
            case 2:
                // 4 bytes
                value = (b1 << 24)
                        + ((long) readUnsignedByte() << 16)
                        + readUnsignedShort();
                break;
            case 3:
                // All 8 bytes
                value = (b1 << 56)
                        + ((long) readUnsignedByte() << 48)
                        + ((long) readUnsignedShort() << 32)
                        + (readInt() & 0xFFFF_FFFFL);
                break;
            default:
                throw new SerializationException(String.format(
                        "Unable to deserialize compact signed long. Unrecognized header value %d.", header));
        }

        if (value > RevisionDataOutput.COMPACT_SIGNED_LONG_MAX) {
            throw new SerializationException(String.format(
                    "Unable to deserialize compact signed long. Resulting value (%d) is outside of permissible bounds.",
                    negative ? RevisionDataOutputStream.negateSignedNumber(value) : value));
        }

        return negative ? RevisionDataOutputStream.negateSignedNumber(value) : value;
    }

    @Override
    public int readCompactInt() throws IOException {
        // This uses the DataInput APIs, which will handle throwing EOFExceptions for us, so we don't need to do any more checking.
        // Read first byte and determine how many other bytes are used.
        int b1 = readUnsignedByte();
        if (b1 >>> 7 == 0) {
            // 1 byte.
            return b1;
        } else if ((b1 >>> 6 & 0x1) == 0) {
            // 2 bytes; clear out the 2 MSBs and compose the result by reading 1 additional byte.
            return ((b1 & 0x3F) << 8) + readUnsignedByte();
        } else {
            // All 4 bytes; clear out the 2 MSBs and compose the result by reading 3 additional bytes.
            return ((b1 & 0x3F) << 24)
                    + (readUnsignedByte() << 16)
                    + readUnsignedShort();
        }
    }

    @Override
    public UUID readUUID() throws IOException {
        return new UUID(readLong(), readLong());
    }

    @Override
    public <T> Collection<T> readCollection(ElementDeserializer<T> elementDeserializer) throws IOException {
        return readCollection(elementDeserializer, ArrayList::new);
    }

    @Override
    public <T, C extends Collection<T>> C readCollection(ElementDeserializer<T> elementDeserializer, Supplier<C> newCollection) throws IOException {
        C result = newCollection.get();
        int count = readCompactInt();
        for (int i = 0; i < count; i++) {
            result.add(elementDeserializer.apply(this));
        }
        return result;
    }
    
    @Override
    public <T, C extends ImmutableCollection<T>> void readCollection(ElementDeserializer<T> elementDeserializer, C.Builder<T> builder) throws IOException {
        int count = readCompactInt();
        for (int i = 0; i < count; i++) {
            builder.add(elementDeserializer.apply(this));
        }
    }

    @Override
    public <T> T[] readArray(ElementDeserializer<T> elementDeserializer, IntFunction<T[]> arrayCreator) throws IOException {
        int count = readCompactInt();
        T[] result = arrayCreator.apply(count);
        for (int i = 0; i < count; i++) {
            result[i] = elementDeserializer.apply(this);
        }
        return result;
    }

    @Override
    public byte[] readArray() throws IOException {
        int count = readCompactInt();
        byte[] result = new byte[count];
        readFully(result);
        return result;
    }

    @Override
    public <K, V> Map<K, V> readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer) throws IOException {
        return readMap(keyDeserializer, valueDeserializer, HashMap::new);
    }

    @Override
    public <K, V, M extends Map<K, V>> M readMap(ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer, Supplier<M> newMap) throws IOException {
        M result = newMap.get();
        int count = readCompactInt();
        for (int i = 0; i < count; i++) {
            result.put(keyDeserializer.apply(this), valueDeserializer.apply(this));
        }

        return result;
    }

    @Override
    public <K, V, M extends ImmutableMap<K, V>> void readMap(
            ElementDeserializer<K> keyDeserializer, ElementDeserializer<V> valueDeserializer, 
            M.Builder<K, V> builder) throws IOException {
        int count = readCompactInt();
        for (int i = 0; i < count; i++) {
            builder.put(keyDeserializer.apply(this), valueDeserializer.apply(this));
        }
    }

    //endregion
}