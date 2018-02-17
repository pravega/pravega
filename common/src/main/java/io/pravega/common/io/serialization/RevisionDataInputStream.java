/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.serialization;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BitConverter;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A DataInputStream that is used for deserializing Serialization Revisions. Instances of this class should be used to
 * read data that was serialized using an instance of RevisionDataOutput (i.e., NonSeekableRevisionDataOutput or
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
        return ((BoundedInputStream) this.in).bound;
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
    public int readCompactInt() throws IOException {
        // This uses the DataInput APIs, which will handle throwing EOFExceptions for us, so we don't need to do any more checking.
        // Read first byte and determine how many other bytes are used.
        int b1 = readUnsignedByte();
        int header = b1 >>> 6;
        b1 &= 0x3F;

        switch (header) {
            case 0:
                // Only this byte.
                return b1;
            case 1:
                // 2 bytes
                return (b1 << 8) + readUnsignedByte();
            case 2:
                // 3 bytes
                return (b1 << 16)
                        + readUnsignedShort();
            case 3:
                // All 4 bytes
                return (b1 << 24)
                        + (readUnsignedByte() << 16)
                        + readUnsignedShort();
            default:
                throw new SerializationException(String.format(
                        "Unable to deserialize compact int. Unrecognized header value %d.", header));
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

    //endregion

    //region BoundedInputStream Implementation

    /**
     * InputStream wrapper that counts how many bytes were read and prevents over-reading.
     */
    private static class BoundedInputStream extends FilterInputStream {
        private final int bound;
        private int remaining;

        BoundedInputStream(InputStream inputStream, int bound) {
            super(inputStream);
            this.bound = bound;
            this.remaining = bound;
        }

        @Override
        public void close() throws IOException {
            // Skip over the remaining bytes. Do not close the underlying InputStream.
            if (this.remaining > 0) {
                int toSkip = this.remaining;
                long skipped = skip(toSkip);
                if (skipped != toSkip) {
                    throw new SerializationException(String.format("Read %d fewer byte(s) than expected only able to skip %d.", toSkip, skipped));
                }
            } else if (this.remaining < 0) {
                throw new SerializationException(String.format("Read more bytes than expected (%d).", -this.remaining));
            }
        }

        @Override
        public int read() throws IOException {
            // Do not allow reading more than we should.
            int r = this.remaining > 0 ? super.read() : -1;
            if (r >= 0) {
                this.remaining--;
            }

            return r;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int readLength = Math.min(length, this.remaining);
            int r = this.in.read(buffer, offset, readLength);
            if (r > 0) {
                this.remaining -= r;
            } else if (length > 0 && this.remaining <= 0) {
                // We have reached our bound.
                return -1;
            }
            return r;
        }

        @Override
        public long skip(long count) throws IOException {
            long r = this.in.skip(Math.min(count, this.remaining));
            this.remaining -= r;
            return r;
        }

        @Override
        public int available() throws IOException {
            return Math.min(this.in.available(), this.remaining);
        }
    }

    //endregion
}