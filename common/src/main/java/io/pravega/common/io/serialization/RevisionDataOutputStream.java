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

import io.pravega.common.util.BitConverter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * RevisionDataOutput implementation that makes use of the java.io.DataOutputStream for data encoding.
 */
@NotThreadSafe
abstract class RevisionDataOutputStream extends DataOutputStream implements RevisionDataOutput {
    //region Constructor

    private RevisionDataOutputStream(OutputStream outputStream) {
        super(outputStream);
    }

    /**
     * Wraps the given OutputStream into a specific implementation of RevisionDataOutputStream.
     *
     * @param outputStream The OutputStream to wrap.
     * @return A new instance of a RevisionDataOutputStream sub-class, depending on whether the given OutputStream is a
     * RandomOutput (supports seeking) or not.
     * @throws IOException If an IO Exception occurred. This is because if the given OutputStream is a RandomOutput, this
     *                     will pre-allocate 4 bytes for the length.
     */
    public static RevisionDataOutputStream wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomOutput) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() throws IOException {
        // Nothing to do. We do not want to close the underlying Stream.
    }

    //endregion

    //region RevisionDataOutput Implementation

    @Override
    public OutputStream getBaseStream() {
        return this.out;
    }

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
        if (value < COMPACT_LONG_MIN || value >= COMPACT_LONG_MAX) {
            throw new IllegalArgumentException("writeCompactLong can only serialize non-negative longs up to 2^62.");
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

    @Override
    public void writeCompactLong(long value) throws IOException {
        if (value < COMPACT_LONG_MIN || value >= COMPACT_LONG_MAX) {
            throw new IllegalArgumentException("writeCompactLong can only serialize non-negative longs up to 2^62.");
        } else if (value > 0x3FFF_FFFF) {
            // All 8 bytes
            writeInt((int) (value >>> 32 | 0xC000_0000));
            writeInt((int) value);
        } else if (value > 0x3FFF) {
            // Only 4 bytes.
            writeInt((int) (value | 0x8000_0000));
        } else if (value > 0x3F) {
            // Only 2 bytes.
            this.out.write((int) (value >>> 8 | 0x40 & 0xFF));
            this.out.write((int) (value & 0xFF));
        } else {
            // 1 byte.
            this.out.write((byte) value);
        }
    }

    @Override
    public int getCompactIntLength(int value) {
        if (value < COMPACT_INT_MIN || value >= COMPACT_INT_MAX) {
            throw new IllegalArgumentException("writeCompactInt can only serialize non-negative longs up to 2^30.");
        } else if (value > 0x3F_FFFF) {
            return 4;
        } else if (value > 0x3FFF) {
            return 3;
        } else if (value > 0x3F) {
            return 2;
        } else {
            return 1;
        }
    }

    @Override
    public void writeCompactInt(int value) throws IOException {
        if (value < COMPACT_INT_MIN || value >= COMPACT_INT_MAX) {
            throw new IllegalArgumentException("writeCompactInt can only serialize non-negative longs up to 2^30.");
        } else if (value > 0x3F_FFFF) {
            // All 4 bytes
            this.out.write(value >>> 24 | 0xC0 & 0xFF);
            this.out.write(value >>> 16 & 0xFF);
            this.out.write(value >>> 8 & 0xFF);
        } else if (value > 0x3FFF) {
            // 3 bytes.
            this.out.write(value >>> 16 | 0x80 & 0xFF);
            this.out.write(value >>> 8 & 0xFF);
        } else if (value > 0x3F) {
            // 2 Bytes.
            this.out.write(value >>> 8 | 0x40 & 0xFF);
        }

        // Last byte is always the same.
        this.out.write(value & 0xFF);
    }

    @Override
    public void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
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

    //endregion

    //region Implementations

    /**
     * RevisionDataOutput implementation that writes to a RandomOutput OutputStream. This does not force the caller to
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
            this.initialPosition = ((RandomOutput) outputStream).size();
            BitConverter.writeInt(outputStream, 0);
        }

        @Override
        public void close() throws IOException {
            // Calculate the number of bytes written, making sure to exclude the bytes for the length encoding.
            RandomOutput ros = (RandomOutput) this.out;
            int length = ros.size() - this.initialPosition - Integer.BYTES;

            // Write the length at the appropriate position.
            BitConverter.writeInt(ros.subStream(this.initialPosition, Integer.BYTES), length);
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
    private static class NonSeekableRevisionDataOutput extends RevisionDataOutputStream {
        private final OutputStream realStream;

        NonSeekableRevisionDataOutput(OutputStream outputStream) {
            super(new LengthRequiredOutputStream());
            this.realStream = outputStream;
        }

        @Override
        public boolean requiresExplicitLength() {
            return true;
        }

        @Override
        public void length(int length) throws IOException {
            if (this.out instanceof LengthRequiredOutputStream) {
                BitConverter.writeInt(this.realStream, length);
                super.out = this.realStream;
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
