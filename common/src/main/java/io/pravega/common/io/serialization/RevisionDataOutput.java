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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public interface RevisionDataOutput extends DataOutput {
    void length(int length) throws IOException;

    OutputStream getBaseStream();

    default int getUTFLength(String s) {
        // This code is extracted out of DataOutputStream.writeUTF(). If we change the underlying implementation, this
        // needs to change as well.
        int charCount = s.length();
        int length = 0;
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

    default int getCompactLongLength(long value) {
        if (value < 0 || value > 0x3FFF_FFFF_FFFF_FFFFL) {
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

    default void writeCompactLong(long value) throws IOException {
        if (value < 0 || value > 0x3FFF_FFFF_FFFF_FFFFL) {
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
            writeShort((short) (value | 0x4000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    default int getCompactIntLength(long value) {
        if (value < 0 || value > 0x3FFF_FFFF) {
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

    default void writeCompactInt(int value) throws IOException {
        if (value < 0 || value > 0x3FFF_FFFF) {
            throw new IllegalArgumentException("writeCompactInt can only serialize non-negative longs up to 2^30.");
        } else if (value > 0x3F_FFFF) {
            // All 4 bytes
            writeInt(value | 0xC000_0000);
        } else if (value > 0x3FFF) {
            // 3 bytes.
            writeByte((byte) (value >>> 16 & 0xFF | 0x80));
            writeShort((short) value);
        } else if (value > 0x3F) {
            // 2 Bytes.
            writeShort((short) (value | 0x4000));
        } else {
            // 1 byte.
            writeByte((byte) value);
        }
    }

    default void writeUUID(UUID uuid) throws IOException {
        writeLong(uuid.getMostSignificantBits());
        writeLong(uuid.getLeastSignificantBits());
    }

    default <T> void writeCollection(Collection<T> collection, ElementSerializer<T> elementSerializer) throws IOException {
        if (collection == null) {
            writeInt(0);
            return;
        }

        writeInt(collection.size());
        for (T e : collection) {
            elementSerializer.accept(this, e);
        }
    }

    default <K, V> void writeMap(Map<K, V> map, ElementSerializer<K> keySerializer, ElementSerializer<V> valueSerializer) throws IOException {
        if (map == null) {
            writeInt(0);
            return;
        }

        writeInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet()) {
            keySerializer.accept(this, e.getKey());
            valueSerializer.accept(this, e.getValue());
        }
    }

    @FunctionalInterface
    interface ElementSerializer<T> {
        void accept(RevisionDataOutput dataOutput, T element) throws IOException;
    }

    //region Factory

    static CloseableRevisionDataOutput wrap(OutputStream outputStream) throws IOException {
        if (outputStream instanceof RandomOutputStream) {
            return new RandomRevisionDataOutput(outputStream);
        } else {
            return new NonSeekableRevisionDataOutput(outputStream);
        }
    }

    interface CloseableRevisionDataOutput extends RevisionDataOutput, AutoCloseable {
        @Override
        void close() throws IOException;
    }

    //endregion
}
