/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Serializes {@link TableEntry} instances.
 */
class EntrySerializer {
    static final int HEADER_LENGTH = 1 + Integer.BYTES * 2; // Version, Key Length, Value Length.
    static final int MAX_KEY_LENGTH = 8 * 1024; // 8KB
    private static final int MAX_SERIALIZATION_LENGTH = 1024 * 1024; // 1MB
    private static final byte CURRENT_SERIALIZATION_VERSION = 0;
    private static final int NO_VALUE = -1;

    //region Updates

    /**
     * Calculates the number of bytes required to serialize the given {@link TableEntry} update.
     *
     * @param entry The {@link TableEntry} to serialize.
     * @return The number of bytes required to serialize.
     */
    int getUpdateLength(@NonNull TableEntry entry) {
        return HEADER_LENGTH + entry.getKey().getKey().getLength() + entry.getValue().getLength();
    }

    /**
     * Serializes the given {@link TableEntry} list into the given byte array.
     *
     * @param entries A List of {@link TableEntry} to serialize.
     * @param target  The byte array to serialize into.
     */
    void serializeUpdate(@NonNull List<TableEntry> entries, byte[] target) {
        int offset = 0;
        for (TableEntry e : entries) {
            offset = serializeUpdate(e, target, offset);
        }
    }

    /**
     * Serializes the given {@link TableEntry} to the given byte array.
     *
     * @param entry        The {@link TableEntry} to serialize.
     * @param target       The byte array to serialize to.
     * @param targetOffset The first offset within the byte array to serialize at.
     * @return The first offset in the given byte array after the serialization.
     */
    private int serializeUpdate(@NonNull TableEntry entry, byte[] target, int targetOffset) {
        val key = entry.getKey().getKey();
        val value = entry.getValue();
        Preconditions.checkArgument(key.getLength() <= MAX_KEY_LENGTH, "Key too large.");
        int serializationLength = getUpdateLength(entry);
        Preconditions.checkArgument(serializationLength <= MAX_SERIALIZATION_LENGTH, "Key+Value serialization too large.");
        Preconditions.checkElementIndex(targetOffset + serializationLength - 1, target.length, "serialization does not fit in target buffer");

        // Serialize Header.
        target[targetOffset] = CURRENT_SERIALIZATION_VERSION;
        targetOffset++;
        targetOffset += BitConverter.writeInt(target, targetOffset, key.getLength());
        targetOffset += BitConverter.writeInt(target, targetOffset, value.getLength());

        // Key
        System.arraycopy(key.array(), key.arrayOffset(), target, targetOffset, key.getLength());
        targetOffset += key.getLength();

        // Value.
        System.arraycopy(value.array(), value.arrayOffset(), target, targetOffset, value.getLength());
        targetOffset += value.getLength();

        return targetOffset;
    }

    //endregion

    //region Removals

    /**
     * Calculates the number of bytes required to serialize the given {@link TableKey} removal.
     *
     * @param key The {@link TableKey} to serialize for removal.
     * @return The number of bytes required to serialize.
     */
    int getRemovalLength(@NonNull TableKey key) {
        return HEADER_LENGTH + key.getKey().getLength();
    }

    /**
     * Serializes the given {@link TableKey} list for removal into the given byte array.
     *
     * @param keys   A List of {@link TableKey} to serialize for removals.
     * @param target The byte array to serialize into.
     */
    void serializeRemoval(@NonNull List<TableKey> keys, byte[] target) {
        int offset = 0;
        for (TableKey e : keys) {
            offset = serializeRemoval(e, target, offset);
        }
    }

    /**
     * Serializes the given {@link TableKey} for removal into the given array.
     *
     * @param tableKey     The {@link TableKey} to serialize.
     * @param target       The byte array to serialize to.
     * @param targetOffset The first offset within the byte array to serialize at.
     * @return The first offset in the given byte array after the serialization.
     */
    private int serializeRemoval(@NonNull TableKey tableKey, byte[] target, int targetOffset) {
        val key = tableKey.getKey();
        Preconditions.checkArgument(key.getLength() <= MAX_KEY_LENGTH, "Key too large.");
        int serializationLength = getRemovalLength(tableKey);
        Preconditions.checkElementIndex(targetOffset + serializationLength - 1, target.length, "serialization does not fit in target buffer");

        // Serialize Header.
        target[targetOffset] = CURRENT_SERIALIZATION_VERSION;
        targetOffset++;
        targetOffset += BitConverter.writeInt(target, targetOffset, key.getLength());
        targetOffset += BitConverter.writeInt(target, targetOffset, NO_VALUE);

        // Key
        System.arraycopy(key.array(), key.arrayOffset(), target, targetOffset, key.getLength());
        return targetOffset + key.getLength();
    }

    //endregion

    //region Headers

    /**
     * Reads the Entry's Header from the given {@link ArrayView}.
     *
     * @param input The {@link ArrayView} to read from.
     * @return The Entry Header.
     * @throws SerializationException If an invalid header was detected.
     */
    Header readHeader(@NonNull ArrayView input) throws SerializationException {
        byte version = input.get(0);
        int keyLength = BitConverter.readInt(input, 1);
        int valueLength = BitConverter.readInt(input, 1 + Integer.BYTES);
        validateHeader(keyLength, valueLength);
        return new Header(version, keyLength, valueLength);
    }

    /**
     * Reads the Entry's Header from the given {@link InputStream}.
     *
     * @param input The {@link InputStream} to read from.
     * @return The Entry Header.
     * @throws IOException If an invalid header was detected or another IOException occurred.
     */
    Header readHeader(@NonNull InputStream input) throws IOException {
        byte version = (byte) input.read();
        int keyLength = BitConverter.readInt(input);
        int valueLength = BitConverter.readInt(input);
        validateHeader(keyLength, valueLength);
        return new Header(version, keyLength, valueLength);
    }

    private void validateHeader(int keyLength, int valueLength) throws SerializationException {
        if (keyLength <= 0 || keyLength > MAX_KEY_LENGTH || (valueLength < 0 && valueLength != NO_VALUE) || keyLength + valueLength > MAX_SERIALIZATION_LENGTH) {
            throw new SerializationException(String.format("Read header with invalid data. KeyLength=%s, ValueLength=%s", keyLength, valueLength));
        }
    }

    /**
     * Defines a serialized Entry's Header.
     */
    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class Header {
        private final byte version;
        private final int keyLength;
        private final int valueLength;

        int getKeyOffset() {
            return HEADER_LENGTH;
        }

        int getValueOffset() {
            Preconditions.checkState(!isDeletion(), "Cannot request value offset for a removal entry.");
            return HEADER_LENGTH + this.keyLength;
        }

        int getTotalLength() {
            return HEADER_LENGTH + this.keyLength + Math.max(0, this.valueLength);
        }

        boolean isDeletion() {
            return this.valueLength == NO_VALUE;
        }

        @Override
        public String toString() {
            return String.format("Length: Key=%s, Value=%s, Total=%s", this.keyLength, this.valueLength, getTotalLength());
        }
    }

    //endregion
}