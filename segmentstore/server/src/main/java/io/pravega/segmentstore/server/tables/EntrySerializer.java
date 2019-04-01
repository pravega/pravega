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
import java.util.Collection;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Serializes {@link TableEntry} instances.
 * The format is:
 * - Header: Entry Serialization Version (1 byte), the Key Length (4 bytes) and the Value Length (4 bytes). Value length
 * is negative if this serialization represents a deletion.
 * - Key: one or more bytes representing the Key.
 * - Value: zero (if empty or a deletion) or more bytes representing the Value.
 *
 * We can't use VersionedSerializer here because in most cases we need to read only key and not the value. VersionedSerializer
 * requires us to read the whole thing before retrieving anything.
 */
class EntrySerializer {
    static final int HEADER_LENGTH = 1 + Integer.BYTES * 2 + Long.BYTES; // Serialization Version, Key Length, Value Length, Entry Version.
    static final int MAX_KEY_LENGTH = 8 * 1024; // 8KB
    static final int MAX_SERIALIZATION_LENGTH = 1024 * 1024; // 1MB
    private static final int VERSION_POSITION = 0;
    private static final int KEY_POSITION = VERSION_POSITION + 1;
    private static final int VALUE_POSITION = KEY_POSITION + Integer.BYTES;
    private static final int ENTRY_VERSION_POSITION = VALUE_POSITION + Integer.BYTES;
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
     * Serializes the given {@link TableEntry} collection into the given byte array, without explicitly recording the
     * versions for each entry.
     *
     * @param entries A Collection of {@link TableEntry} to serialize.
     * @param target  The byte array to serialize into.
     */
    void serializeUpdate(@NonNull Collection<TableEntry> entries, byte[] target) {
        int offset = 0;
        for (TableEntry e : entries) {
            offset = serializeUpdate(e, target, offset, TableKey.NO_VERSION);
        }
    }

    /**
     * Serializes the given {@link TableEntry} collection into the given byte array, explicitly recording the versions
     * for each entry ({@link TableKey#getVersion()}). This should be used for {@link TableEntry} instances that were
     * previously read from the Table Segment as only in that case does the version accurately reflect the entry's version.
     *
     * @param entries A Collection of {@link TableEntry} to serialize.
     * @param target  The byte array to serialize into.
     */
    void serializeUpdateWithExplicitVersion(@NonNull Collection<TableEntry> entries, byte[] target) {
        int offset = 0;
        for (TableEntry e : entries) {
            offset = serializeUpdate(e, target, offset, e.getKey().getVersion());
        }
    }

    /**
     * Serializes the given {@link TableEntry} to the given byte array.
     *
     * @param entry        The {@link TableEntry} to serialize.
     * @param target       The byte array to serialize to.
     * @param targetOffset The first offset within the byte array to serialize at.
     * @param version      The version to serialize. This will be encoded in the {@link Header}.
     * @return The first offset in the given byte array after the serialization.
     */
    private int serializeUpdate(@NonNull TableEntry entry, byte[] target, int targetOffset, long version) {
        val key = entry.getKey().getKey();
        val value = entry.getValue();
        Preconditions.checkArgument(key.getLength() <= MAX_KEY_LENGTH, "Key too large.");
        int serializationLength = getUpdateLength(entry);
        Preconditions.checkArgument(serializationLength <= MAX_SERIALIZATION_LENGTH, "Key+Value serialization too large.");
        Preconditions.checkElementIndex(targetOffset + serializationLength - 1, target.length, "serialization does not fit in target buffer");

        // Serialize Header.
        targetOffset = writeHeader(target, targetOffset, key.getLength(), value.getLength(), version);

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
     * Serializes the given {@link TableKey} collection for removal into the given byte array.
     *
     * @param keys   A Collection of {@link TableKey} to serialize for removals.
     * @param target The byte array to serialize into.
     */
    void serializeRemoval(@NonNull Collection<TableKey> keys, byte[] target) {
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

        // Serialize Header. Not caring about explicit versions since we do not reinsert removals upon compaction.
        targetOffset = writeHeader(target, targetOffset, key.getLength(), NO_VALUE, TableKey.NO_VERSION);

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
        byte version = input.get(VERSION_POSITION);
        int keyLength = BitConverter.readInt(input, KEY_POSITION);
        int valueLength = BitConverter.readInt(input, VALUE_POSITION);
        long entryVersion = BitConverter.readLong(input, ENTRY_VERSION_POSITION);
        validateHeader(keyLength, valueLength);
        return new Header(version, keyLength, valueLength, entryVersion);
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
        long entryVersion = BitConverter.readLong(input);
        validateHeader(keyLength, valueLength);
        return new Header(version, keyLength, valueLength, entryVersion);
    }

    private int writeHeader(byte[] target, int targetOffset, int keyLength, int valueLength, long entryVersion) {
        target[targetOffset] = CURRENT_SERIALIZATION_VERSION;
        targetOffset++;
        targetOffset += BitConverter.writeInt(target, targetOffset, keyLength);
        targetOffset += BitConverter.writeInt(target, targetOffset, valueLength);
        targetOffset += BitConverter.writeLong(target, targetOffset, entryVersion);
        return targetOffset;
    }

    private void validateHeader(int keyLength, int valueLength) throws SerializationException {
        if (keyLength <= 0 || keyLength > MAX_KEY_LENGTH || (valueLength < 0 && valueLength != NO_VALUE) || keyLength + valueLength > MAX_SERIALIZATION_LENGTH) {
            throw new SerializationException(String.format("Read header with invalid data. KeyLength=%s, ValueLength=%s", keyLength, valueLength));
        }
    }

    /**
     * Defines a serialized Entry's Header.
     */
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    static class Header {
        private final byte serializationVersion;
        @Getter
        private final int keyLength;
        @Getter
        private final int valueLength;
        @Getter
        private final long entryVersion;

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
            return String.format("Length: {K=%s, V=%s}, EntryVersion: %s", this.keyLength, this.valueLength, this.entryVersion);
        }
    }

    //endregion
}