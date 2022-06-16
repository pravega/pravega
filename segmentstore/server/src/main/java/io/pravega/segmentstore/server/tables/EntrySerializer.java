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
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Function;
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
@VisibleForTesting
public class EntrySerializer {
    @VisibleForTesting
    public static final int HEADER_LENGTH = 1 + Integer.BYTES * 2 + Long.BYTES; // Serialization Version, Key Length, Value Length, Entry Version.
    static final int MAX_KEY_LENGTH = TableStore.MAXIMUM_KEY_LENGTH;
    /**
     * Maximum size allowed for any single Table Segment Update (update or removal).
     * NOTE: If changing this, consider other dependent values that are calculated based on it. Use your IDE to find them.
     */
    static final int MAX_SERIALIZATION_LENGTH = TableStore.MAXIMUM_KEY_LENGTH + TableStore.MAXIMUM_VALUE_LENGTH;
    /**
     * Maximum size allowed for any Table Segment update (single or multiple entry). No update (or removal) serialization
     * may exceed this.
     * NOTE: If changing this, consider other dependent values that are calculated based on it. Use your IDE to find them.
     */
    static final int MAX_BATCH_SIZE = 32 * MAX_SERIALIZATION_LENGTH;
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
     * Serializes the given {@link TableEntry} collection into a {@link BufferView}, without explicitly recording the
     * versions for each entry.
     *
     * @param entries A Collection of {@link TableEntry} to serialize.
     * @return A {@link BufferView} representing the serialization of the given entries.
     */
    @VisibleForTesting
    public BufferView serializeUpdate(@NonNull Collection<TableEntry> entries) {
        return serializeUpdate(entries, e -> TableKey.NO_VERSION);
    }

    private BufferView serializeUpdate(@NonNull Collection<TableEntry> entries, Function<TableKey, Long> getVersion) {
        val builder = BufferView.builder(entries.size() * 3);
        entries.forEach(e -> serializeUpdate(e, getVersion, builder::add));
        Preconditions.checkArgument(builder.getLength() <= MAX_BATCH_SIZE, "Update batch size cannot exceed %s. Given %s.", MAX_BATCH_SIZE, builder.getLength());
        return builder.build();
    }

    private void serializeUpdate(@NonNull TableEntry entry, Function<TableKey, Long> getVersion, Consumer<BufferView> acceptBuffer) {
        val key = entry.getKey().getKey();
        val value = entry.getValue();
        Preconditions.checkArgument(key.getLength() <= MAX_KEY_LENGTH, "Key too large.");
        int serializationLength = getUpdateLength(entry);
        Preconditions.checkArgument(serializationLength <= MAX_SERIALIZATION_LENGTH, "Key+Value serialization too large.");

        // Serialize Header.
        acceptBuffer.accept(serializeHeader(key.getLength(), value.getLength(), getVersion.apply(entry.getKey())));
        acceptBuffer.accept(key);
        acceptBuffer.accept(value);
    }

    /**
     * Serializes the given {@link TableEntry} collection into a {@link BufferView}, explicitly recording the versions
     * for each entry ({@link TableKey#getVersion()}). This should be used for {@link TableEntry} instances that were
     * previously read from the Table Segment as only in that case does the version accurately reflect the entry's version.
     *
     * @param entries A Collection of {@link TableEntry} to serialize.
     * @return A {@link BufferView} representing the serialization of the given entries.
     */
    BufferView serializeUpdateWithExplicitVersion(@NonNull Collection<TableEntry> entries) {
        return serializeUpdate(entries, TableKey::getVersion);
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
     * Serializes the given {@link TableKey} collection for removal into a {@link BufferView}.
     *
     * @param keys A Collection of {@link TableKey} to serialize for removals.
     * @return A {@link BufferView} representing the serialization of the given keys.
     */
    @VisibleForTesting
    public BufferView serializeRemoval(@NonNull Collection<TableKey> keys) {
        val builder = BufferView.builder(keys.size() * 2);
        keys.forEach(k -> serializeRemoval(k, builder::add));
        return builder.build();
    }

    private void serializeRemoval(@NonNull TableKey tableKey, Consumer<BufferView> acceptBuffer) {
        val key = tableKey.getKey();
        Preconditions.checkArgument(key.getLength() <= MAX_KEY_LENGTH, "Key too large.");

        // Serialize Header. Not caring about explicit versions since we do not reinsert removals upon compaction.
        acceptBuffer.accept(serializeHeader(key.getLength(), NO_VALUE, TableKey.NO_VERSION));
        acceptBuffer.accept(key);
    }

    //endregion

    //region Headers

    /**
     * Reads the Entry's Header from the given {@link BufferView.Reader}.
     *
     * @param input The {@link BufferView.Reader} to read from.
     * @return The Entry Header.
     * @throws SerializationException If an invalid header was detected.
     */
    @VisibleForTesting
    public Header readHeader(@NonNull BufferView.Reader input) throws SerializationException {
        byte version = input.readByte();
        int keyLength = input.readInt();
        int valueLength = input.readInt();
        long entryVersion = input.readLong();
        validateHeader(keyLength, valueLength);
        return new Header(version, keyLength, valueLength, entryVersion);
    }

    private BufferView serializeHeader(int keyLength, int valueLength, long entryVersion) {
        ByteArraySegment data = new ByteArraySegment(new byte[HEADER_LENGTH]);
        data.set(VERSION_POSITION, CURRENT_SERIALIZATION_VERSION);
        data.setInt(KEY_POSITION, keyLength);
        data.setInt(VALUE_POSITION, valueLength);
        data.setLong(ENTRY_VERSION_POSITION, entryVersion);
        return data;
    }

    private void validateHeader(int keyLength, int valueLength) throws SerializationException {
        if (keyLength <= 0 || keyLength > MAX_KEY_LENGTH || (valueLength < 0 && valueLength != NO_VALUE) || keyLength + valueLength > MAX_SERIALIZATION_LENGTH) {
            throw new SerializationException(String.format("Read header with invalid data. KeyLength=%s, ValueLength=%s", keyLength, valueLength));
        }
    }

    /**
     * Defines a serialized Entry's Header.
     */
    @VisibleForTesting
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Header {
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