/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.IndexedMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

/**
 * Helper class that aids in serializing and deserializing Segment Attribute Index Cache Entries using a compact format.
 * Since the serializations coming out of this class need not be persisted or transferred between different processes, no
 * versioning is required.
 *
 * Layout: Count|AttributeData
 * - Count: 4 Bytes representing the number of attributes encoded.
 * - AttributeData: Attributes: ID (16 bytes), Version (8 bytes), Value (8 bytes).
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class CacheEntryLayout implements IndexedMap<UUID, Long> {
    //region Members

    @VisibleForTesting
    static final int RECORD_LENGTH = 4 * Long.BYTES;
    private static final int VERSION_OFFSET = 2 * Long.BYTES;
    private static final int VALUE_OFFSET = 3 * Long.BYTES;
    private static final int HEADER_LENGTH = Integer.BYTES;
    private final byte[] data;

    //endregion

    //region IndexedMap Implementation

    @Override
    public int getCount() {
        int result = BitConverter.readInt(this.data, 0);
        Preconditions.checkArgument(this.data.length >= HEADER_LENGTH + result * RECORD_LENGTH, "Invalid or corrupted cache entry.");
        return result;
    }

    @Override
    public UUID getKey(int position) {
        int offset = HEADER_LENGTH + position * RECORD_LENGTH;
        return new UUID(BitConverter.readLong(data, offset), BitConverter.readLong(data, offset + Long.BYTES));
    }

    @Override
    public Long getValue(int position) {
        return BitConverter.readLong(this.data, HEADER_LENGTH + position * RECORD_LENGTH + VALUE_OFFSET);
    }

    /**
     * Gets all the Attribute Ids and their Values.
     *
     * @return A modifiable Map (UUID, VersionedValue) representing the result.
     */
    Map<UUID, VersionedValue> getAllValues() {
        int count = getCount();
        Map<UUID, VersionedValue> result = new HashMap<>();
        int offset = HEADER_LENGTH;
        for (int i = 0; i < count; i++) {
            result.put(new UUID(BitConverter.readLong(this.data, offset), BitConverter.readLong(data, offset + Long.BYTES)),
                    new VersionedValue(BitConverter.readLong(this.data, offset + VERSION_OFFSET), BitConverter.readLong(this.data, offset + VALUE_OFFSET)));
            offset += RECORD_LENGTH;
        }

        return result;
    }

    //endregion

    //region Static Methods

    /**
     * Creates a new CacheEntryLayout for the given serialization.
     *
     * @param data The serialization to wrap.
     * @return A new CacheEntryLayout instance. The outcome of any operation invoked on the methods on this instance is
     * undefined if the contents of "data" has not been previously serialized using setValues().
     */
    static CacheEntryLayout wrap(byte[] data) {
        return new CacheEntryLayout(Preconditions.checkNotNull(data, "data"));
    }

    /**
     * Serializes the given Attribute Values into the given array, or creates a new one if they cannot fit.
     *
     * @param buffer     The buffer to attempt to reuse.
     * @param values     An Iterator that returns (UUID, VersionedValue) pairs.
     * @param valueCount The number of items that will be returned by the values iterator.
     * @return Either buffer, if the new serialization can fit in it, or a new byte array of a larger size which contains
     * the serialization. If a new byte array is returned, the incoming buffer is not touched.
     */
    static byte[] setValues(byte[] buffer, Iterator<Map.Entry<UUID, VersionedValue>> values, int valueCount) {
        int size = HEADER_LENGTH + RECORD_LENGTH * valueCount;
        if (buffer == null || buffer.length < size) {
            buffer = new byte[size];
        }

        BitConverter.writeInt(buffer, 0, valueCount);
        int offset = HEADER_LENGTH;
        while (values.hasNext()) {
            Map.Entry<UUID, VersionedValue> e = values.next();
            offset += BitConverter.writeLong(buffer, offset, e.getKey().getMostSignificantBits());
            offset += BitConverter.writeLong(buffer, offset, e.getKey().getLeastSignificantBits());
            offset += BitConverter.writeLong(buffer, offset, e.getValue().version);
            offset += BitConverter.writeLong(buffer, offset, e.getValue().value);
        }

        return buffer;
    }

    //endregion
}
