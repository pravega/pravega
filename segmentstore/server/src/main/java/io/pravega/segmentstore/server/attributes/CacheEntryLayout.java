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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Helper class that aids in serializing and deserializing Segment Attribute Index Cache Entries using a compact format.
 * Since the serializations coming out of this class need not be persisted or transferred between different processes, no
 * versioning is required.
 *
 * Layout: Count|AttributeData
 * - Count: 4 Bytes representing the number of attributes encoded.
 * - AttributeData: Attributes: ID (16 bytes), Version (8 bytes), Value (8 bytes).
 */
class CacheEntryLayout {
    @VisibleForTesting
    static final int RECORD_LENGTH = 4 * Long.BYTES;
    private static final int VERSION_OFFSET = 2 * Long.BYTES;
    private static final int VALUE_OFFSET = 3 * Long.BYTES;
    private static final int HEADER_LENGTH = Integer.BYTES;

    /**
     * Gets a value representing the number of attributes encoded in the given serialization.
     *
     * @param data The Cache Entry serialization.
     * @return The count. This value is undefined if data was not previously encoded using this CacheEntryLayout.
     */
    static int getCount(byte[] data) {
        int result = BitConverter.readInt(data, 0);
        Preconditions.checkArgument(data.length >= HEADER_LENGTH + result * RECORD_LENGTH, "Invalid or corrupted cache entry.");
        return result;
    }

    /**
     * Gets the Attribute Id located at the given position within the given serialization.
     *
     * @param data     The Cache Entry serialization.
     * @param position The position within the entry for which to get the Attribute Id.
     * @return The Attribute Id. This value is undefined if data was not previously encoded using this CacheEntryLayout.
     */
    static UUID getAttributeId(byte[] data, int position) {
        int offset = HEADER_LENGTH + position * RECORD_LENGTH;
        return new UUID(BitConverter.readLong(data, offset), BitConverter.readLong(data, offset + Long.BYTES));
    }

    /**
     * Gets the Attribute Value located at the given position within the given serialization.
     *
     * @param data     The Cache Entry serialization.
     * @param position The position within the entry for which to get the Attribute Value.
     * @return The Attribute Value. This value is undefined if data was not previously encoded using this CacheEntryLayout.
     */
    static long getValue(byte[] data, int position) {
        return BitConverter.readLong(data, HEADER_LENGTH + position * RECORD_LENGTH + VALUE_OFFSET);
    }

    /**
     * Gets all the Attribute Ids and their Values encoded in the given serialization.
     *
     * @param data The Cache Entry serialization.
     * @return A modifiable Map (UUID, UUID) representing the result. The Keys are Attribute Ids, while the Values are UUIDs
     * whose MSBs are the Version for that Value, and LSBs are the Attribute Values themselves. This result is undefined if
     * data was not previously encoded using this CacheEntryLayout.
     */
    static Map<UUID, UUID> getAllValues(byte[] data) {
        if (data == null) {
            return new HashMap<>();
        }

        int count = getCount(data);
        Map<UUID, UUID> result = new HashMap<>();
        int offset = HEADER_LENGTH;
        for (int i = 0; i < count; i++) {
            result.put(new UUID(BitConverter.readLong(data, offset), BitConverter.readLong(data, offset + Long.BYTES)),
                    new UUID(BitConverter.readLong(data, offset + VERSION_OFFSET), BitConverter.readLong(data, offset + VALUE_OFFSET)));
            offset += RECORD_LENGTH;
        }

        return result;
    }

    /**
     * Serializes the given Attribute Values into the given array, or creates a new one if they cannot fit.
     *
     * @param buffer     The buffer to attempt to reuse.
     * @param values     An Iterator that returns (UUID, UUID) pairs. Similarly to getAllValues(), the Keys are Attribute Ids,
     *                   while the Values are UUIDs whose MSBs are the Version for that Value, and LSBs are the Attribute Values
     *                   themselves.
     * @param valueCount The number of items that will be returned by the values iterator.
     * @return Either buffer, if the new serialization can fit in it, or a new byte array of a larger size which contains
     * the serialization. If a new byte array is returned, the incoming buffer is not touched.
     */
    static byte[] setValues(byte[] buffer, Iterator<Map.Entry<UUID, UUID>> values, int valueCount) {
        int size = HEADER_LENGTH + RECORD_LENGTH * valueCount;
        if (buffer == null || buffer.length < size) {
            buffer = new byte[size];
        }

        BitConverter.writeInt(buffer, 0, valueCount);
        int offset = HEADER_LENGTH;
        while (values.hasNext()) {
            Map.Entry<UUID, UUID> e = values.next();
            offset += BitConverter.writeLong(buffer, offset, e.getKey().getMostSignificantBits());
            offset += BitConverter.writeLong(buffer, offset, e.getKey().getLeastSignificantBits());
            offset += BitConverter.writeLong(buffer, offset, e.getValue().getMostSignificantBits());
            offset += BitConverter.writeLong(buffer, offset, e.getValue().getLeastSignificantBits());
        }

        return buffer;
    }
}
