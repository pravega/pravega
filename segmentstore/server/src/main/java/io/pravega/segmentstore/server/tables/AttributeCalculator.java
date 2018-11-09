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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.Attributes;
import java.util.UUID;

/**
 * Provides methods for generating Extended Attributes (Keys and Values) for Tables.
 */
class AttributeCalculator {
    //region Members

    @VisibleForTesting
    static final int PRIMARY_HASH_LENGTH = Long.BYTES * 2;
    @VisibleForTesting
    static final int SECONDARY_HASH_LENGTH = Long.BYTES + Integer.BYTES;
    @VisibleForTesting
    static final int MAX_NODE_ID = (1 << 30) - 1; // First 2 bits are ignored.
    static final UUID MIN_PRIMARY_HASH_KEY = resolveCoreAttributeCollision(new UUID(0L, Long.MIN_VALUE));
    static final UUID MAX_PRIMARY_HASH_KEY = resolveCoreAttributeCollision(new UUID(Long.MAX_VALUE, Long.MAX_VALUE));
    @VisibleForTesting
    static final long PRIMARY_HASH_SET = 0x8000_0000_0000_0000L; // First bit is 1.
    @VisibleForTesting
    static final long ONE_BIT_MASK = 0x7FFF_FFFF_FFFF_FFFFL; // Preserve lower 63 bits (of the 64 MSB of ID).
    private static final long TWO_BIT_MASK = 0x3FFF_FFFF_FFFF_FFFFL; // Preserve lower 62 bits (of the 64 MSB of ID).
    private static final long SECONDARY_HASH_SET = 0x4000_0000_0000_0000L; // First 2 bits are 01.
    private static final long INDEX_NODE_SET = 0x8000_0000_0000_0000L; // First bit is 1.
    private static final long BACKPOINTER_MSB = 0L; // First 64 bits are 0 (including first 2).

    //endregion

    // region Attribute Keys

    /**
     * Generates a 16-byte UUID that encodes the given Primary Hash:
     * Format: 1{PrimaryHash}
     * - MSB is 1 followed by bits 1 through 64 of the given Primary Hash.
     * - LSB is the remaining 64 bits of the given Primary Hash.
     *
     * @param primaryHash 128-bit Primary Hash of the Key to generate an Attribute Key for. The first bit will be ignored.
     * @return A UUID representing the Attribute Key.
     */
    UUID getPrimaryHashAttributeKey(ArrayView primaryHash) {
        Preconditions.checkArgument(primaryHash.getLength() == PRIMARY_HASH_LENGTH, "Given hash has incorrect length.");
        long msb = BitConverter.readLong(primaryHash, 0);
        msb = (msb & ONE_BIT_MASK) | PRIMARY_HASH_SET;
        long lsb = BitConverter.readLong(primaryHash, Long.BYTES);
        return resolveCoreAttributeCollision(new UUID(msb, lsb));
    }

    /**
     * Generates a 16-byte array that is the serialization of the given Primary Hash.
     *
     * @param primaryHashKey A UUID representing the Primary Hash Key.
     * @return An ArrayView.
     */
    ArrayView getPrimaryHash(UUID primaryHashKey) {
        long msb = primaryHashKey.getMostSignificantBits();
        Preconditions.checkArgument((msb & PRIMARY_HASH_SET) == PRIMARY_HASH_SET, "attributeKey is not a Primary Hash.");
        byte[] result = new byte[PRIMARY_HASH_LENGTH];
        BitConverter.writeLong(result, 0, msb);
        BitConverter.writeLong(result, Long.BYTES, primaryHashKey.getLeastSignificantBits());
        return new ByteArraySegment(result);
    }

    /**
     * Generates a Primary Hash Key that is immediately after the given one, when compared using {@link UUID#compareTo}.
     * We define U2 to be immediately after U1 if there doesn't exist U3 such that U1<U3<U2.
     *
     * @param id The Primary Hash key to calculate the successor for.
     * @return The succesor, or null if there is no such thing.
     */
    UUID getNextPrimaryHashAttributeKey(UUID id) {
        long msb = id.getMostSignificantBits();
        Preconditions.checkArgument((msb & PRIMARY_HASH_SET) == PRIMARY_HASH_SET && !Attributes.isCoreAttribute(id),
                "Given Id (%s) is not a Primary Hash.", id);
        long lsb = id.getLeastSignificantBits();
        if (lsb == Long.MAX_VALUE) {
            msb &= ONE_BIT_MASK;
            if (msb == Long.MAX_VALUE) {
                return null;
            }

            msb = (msb + 1) | PRIMARY_HASH_SET;
            lsb = Long.MIN_VALUE;
        } else {
            lsb++;
        }

        return new UUID(msb, lsb);
    }

    /**
     * Generates a 16-byte UUID that encodes the given Secondary Hash and Node Id.
     * Format: 01{NodeId}{SecondaryHash}
     * - MSB is 01 followed by the last 30 bits of NodeId, then the first 32 bits of the given Secondary Hash.
     * - LSB is the remaining 64 bits of the given Secondary Hash.
     *
     * @param secondaryHash 96-bit Secondary Hash of the Key to generate an Attribute Key for.
     * @param nodeId        30-bit Node Id (the first 2 bits of this 32-bit integer will be ignored; using only the last 30 bits).
     * @return A UUID representing the Attribute Key.
     */
    UUID getSecondaryHashAttributeKey(ArrayView secondaryHash, int nodeId) {
        Preconditions.checkArgument(nodeId > 0 && nodeId <= MAX_NODE_ID,
                "nodeId must be a positive integer less than or equal to %s. Given %s", MAX_NODE_ID, nodeId);
        Preconditions.checkArgument(secondaryHash.getLength() == SECONDARY_HASH_LENGTH, "Given hash has incorrect length.");
        long msb = ((long) nodeId << Integer.SIZE) + BitConverter.readInt(secondaryHash, 0);
        msb = (msb & TWO_BIT_MASK) | SECONDARY_HASH_SET;
        long lsb = BitConverter.readLong(secondaryHash, Integer.BYTES);
        return new UUID(msb, lsb);
    }

    /**
     * Generates a 16-byte UUID that encodes the given Offset as a Backpointer (to some other offset).
     * Format {0(64)}{Offset}
     * - MSB is 0
     * - LSB is Offset.
     *
     * @param offset The offset to generate a backpointer from.
     * @return A UUID representing the Attribute Key.
     */
    UUID getBackpointerAttributeKey(long offset) {
        Preconditions.checkArgument(offset >= 0, "offset must be a non-negative number.");
        return new UUID(BACKPOINTER_MSB, offset);
    }

    /**
     * Determines if the given Attribute Key is Backpointer.
     *
     * @param key The Key to test.
     * @return True if backpointer, false otherwise.
     */
    @VisibleForTesting
    boolean isBackpointerAttributeKey(UUID key) {
        return key.getMostSignificantBits() == BACKPOINTER_MSB;
    }

    //endregion

    //region Attribute Values

    /**
     * Encodes the given value as an Index Node Pointer.
     * Format: 1 followed by the last 63 bits of value.
     * This value can be decoded using extractValue().
     *
     * @param value The positive value to encode. The sign bit will be ignored.
     * @return A Long representing the encoded value.
     */
    long getIndexNodeAttributeValue(long value) {
        Preconditions.checkArgument(value >= 0, "value must be a non-negative number.");
        return (value & ONE_BIT_MASK) | INDEX_NODE_SET;
    }

    /**
     * Encodes the given value as a Data Offset Pointer.
     * Format: 0 followed by the last 63 bits of value.
     * This value can be decoded using extractValue().
     *
     * @param value The positive value to encode. The sign bit will be ignored.
     * @return A Long representing the encoded value.
     */
    long getSegmentOffsetAttributeValue(long value) {
        Preconditions.checkArgument(value >= 0, "value must be a non-negative number.");
        return value & ONE_BIT_MASK;
    }

    /**
     * Extracts the value for a Node Attribute Value (eliminates the first bit).
     * This is the reverse of getIndexNodeAttributeValue() and getSegmentOffsetAttributeValue().
     *
     * @param nodeAttributeValue The Node Attribute Value to extract the value from.
     * @return The result.
     */
    long extractValue(long nodeAttributeValue) {
        return nodeAttributeValue & ONE_BIT_MASK;
    }

    /**
     * Determines whether the given value is a Node Pointer or not.
     *
     * @param nodeAttributeValue The value to examine.
     * @return True if Node Pointer, false if Offset Pointer.
     */
    boolean isIndexNodePointer(long nodeAttributeValue) {
        return (nodeAttributeValue & ~ONE_BIT_MASK) == INDEX_NODE_SET;
    }

    //endregion

    //region Helpers

    private static UUID resolveCoreAttributeCollision(UUID id) {
        if (Attributes.isCoreAttribute(id)) {
            // Since we don't want to interfere with Core Attributes, if by any chance we end up with MSB == Long.MIN_VALUE,
            // add 1 to it to resolve the problem.
            id = new UUID(id.getMostSignificantBits() + 1, id.getLeastSignificantBits());
        }

        return id;
    }

    //endregion
}