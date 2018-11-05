/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables.hashing;

import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Key Hashing Configuration.
 */
public class HashConfig {
    private static final byte IGNORE_LEADING_BIT_MASK = (byte) 0x7F; // When applied to a hash, clears the Most Significant Bit.
    private final int[] endByteOffsets;

    private HashConfig(int[] endByteOffsets) {
        this.endByteOffsets = endByteOffsets;
    }

    /**
     * Creates a new instance of the {@link HashConfig} class.
     *
     * @param byteLengths An array representing the lengths (in bytes) for each of the Hash components.
     * @return A new {@link HashConfig} instance.
     */
    public static HashConfig of(@NonNull int... byteLengths) {
        Preconditions.checkArgument(byteLengths.length > 0, "byteLengths must not be empty.");
        int[] offsets = new int[byteLengths.length];
        int endOffset = 0;
        for (int i = 0; i < byteLengths.length; i++) {
            Preconditions.checkArgument(byteLengths[i] > 0, "length must be a positive integer for index %s.", i);
            endOffset += byteLengths[i];
            offsets[i] = endOffset;
        }

        return new HashConfig(offsets);
    }

    /**
     * Gets a value representing the number of Hash components.
     */
    public int getHashCount() {
        return this.endByteOffsets.length;
    }

    /**
     * Gets a value representing the minimum number of bytes that a Hash function must generate in order to use this config.
     */
    public int getMinHashLengthBytes() {
        return this.endByteOffsets[this.endByteOffsets.length - 1];
    }

    /**
     * Gets a {@link Pair} of Offsets representing the Start and End offsets for the Hash Component with given index.
     * @param index The Hash Component index.
     * @return The {@link Pair} of offsets.
     */
    Pair<Integer, Integer> getOffsets(int index) {
        Preconditions.checkArgument(index >= 0, "index must be non-negative.");
        if (index >= this.endByteOffsets.length) {
            return null;
        }

        int start = index == 0 ? 0 : this.endByteOffsets[index - 1];
        return Pair.of(start, this.endByteOffsets[index]);
    }

    /**
     * Applies a mask to the given hash in order to conform it to the rules defined in this {@link HashConfig} instance.
     * In particular, it clears the first bit of the hash, as that is going to be ignored anyway when constructing the
     * Index and is best if the Hash doesn't contain it in the first place.
     *
     * @param hash A byte array representing the hash to modify.
     */
    void applyHashMask(byte[] hash) {
        Preconditions.checkArgument(hash.length >= getMinHashLengthBytes(), "Invalid hash length.");
        hash[0] &= IGNORE_LEADING_BIT_MASK;
    }
}
