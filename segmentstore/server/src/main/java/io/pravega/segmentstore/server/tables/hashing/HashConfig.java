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

public class HashConfig {
    private final int[] endByteOffsets;

    private HashConfig(int[] endByteOffsets) {
        this.endByteOffsets = endByteOffsets;
    }

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

    public int getCount() {
        return this.endByteOffsets.length;
    }

    int getMinHashLengthBytes() {
        return this.endByteOffsets[this.endByteOffsets.length - 1];
    }

    Pair<Integer, Integer> getOffsets(int index) {
        Preconditions.checkArgument(index >= 0, "index must be non-negative.");
        if (index >= this.endByteOffsets.length) {
            return null;
        }

        int start = index == 0 ? 0 : this.endByteOffsets[index - 1];
        return Pair.of(start, this.endByteOffsets[index]);
    }
}
