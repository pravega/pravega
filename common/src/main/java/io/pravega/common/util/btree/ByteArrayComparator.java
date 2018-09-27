/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.common.util.ByteArraySegment;
import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares two byte arrays of the same length.
 */
final class ByteArrayComparator implements Comparator<byte[]>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(byte[] b1, byte[] b2) {
        assert b1.length == b2.length;
        return compare(b1, 0, b2, 0, b1.length);
    }

    /**
     * Compares two non-null ByteArraySegments of the same length.
     *
     * @param b1 First instance.
     * @param b2 Second instance.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    int compare(ByteArraySegment b1, ByteArraySegment b2) {
        assert b1.getLength() == b2.getLength();
        return compare(b1.array(), b1.arrayOffset(), b2.array(), b2.arrayOffset(), b1.getLength());
    }

    /**
     * Compares two byte arrays from the given offsets.
     *
     * @param b1      First array.
     * @param offset1 The first offset to begin comparing in the first array.
     * @param b2      Second array.
     * @param offset2 The first offset to begin comparing in the second array.
     * @param length  The number of bytes to compare.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    int compare(byte[] b1, int offset1, byte[] b2, int offset2, int length) {
        int r;
        for (int i = 0; i < length; i++) {
            r = Byte.compare(b1[offset1 + i], b2[offset2 + i]);
            if (r != 0) {
                return r;
            }
        }

        return 0;
    }
}