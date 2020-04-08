/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Performs a lexicographic bitwise comparison of two byte arrays.
 *
 * Lexicographic bitwise comparison for arrays of the same length:
 * - Consider two arrays A and B, with each having L bits (L is a multiple of 8).
 * - Define A{n} and B{n} as the bit at position n in A and B, respectively. This can be either 0 or 1.
 * - A precedes B if there exists bit position i such that for all bit positions j smaller than i,
 * then A{j} is equal to B{j}, A{i} is 0 and B{i} is 1.
 * - A is equal to B if the values of all bit positions in both arrays match.
 *
 * Lexicographic bitwise comparison for arrays of different lengths:
 * - Consider two arrays A and B, with A having LA bits and B having LB bits (LA, LB are multiples of 8).
 * - We do a Lexicographic bitwise comparison of the prefixes of A and B of lengths Min(LA, LB).
 * - If the prefixes are equal, then the shorter of A and B precedes the longer of A and B.
 * -- If LA &lt; LB, then A is before B; if LA &gt; LB, then A is after B.
 * - If the prefixes are not equal, then the result from the prefix comparison is used to order A and B (see above).
 *
 * Lexicographic bitwise comparison matches the natural order of numbers when serialized as unsigned (i.e., using the
 * specialized methods in {@link BitConverter}) since they avoid the complications involved with interpreting individual
 * bytes with the first bit set to 1 using 2's complement (128 is before 127 if we used signed bytes).
 *
 * For example:
 * - Consider any two Longs L1 and L2.
 * - Let S1 be the result of {@link BitConverter#writeUnsignedLong} when applied to L1, and S2 the result when applied to L2.
 * - Then {@link Long#compare} applied to (L1, L2) is equal to {@link ByteArrayComparator#compare} applied to (S1, S2).
 * - This equality would not hold should L1 and L2 be serialized using {@link BitConverter#writeLong} or if we used plain
 * (signed) byte comparison internally.
 */
public final class ByteArrayComparator implements Comparator<byte[]>, Serializable {
    /**
     * The minimum byte value for this comparison. Since we use unsigned bytes, this is 0-based.
     */
    public static final byte MIN_VALUE = 0;
    /**
     * The maximum byte value for this comparison. Since we use unsigned bytes, this is 0-based, hence 255.
     * Note that the actual value stored in Java for this is 0xFF, which is actually -1.
     */
    public static final byte MAX_VALUE = (byte) 255;
    private static final long serialVersionUID = 1L;

    @Override
    public int compare(byte[] b1, byte[] b2) {
        assert b1.length == b2.length;
        return compare(b1, 0, b2, 0, b1.length);
    }

    /**
     * Compares two non-null {@link ArrayView} using lexicographic bitwise comparison.
     *
     * @param b1 First instance.
     * @param b2 Second instance.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    public int compare(ArrayView b1, ArrayView b2) {
        if (b1.getLength() == b2.getLength()) {
            return compare(b1.array(), b1.arrayOffset(), b2.array(), b2.arrayOffset(), b1.getLength());
        } else {
            int len = Math.min(b1.getLength(), b2.getLength());
            int c = compare(b1.array(), b1.arrayOffset(), b2.array(), b2.arrayOffset(), len);
            if (c == 0) {
                // If b2 is longer than b1, then b1 is a prefix of b2 so b1 should be before b2.
                // If b2 is shorter than b1, then b2 is a prefix of b1 so b2 should be before b1.
                c = b2.getLength() > b1.getLength() ? -1 : 1;
            }
            return c;
        }
    }

    /**
     * Compares two byte arrays from the given offsets using lexicographic bitwise comparison.
     *
     * @param b1      First array.
     * @param offset1 The first offset to begin comparing in the first array.
     * @param b2      Second array.
     * @param offset2 The first offset to begin comparing in the second array.
     * @param length  The number of bytes to compare.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    public int compare(byte[] b1, int offset1, byte[] b2, int offset2, int length) {
        int r;
        for (int i = 0; i < length; i++) {
            // Unsigned comparison mimics bitwise comparison.
            r = (b1[offset1 + i] & 0xFF) - (b2[offset2 + i] & 0xFF);
            if (r != 0) {
                return r;
            }
        }

        return 0;
    }

    /**
     * Gets the minimum non-empty value. When compared against this, all other byte arrays will be larger.
     *
     * @return The minimum, non-empty value.
     */
    public static byte[] getMinValue() {
        return new byte[]{MIN_VALUE};
    }
}
