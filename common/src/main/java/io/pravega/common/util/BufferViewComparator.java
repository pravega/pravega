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

import com.google.common.base.Preconditions;
import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.util.Arrays;
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
public final class BufferViewComparator implements Comparator<BufferView>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    @VisibleForTesting
    public int compare(BufferView bufferView, BufferView t1) {
        Preconditions.checkNotNull(bufferView, "bufferView can not be null");
        Preconditions.checkNotNull(t1, "t1 can not be null");

        if ((bufferView instanceof CompositeBufferView) && (t1 instanceof CompositeBufferView)) {
            int l = bufferView.getLength();
            if (l != t1.getLength()) {
                return -1;
            }

            if (l > 0) {
                byte[] thisBytes = bufferView.getCopy();
                byte[] thatBytes = t1.getCopy();
                return Arrays.compare(thisBytes, 0, l, thatBytes, 0, l);
            }
            return -1;
        }

        //        if ((bufferView instanceof CompositeByteArraySegment) && (t1 instanceof CompositeByteArraySegment)) {
        //            int l = bufferView.getLength();
        //            if (l != t1.getLength()) {
        //                return -1;
        //            }
        //
        //            if (l > 0) {
        //                byte[] thisBytes = bufferView.getCopy();
        //                byte[] thatBytes = t1.getCopy();
        //                return Arrays.compare(thisBytes, 0, l, thatBytes, 0, l);
        //            }
        //            return -1;
        //        }
        //
        //        if ((bufferView instanceof ByteArraySegment) && (t1 instanceof ByteArraySegment)) {
        //            int l = bufferView.getLength();
        //            if (l != t1.getLength()) {
        //                return -1;
        //            }
        //
        //            if (l > 0) {
        //                byte[] thisBytes = bufferView.getCopy();
        //                byte[] thatBytes = t1.getCopy();
        //                return Arrays.compare(thisBytes, 0, l, thatBytes, 0, l);
        //            }
        //            return -1;
        //        }
        return -1;
    }
}

