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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Performs a lexicographic bitwise comparison of {@link BufferView} instances (and of implemented classes).
 * <p>
 * Lexicographic bitwise comparison for {@link BufferView}s of the same length:
 * - Consider two {@link BufferView}s A and B, with each having L bits (L is a multiple of 8).
 * - Define A{n} and B{n} as the bit at position n in A and B, respectively. This can be either 0 or 1.
 * - A precedes B if there exists bit position i such that for all bit positions j smaller than i,
 * then A{j} is equal to B{j}, A{i} is 0 and B{i} is 1.
 * - A is equal to B if the values of all bit positions in both {@link BufferView}s match.
 * <p>
 * Lexicographic bitwise comparison for {@link BufferView}s of different lengths:
 * - Consider two {@link BufferView}s A and B, with A having LA bits and B having LB bits (LA, LB are multiples of 8).
 * - We do a Lexicographic bitwise comparison of the prefixes of A and B of lengths Min(LA, LB).
 * - If the prefixes are equal, then the shorter of A and B precedes the longer of A and B.
 * -- If LA &lt; LB, then A is before B; if LA &gt; LB, then A is after B.
 * - If the prefixes are not equal, then the result from the prefix comparison is used to order A and B (see above).
 * <p>
 * Lexicographic bitwise comparison matches the natural order of numbers when serialized as unsigned (i.e., using the
 * specialized methods in {@link BitConverter}) since they avoid the complications involved with interpreting individual
 * bytes with the first bit set to 1 using 2's complement (128 is before 127 if we used signed bytes).
 * <p>
 * For example:
 * - Consider any two Longs L1 and L2.
 * - Let S1 be the result of {@link StructuredWritableBuffer#setUnsignedLong} when applied to L1, and S2 the result when applied to L2.
 * - Then {@link Long#compare} applied to (L1, L2) is equal to {@link #compare} applied to (S1, S2).
 * - This equality would not hold should L1 and L2 be serialized using {@link StructuredWritableBuffer#setLong(int, long)}
 * or if we used plain (signed) byte comparison internally.
 */
public abstract class BufferViewComparator implements Comparator<byte[]>, Serializable {
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

    //region Constructor

    /**
     * Determines if Intrinsic (JRE9+) array and {@link ByteBuffer} comparison is available.
     */
    private static final boolean INTRINSIC_SUPPORTED;

    static {
        // Figure out if we support JRE9+ Intrinsic comparators. We do compile this code with JDK11, however this is to
        // have a failover in case we run with a lesser JRE.
        boolean intrinsicSupported = true;
        try {
            ByteBuffer bb = ByteBuffer.wrap(new byte[1]);
            bb.mismatch(bb);
        } catch (ExceptionInInitializerError | UnsatisfiedLinkError | NoClassDefFoundError e) {
            intrinsicSupported = false;
        }
        INTRINSIC_SUPPORTED = intrinsicSupported;
    }

    private BufferViewComparator() {
        // Intentionally left blank.
    }

    /**
     * Creates a new {@link BufferViewComparator} instance.
     *
     * @return A new {@link BufferViewComparator} instance.
     */
    public static BufferViewComparator create() {
        return INTRINSIC_SUPPORTED ? new IntrinsicComparator() : new LegacyComparator();
    }

    //endregion

    //region Abstract Methods

    @Override
    public abstract int compare(byte[] b1, byte[] b2);

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
    public abstract int compare(byte[] b1, int offset1, byte[] b2, int offset2, int length);

    /**
     * Compares two non-null {@link ArrayView}s using lexicographic bitwise comparison.
     *
     * @param b1 First instance.
     * @param b2 Second instance.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    public abstract int compare(ArrayView b1, ArrayView b2);

    /**
     * Compares two non-null {@link BufferView} using lexicographic bitwise comparison.
     *
     * @param b1 First instance.
     * @param b2 Second instance.
     * @return -1 if b1 should be before b2, 0 if b1 equals b2 and 1 if b1 should be after b2.
     */
    public abstract int compare(BufferView b1, BufferView b2);

    //endregion

    //region Other methods

    /**
     * Gets the minimum non-empty value. When compared against this, all other byte arrays will be larger.
     *
     * @return The minimum, non-empty value.
     */
    public static byte[] getMinValue() {
        return new byte[]{MIN_VALUE};
    }

    /**
     * Gets the minimum value for an array with given length. When compared against this, all other byte arrays with lengths
     * equal to or greater than {@code length} will be larger.
     *
     * @param length The length.
     * @return The minimum value.
     */
    public static byte[] getMinValue(int length) {
        byte[] r = new byte[length];
        Arrays.fill(r, 0, r.length, MIN_VALUE);
        return r;
    }

    /**
     * Gets the maximum value for an array with given length. When compared against this, all other byte arrays with lengths
     * equal to or smaller than {@code length} will be smaller.
     *
     * @param length The length.
     * @return The maximum value.
     */
    public static byte[] getMaxValue(int length) {
        byte[] r = new byte[length];
        Arrays.fill(r, 0, r.length, MAX_VALUE);
        return r;
    }

    /**
     * Gets an {@link ArrayView} of the same length as the given one that immediately succeeds it when compared with
     * {@link BufferViewComparator}.
     *
     * @param array The input array.
     * @return The result, or null if the input array has max value (there is no array of the same length that can be
     * greater than it when compared using {@link BufferViewComparator}).
     */
    public static ArrayView getNextItemOfSameLength(ArrayView array) {
        final int maxValue = MAX_VALUE & 0xFF;
        final byte[] result = array.getCopy();
        int index = result.length - 1;
        while (index >= 0) {
            int v = result[index] & 0xFF;
            if (v >= maxValue) {
                result[index] = BufferViewComparator.MIN_VALUE;
            } else {
                result[index] = (byte) (v + 1);
                return new ByteArraySegment(result); // Found one.
            }

            index--;
        }

        // This is the highest item of its length.
        return null;
    }

    //endregion

    //region LegacyComparator

    @VisibleForTesting
    static class LegacyComparator extends BufferViewComparator {
        @Override
        public int compare(byte[] b1, byte[] b2) {
            assert b1.length == b2.length;
            return compare(b1, 0, b2, 0, b1.length);
        }

        @Override
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

        @Override
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

        @Override
        public int compare(BufferView b1, BufferView b2) {
            if (b1 instanceof ArrayView && b2 instanceof ArrayView) {
                return compare((ArrayView) b1, (ArrayView) b2);
            }

            BufferView.Reader r1 = b1.getBufferViewReader();
            BufferView.Reader r2 = b2.getBufferViewReader();
            int r;
            while (r1.available() > 0 && r2.available() > 0) {
                r = (r1.readByte() & 0xFF) - (r2.readByte() & 0xFF);
                if (r != 0) {
                    return r;
                }
            }

            if (r1.available() == r2.available()) {
                return 0;
            } else {
                return r2.available() > 0 ? -1 : 1;
            }
        }
    }

    //endregion

    //region IntrinsicComparator

    @VisibleForTesting
    static class IntrinsicComparator extends BufferViewComparator {
        @Override
        public int compare(byte[] b1, byte[] b2) {
            assert b1.length == b2.length;
            return compare(b1, 0, b2, 0, b1.length);
        }

        @Override
        public int compare(byte[] b1, int offset1, byte[] b2, int offset2, int length) {
            return Arrays.compareUnsigned(b1, offset1, offset1 + length, b2, offset2, offset2 + length);
        }

        @Override
        public int compare(ArrayView b1, ArrayView b2) {
            return Arrays.compareUnsigned(b1.array(), b1.arrayOffset(), b1.arrayOffset() + b1.getLength(),
                    b2.array(), b2.arrayOffset(), b2.arrayOffset() + b2.getLength());
        }

        @Override
        public int compare(BufferView bufferView1, BufferView bufferView2) {
            if (bufferView1 instanceof ArrayView && bufferView2 instanceof ArrayView) {
                return compare((ArrayView) bufferView1, (ArrayView) bufferView2);
            }

            if (bufferView1.getLength() == 0) {
                return bufferView2.getLength() > 0 ? -1 : 0;
            } else if (bufferView2.getLength() == 0) {
                return 1;
            } else {
                return compare(bufferView1.iterateBuffers(), bufferView2.iterateBuffers());
            }
        }

        private int compare(Iterator<ByteBuffer> i1, Iterator<ByteBuffer> i2) {
            ByteBuffer b1 = i1.next();
            ByteBuffer b2 = i2.next();
            while (b1 != null && b2 != null) {
                int mismatchIndex = b1.mismatch(b2);
                if (mismatchIndex >= 0 && mismatchIndex < Math.min(b1.remaining(), b2.remaining())) {
                    // We found a difference.
                    return Byte.compareUnsigned(b1.get(b1.position() + mismatchIndex), b2.get(b2.position() + mismatchIndex));
                } else {
                    // No difference.
                    int lengthDiff = b1.remaining() - b2.remaining();
                    if (lengthDiff == 0) {
                        b1 = null;
                        b2 = null;
                    } else if (lengthDiff < 0) {
                        b1 = null;
                        b2.position(b2.limit() + lengthDiff);
                    } else {
                        b1.position(b1.limit() - lengthDiff);
                        b2 = null;
                    }
                }

                if (b1 == null && i1.hasNext()) {
                    b1 = i1.next();
                }

                if (b2 == null && i2.hasNext()) {
                    b2 = i2.next();
                }
            }

            if (b1 == null) {
                // Nothing left in first BufferView.
                return b2 == null ? 0 : -1;
            } else {
                // First BufferView still has something.
                return b2 == null ? 1 : 0;
            }
        }
    }


    //endregion
}
