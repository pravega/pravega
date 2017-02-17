/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class HashHelper {

    private static final long LEADING_BITS = 0x3ff0000000000000L;
    private static final long MASK = 0x000fffffffffffffL;
    private HashFunction hash;

    private HashHelper(int seed) {
        hash = Hashing.murmur3_128(seed);
    }

    public static HashHelper seededWith(String seed) {
        return new HashHelper(seed.hashCode());
    }

    public int hash(long longNumber) {
        return hash.hashLong(longNumber).asInt();
    }

    public long hash(String str) {
        return hash.hashUnencodedChars(str).asLong();
    }

    public int hashToBucket(String str, int numBuckets) {
        return Hashing.consistentHash(hash.hashUnencodedChars(str), numBuckets);
    }

    /**
     * Returns a double uniformly randomly distributed between 0 and 1 using the hash function.
     *
     * @param str The input string.
     */
    public double hashToRange(String str) {
        return longToDoubleFraction(hash.hashUnencodedChars(str).asLong());
    }

    /**
     * Turns the leading 54 bits of a long into a double between 0 and 1.
     *
     * @param value The input.
     */
    @VisibleForTesting
    static double longToDoubleFraction(long value) {
        long shifted = (value >> 12) & MASK;
        return Double.longBitsToDouble(LEADING_BITS + shifted) - 1;
    }
}
