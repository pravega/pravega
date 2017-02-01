/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    /**
     * Converts seed into hashcode seed.
     * @param seed A raw seed
     * @return HashHelper instance with hashcoded seed
     */
    public static HashHelper seededWith(String seed) {
        return new HashHelper(seed.hashCode());
    }

    /**
     * Hashes the given long value into int.
     * @param longNumber A value to be hashed
     * @return integer hash value
     */
    public int hash(long longNumber) {
        return hash.hashLong(longNumber).asInt();
    }

    /**
     * Hashes the given string value into int.
     * @param str A string value to be hashed
     * @return integer hash value
     */
    public long hash(String str) {
        return hash.hashUnencodedChars(str).asLong();
    }

    /**
     * Hashes the given string into int.
     * @param str A string value to be hashed.
     * @param numBuckets Number of buckets that hashing operates in.
     * @return integer hash value
     */
    public int hashToBucket(String str, int numBuckets) {
        return Hashing.consistentHash(hash.hashUnencodedChars(str), numBuckets);
    }

    /**
     * Returns a double uniformly randomly distributed between 0 and 1 using the hash function.
     *
     * @param str The input string.
     * @return a double indicating a range of the hash btw 0 and 1.
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
