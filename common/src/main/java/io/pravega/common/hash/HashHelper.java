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
package io.pravega.common.hash;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.pravega.common.util.BufferView;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

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

    public int hash(byte[] array, int offset, int length) {
        return hash.hashBytes(array, offset, length).asInt();
    }

    public UUID toUUID(String str) {
        assert hash.bits() == 128;
        return bytesToUUID(hash.hashUnencodedChars(str).asBytes());
    }

    /**
     * Converts a 128 bit array into a UUID.
     * Copied from UUID's private constructor.
     */
    @VisibleForTesting
    static UUID bytesToUUID(byte[] data) {
        long msb = 0;
        long lsb = 0;
        assert data.length == 16 : "data must be 16 bytes in length";
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (data[i] & 0xff);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (data[i] & 0xff);
        }
        return new UUID(msb, lsb);
    }

    public int hashToBucket(String str, int numBuckets) {
        return Hashing.consistentHash(hash.hashUnencodedChars(str), numBuckets);
    }

    public int hashToBucket(UUID uuid, int numBuckets) {
        return Hashing.consistentHash(
                Hashing.combineOrdered(Arrays.asList(hash.hashLong(uuid.getMostSignificantBits()), hash.hashLong(uuid.getLeastSignificantBits()))),
                numBuckets);
    }

    public int hashToBucket(byte[] array, int numBuckets) {
        return Hashing.consistentHash(hash.hashBytes(array), numBuckets);
    }

    public int hashToBucket(BufferView bufferView, int numBuckets) {
        HashBuilder builder = newBuilder();
        bufferView.collect(builder::put);
        return Hashing.consistentHash(builder.getHashCode(), numBuckets);
    }

    public HashBuilder newBuilder() {
        return new HashBuilder(hash.newHasher());
    }

    /**
     * Returns a double uniformly randomly distributed between 0 and 1 using the hash function.
     *
     * @param str The input string.
     * @return Uniformly distributed double between 0 and 1.
     */
    public double hashToRange(String str) {
        return longToDoubleFraction(hash.hashUnencodedChars(str).asLong());
    }

    /**
     * Returns a double uniformly randomly distributed between 0 and 1 using the hash function.
     *
     * @param bufs The input {@link ByteBuffer}s to hash.
     * @return Uniformly distributed double between 0 and 1.
     */
    public double hashToRange(ByteBuffer... bufs) {
        Preconditions.checkArgument(bufs.length > 0, "At least one buffer expected.");
        HashCode result;
        if (bufs.length == 1) {
            result = hash.hashBytes(bufs[0]);
        } else {
            Hasher h = hash.newHasher();
            for (ByteBuffer buf : bufs) {
                h.putBytes(buf);
            }
            result = h.hash();
        }

        return longToDoubleFraction(result.asLong());
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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public final class HashBuilder {
        private final Hasher hasher;

        public void put(ByteBuffer bb) {
            this.hasher.putBytes(bb);
        }

        private HashCode getHashCode() {
            return hasher.hash();
        }

        public int getAsInt() {
            return getHashCode().asInt();
        }
    }
}
