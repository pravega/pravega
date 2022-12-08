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
import io.pravega.common.util.ByteBufferUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.val;

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

    @VisibleForTesting
    public byte[] numToBytes(int num) {
        return hash.hashInt(num).asBytes();
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
    
    /**
     * Hashes a bufferview in-place. (in contrast to murmur above which needs to copy data into a single array)
     * (This passes the smhasher quality test suite)
     * (See updateHashState below for explanation of the hashing function)
     * @param bufferView The input.
     */
    public static final long hashBufferView(BufferView bufferView) {
        final long multiple = 6364136223846793005L; // From knuth's LCG
        final long inc = 1442695040888963407L; // From knuth' LCG
        final int shift = 47; //(arbitrary odd number between 32 and 56)
        long state = 0xc3a5c85c97cb3127L; // (arbitrary, from farmhash) 
        long weyl = 0x9ae16a3b2f90404fL; // (arbitrary, from farmhash) 
        ByteBuffer leftOvers = ByteBuffer.wrap(new byte[16]);
        val iter = bufferView.iterateBuffers();
        while (iter.hasNext()) {
            ByteBuffer buffer = iter.next();
            if (leftOvers.position() > 0) {
                ByteBufferUtils.copy(buffer, leftOvers);
                if (!leftOvers.hasRemaining()) {
                    leftOvers.flip(); 
                    state = updateHashState(state, weyl, leftOvers);
                    weyl += inc; 
                    leftOvers.clear();
                }
            }
            while (buffer.remaining() >= 16) {
                state = updateHashState(state, weyl, buffer);
                weyl += inc;
            }
            if (buffer.hasRemaining()) {
                ByteBufferUtils.copy(buffer, leftOvers);
            }
        }
        leftOvers.flip();
        while (leftOvers.hasRemaining()) {
            byte b = leftOvers.get();
            state = (state ^ b) * weyl;
            state ^= state >> shift;
            weyl += inc;
        }
        int rot = (int) state & 0b0111111; // (63) RR permutation from PCG
        return Long.rotateRight(state * multiple + inc, rot); // LGC step + RR
    }
    
    /**
     * Provides an updated hash state reading two longs from the provided buffer.
     * This hash uses multiplication as a scrambling function.
     * In general multiplication is a great scrambler, but it has 3 drawbacks that must be compensated for.
     * 1. If the value being multiplied by is fixed, then flipping one bit in the input adds a constant to the output, 
     * which can be 'undone' by flipping that same bit in the other way in a subsequent update.
     * 2. Input bits only affect output bits that are to the left of their position. So high order input bits have very little effect on the output.
     * 3. The upper output bits are much stronger than the lower output bits.
     * Additionally any good update function should:
     * 1. Have non-linearity
     * 2. New input bits should affect the state in multiple ways or at multiple places.
     * 
     * This function has both of these properties and compensates for each of these drawbacks by:
     * 1. Using xor between add/multiplication because it is non-commutative with multiplication and addition.
     * 2. Using a different multiple on each iteration.
     * 3. Using both the forward and reversed input longs so that all input bits affect at least 33 state bits, and in multiple ways.
     * 4. xorshifting the high quality upper bits down into the lower portion.
     * (Note that reverseBytes is very cheap after JIT optimization) 
     */
    private static long updateHashState(long state, long multiple, final ByteBuffer buffer) {
        assert buffer.remaining() >= 16;
        final long offset = 1013904223; // (arbitrary,From Numerical Recipes)
        final int shift = 47; //(arbitrary odd number between 32 and 56)
        long new1 = buffer.getLong();
        long new2 = buffer.getLong();
        state = (state ^ new1) * (multiple ^ Long.reverseBytes(new2)); //Changing rather than fixed multiple removes linearity
        state = (state ^ new2) * ((multiple + offset) ^ Long.reverseBytes(new1)); //Reversing bytes prevents low impact high order bits.
        state ^= state >> shift; // xorshift some good bits to the bottom
        return state;
    } 
}
