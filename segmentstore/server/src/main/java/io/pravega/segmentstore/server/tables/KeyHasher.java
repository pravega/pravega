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
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import java.util.UUID;
import java.util.function.Function;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Defines a Hasher for a Table Key.
 */
abstract class KeyHasher {
    /**
     * Size of the Hash, in bytes.
     */
    static final int HASH_SIZE_BYTES = Long.BYTES + Long.BYTES; // UUID length.

    /**
     * Minimum value for any Key Hash, when compared using {@link UUID#compareTo}.
     */
    static final UUID MIN_HASH = new UUID(TableBucket.CORE_ATTRIBUTE_PREFIX + 1, Long.MIN_VALUE);

    /**
     * Maximum value for any Key Hash, when compared using {@link UUID#compareTo}.
     */
    static final UUID MAX_HASH = new UUID(TableBucket.BACKPOINTER_PREFIX - 1, Long.MAX_VALUE);

    /**
     * Generates a new Key Hash for the given Key.
     *
     * @param key The Key to hash.
     * @return A UUID representing the Hash for the given Key.
     */
    public UUID hash(@NonNull byte[] key) {
        return hash(new ByteArraySegment(key));
    }

    /**
     * Generates a new Key Hash for the given Key.
     *
     * @param key The Key to hash.
     * @return A UUID representing the Hash for the given Key.
     */
    public abstract UUID hash(@NonNull BufferView key);

    protected UUID toUUID(byte[] rawHash) {
        assert rawHash.length == HASH_SIZE_BYTES;
        long msb = BitConverter.readLong(rawHash, 0);
        long lsb = BitConverter.readLong(rawHash, Long.BYTES);
        if (msb == TableBucket.CORE_ATTRIBUTE_PREFIX) {
            msb++;
        } else if (msb == TableBucket.BACKPOINTER_PREFIX) {
            msb--;
        }

        return new UUID(msb, lsb);
    }

    /**
     * Generates a new Key Hash that is immediately after the given one. We define Key Hash H2 to be immediately after
     * Key Hash h1 if there doesn't exist Key Hash H3 such that H1&lt;H3&lt;H2. The ordering is performed using {@link UUID#compareTo}.
     *
     * @return The successor Key Hash, or null if no more successors are available (if {@link IteratorState#isEnd} returns true).
     */
    static UUID getNextHash(UUID hash) {
        if (hash == null) {
            // No hash given. By definition, the first hash is the "next" one".
            hash = MIN_HASH;
        } else if (hash.compareTo(MAX_HASH) >= 0) {
            // Given hash already equals or exceeds the max value. There is no successor.
            return null;
        }

        long msb = hash.getMostSignificantBits();
        long lsb = hash.getLeastSignificantBits();
        if (lsb == Long.MAX_VALUE) {
            msb++; // This won't overflow since we've checked that state is not end (i.e., id != MAX).
            lsb = Long.MIN_VALUE;
        } else {
            lsb++;
        }

        return new UUID(msb, lsb);
    }

    /**
     * Determines whether the given UUID is a valid Key Hash.
     *
     * @param keyHash The UUID to test.
     * @return True if a valid Key Hash, false otherwise.
     */
    static boolean isValid(UUID keyHash) {
        return MIN_HASH.compareTo(keyHash) <= 0 && MAX_HASH.compareTo(keyHash) >= 0;
    }

    /**
     * Creates a new instance of the KeyHasher class that generates hashes using the SHA-256 algorithm.
     *
     * @return A new instance of the KeyHasher class.
     */
    static KeyHasher sha256() {
        return new Sha256Hasher();
    }

    /**
     * Creates a new instance of the KeyHasher class that generates custom hashes, based on the given Function.
     *
     * @param hashFunction A Function that, given an {@link BufferView}, produces a byte array representing its hash.
     * @return A new instance of the KeyHasher class.
     */
    @VisibleForTesting
    static KeyHasher custom(Function<BufferView, byte[]> hashFunction) {
        return new CustomHasher(hashFunction);
    }

    //region Sha256Hasher

    private static class Sha256Hasher extends KeyHasher {
        private static final HashFunction HASH = Hashing.sha256();

        @Override
        public UUID hash(@NonNull BufferView key) {
            val h = HASH.newHasher();
            key.collect(h::putBytes);
            byte[] rawHash = new byte[HASH_SIZE_BYTES];
            int c = h.hash().writeBytesTo(rawHash, 0, rawHash.length);
            assert c == rawHash.length;
            return toUUID(rawHash);
        }
    }

    //endregion

    //region CustomHasher

    @RequiredArgsConstructor
    private static class CustomHasher extends KeyHasher {
        @NonNull
        private final Function<BufferView, byte[]> hashFunction;

        @Override
        public UUID hash(@NonNull BufferView key) {
            byte[] rawHash = this.hashFunction.apply(key);
            Preconditions.checkState(rawHash.length == HASH_SIZE_BYTES, "Resulting KeyHash has incorrect length.");
            return toUUID(rawHash);
        }
    }

    //endregion
}