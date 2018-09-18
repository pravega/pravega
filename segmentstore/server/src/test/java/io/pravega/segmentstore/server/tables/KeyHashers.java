/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.ArrayView;
import io.pravega.segmentstore.server.tables.hashing.HashConfig;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import lombok.val;

/**
 * Key Hashers used throughout testing.
 */
class KeyHashers {

    // Default Hashing uses a SHA512 hashing, which produces very few collisions. This is the same as used in the non-test
    // version of the code.
    static final HashConfig HASH_CONFIG = HashConfig.of(
            AttributeCalculator.PRIMARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH,
            AttributeCalculator.SECONDARY_HASH_LENGTH);

    /**
     * 64-byte Hasher using {@link #HASH_CONFIG} and generating a SHA-512 hash using {@link KeyHasher#sha512}.
     */
    static final KeyHasher DEFAULT_HASHER = KeyHasher.sha512(HASH_CONFIG);

    // Collision Hashing "bucketizes" the SHA512 hash into much smaller buckets with each accepting a very narrow
    // range [0..4) of values. This will produce a lot of collisions, which helps us test adjustable tree depth and
    // collision resolutions in the indexer.
    private static final int COLLISION_HASH_BASE = 4;
    static final int COLLISION_HASH_BUCKETS = (int) Math.pow(COLLISION_HASH_BASE, HASH_CONFIG.getHashCount());

    /**
     * 64-byte Hasher using {@link #HASH_CONFIG} and generating a collision-prone hash (with at most 1024 hash values).
     */
    static final KeyHasher COLLISION_HASHER = KeyHasher.custom(KeyHashers::hashWithCollisions, HASH_CONFIG);

    private static byte[] hashWithCollisions(ArrayView arrayView) {
        // Generate SHA512 hash. We'll use this as the basis for our collision-prone hash.
        val baseHash = DEFAULT_HASHER.hash(arrayView);

        // "Bucketize" it into COLLISION_HASH_BUCKETS buckets (i.e., hash value will be in interval [0..COLLISION_HASH_BUCKETS)).
        int hashValue = HashHelper.seededWith(IndexReaderWriterTests.class.getName()).hashToBucket(baseHash.array(), COLLISION_HASH_BUCKETS);

        // Split the hash value into parts, and generate the final hash. The final hash must still be the same length as
        // the original, so we'll fill it with 0s and only populate one byte of each Hash Part with some value we calculate
        // below.
        byte[] result = new byte[HASH_CONFIG.getMinHashLengthBytes()];
        int resultOffset = 0;
        for (int i = 0; i < HASH_CONFIG.getHashCount(); i++) {
            resultOffset += baseHash.getPart(i).getLength(); // Only populate one byte per part. Skip the rest.
            result[resultOffset - 1] = (byte) (hashValue % COLLISION_HASH_BASE);
            hashValue /= COLLISION_HASH_BASE;
        }

        // At the end, we should have a COLLISION_HASH_CONFIG.getMinHashLengthBytes() hash, with each hash part being 1 byte
        // containing values from 0 (inclusive) until COLLISION_HASH_BASE (exclusive).
        return result;
    }
}
