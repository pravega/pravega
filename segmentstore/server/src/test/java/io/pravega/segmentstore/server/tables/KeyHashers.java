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

import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.BitConverter;
import io.pravega.common.util.BufferView;

/**
 * Key Hashers used throughout testing.
 */
class KeyHashers {
    /**
     * Hasher using a 16-byte hash based on SHA-256.
     */
    static final KeyHasher DEFAULT_HASHER = KeyHasher.sha256();

    // Collision Hashing "bucketizes" the DEFAULT_HASHER hash into much smaller buckets, which helps us test collision handling.
    static final int COLLISION_HASH_BUCKETS = 1024;

    /**
     * Hasher generating a collision-prone hash (with at most 1024 hash values).
     */
    static final KeyHasher COLLISION_HASHER = KeyHasher.custom(KeyHashers::hashWithCollisions);

    /**
     * Hasher generating the same hash for all values.
     */
    static final KeyHasher CONSTANT_HASHER = KeyHasher.custom(KeyHashers::hashConstant);

    private static byte[] hashConstant(BufferView ignored) {
        return new byte[KeyHasher.HASH_SIZE_BYTES];
    }

    private static byte[] hashWithCollisions(BufferView bufferView) {
        int hashValue = HashHelper.seededWith(IndexReaderWriterTests.class.getName()).hashToBucket(bufferView, COLLISION_HASH_BUCKETS);
        byte[] result = new byte[KeyHasher.HASH_SIZE_BYTES];
        BitConverter.writeInt(result, 0, hashValue);
        return result;
    }
}
