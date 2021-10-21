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

import io.pravega.segmentstore.contracts.Attributes;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Represents a Table Bucket, which is identified by its unique Hash ({@link #getHash} and which is a collection of one
 * or more Table Entries. The Table Bucket points to the Table Entry with highest offset in the Segment.
 */
@RequiredArgsConstructor
class TableBucket {
    /**
     * The MSB 64 bits of the Attribute Keys that represent Core Attributes.
     */
    static final long CORE_ATTRIBUTE_PREFIX = Attributes.CORE_ATTRIBUTE_ID_PREFIX;
    /**
     * The MSB 64 bits of the Attribute Keys that represent Backpointers.
     */
    static final long BACKPOINTER_PREFIX = Long.MAX_VALUE;

    /**
     * The Table Bucket's Hash.
     */
    @Getter
    @NonNull
    private final UUID hash;

    /**
     * The highest Offset of any Table Entry in this Bucket.
     */
    @Getter
    private final long segmentOffset;

    /**
     * Gets a value indicating whether this TableBucket exists.
     * @return True if it exists (points to a real Table Entry) or false otherwise.
     */
    boolean exists() {
        return this.segmentOffset >= 0;
    }

    @Override
    public int hashCode() {
        return this.hash.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TableBucket) {
            return this.hash.equals(((TableBucket) obj).hash);
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("Hash = %s, Offset = %s", this.hash, this.segmentOffset);
    }
}