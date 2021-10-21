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

import com.google.common.base.Preconditions;
import java.util.UUID;
import lombok.Getter;

/**
 * Result from {@link ContainerKeyCache#get(long, UUID)}.
 */
@Getter
class CacheBucketOffset {
    /**
     * We flip the first bit for instances with {@link #isRemoval} being true.
     */
    private static final long FIRST_BIT = Long.MIN_VALUE;
    /**
     * The offset within the Segment where the latest update (or removal) for any key in the Table Bucket is located.
     */
    private final long segmentOffset;
    /**
     * True if the Key at given Segment Offset is removed, false otherwise.
     */
    private final boolean removal;

    /**
     * Creates a new instance of the CacheBucketOffset class.
     *
     * @param segmentOffset The offset within the Segment where the latest update (or removal) for any key in the Table
     *                      Bucket is located.
     * @param isRemoval     True if the Key at given Segment Offset is removed, false otherwise.
     */
    CacheBucketOffset(long segmentOffset, boolean isRemoval) {
        Preconditions.checkArgument(segmentOffset >= 0, "segmentOffset must be a non-negative number.");
        this.segmentOffset = segmentOffset;
        this.removal = isRemoval;
    }


    /**
     * Encodes the contents of this instance into a long. This can be decoded using {@link #decode}.
     *
     * @return The encoded value.
     */
    long encode() {
        return this.removal ? this.segmentOffset | FIRST_BIT : this.segmentOffset;
    }

    /**
     * Decodes a long value generated using {@link #encode} into a {@link CacheBucketOffset} instance.
     *
     * @param value The encoded value.
     * @return A {@link CacheBucketOffset instance.}
     */
    static CacheBucketOffset decode(long value) {
        return value < 0 ? new CacheBucketOffset(value ^ FIRST_BIT, true) : new CacheBucketOffset(value, false);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheBucketOffset) {
            CacheBucketOffset other = (CacheBucketOffset) obj;
            return this.segmentOffset == other.segmentOffset && this.removal == other.removal;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(this.segmentOffset);
    }

    @Override
    public String toString() {
        return String.format("%s%s", this.segmentOffset, this.removal ? " [DELETED]" : "");
    }
}