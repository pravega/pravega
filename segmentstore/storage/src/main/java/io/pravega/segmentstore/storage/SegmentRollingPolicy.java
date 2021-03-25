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
package io.pravega.segmentstore.storage;

import com.google.common.base.Preconditions;
import io.pravega.common.io.serialization.RevisionDataOutput;
import lombok.Getter;

/**
 * A generic rolling policy that can be applied to any Storage unit.
 */
public final class SegmentRollingPolicy {
    /**
     * The max allowed value for chunk length.
     */
    public static final long MAX_CHUNK_LENGTH = RevisionDataOutput.COMPACT_LONG_MAX;

    /**
     * Max rolling length is max 62 bit unsigned number (2^62-1) therefore it requires only 62 bits for storage.
     * This allows us to use CompactLong in serialization everywhere. The resulting value is large enough for practical purposes.
     */
    public static final SegmentRollingPolicy NO_ROLLING = new SegmentRollingPolicy(MAX_CHUNK_LENGTH);

    /**
     * Maximum length, as allowed by this Rolling Policy.
     */
    @Getter
    private final long maxLength;

    /**
     * Creates a new instance of the Rolling Policy class.
     *
     * @param maxLength The maximum length as allowed by this Rolling Policy.
     */
    public SegmentRollingPolicy(long maxLength) {
        Preconditions.checkArgument(maxLength > 0, "maxLength must be a positive number.");
        this.maxLength = maxLength;
    }

    @Override
    public String toString() {
        return String.format("MaxLength = %d", this.maxLength);
    }
}
