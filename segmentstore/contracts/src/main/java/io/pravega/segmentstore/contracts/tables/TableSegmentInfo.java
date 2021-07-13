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
package io.pravega.segmentstore.contracts.tables;

import io.pravega.segmentstore.contracts.SegmentType;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * General Table Segment Information.
 */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
@Builder
public final class TableSegmentInfo {
    /**
     * The name of the Table Segment.
     */
    private final String name;
    /**
     * The length of the Table Segment.
     * NOTE: The actual size (in bytes) is {@link #getLength()} - {@link #getStartOffset()} (account for truncation offset).
     */
    private final long length;
    /**
     * The Start Offset (Truncation Offset) of the Table Segment.
     */
    private final long startOffset;
    /**
     * Gets the number of indexed entries (unique keys).
     *
     * NOTE: this is an "eventually consistent" value:
     * - In-flight (not yet acknowledged) updates and removals are not included.
     * - Recently acknowledged updates and removals may or may not be included (depending on whether they were conditional
     * or not). As the index is updated (in the background), this value will converge towards the actual number of unique
     * keys in the index.
     */
    private final long entryCount;
    /**
     * Gets the Key Length. 0 means variable key length; any non-zero (positive) indicates a Fixed Key Length Table Segment.
     */
    private final int keyLength;
    /**
     * The {@link SegmentType} for the segment.
     */
    private final SegmentType type;

    @Override
    public String toString() {
        return String.format("Name = %s, Entries = %s, StartOffset = %d, Length = %d, Type = %s, KeyLength = %s",
                getName(), getEntryCount(), getStartOffset(), getLength(), getType(), getKeyLength());
    }
}
