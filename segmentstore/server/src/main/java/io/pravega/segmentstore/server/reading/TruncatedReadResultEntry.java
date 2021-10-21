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
package io.pravega.segmentstore.server.reading;

import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;

/**
 * ReadResultEntry for data that is no longer available due to the StreamSegment being truncated beyond its starting offset.
 */
class TruncatedReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the ReadResultEntry class.
     *
     * @param segmentReadOffset   The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @param segmentStartOffset  The first offset in the StreamSegment available for reading.
     * @param segmentName         Name of the segment.
     */
    TruncatedReadResultEntry(long segmentReadOffset, int requestedReadLength, long segmentStartOffset, String segmentName) {
        super(ReadResultEntryType.Truncated, segmentReadOffset, requestedReadLength);
        fail(new StreamSegmentTruncatedException(segmentName, segmentStartOffset, segmentReadOffset));
    }
}
