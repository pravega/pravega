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
package io.pravega.segmentstore.contracts;

import io.pravega.common.util.ImmutableDate;
import java.util.Map;

/**
 * General properties about a StreamSegment.
 */
public interface SegmentProperties {
    /**
     * Gets a value indicating the name of this StreamSegment.
     *
     * @return name of this stream segment
     */
    String getName();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     *
     * @return true if Stream Segment has been sealed for modifications
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     *
     * @return true if this Stream Segment does not exist
     */
    boolean isDeleted();

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     *
     * @return time of last modification of the stream segment
     */
    ImmutableDate getLastModified();

    /**
     * Gets a value indicating the first offset in the Segment available for reading. For non-truncated Segments, this
     * will return 0 (whole segment is available for reading), while for truncated Segments, it will return the last
     * truncation offset.
     *
     * @return long indicating the first offset available for reading in the segment
     *
     */
    long getStartOffset();

    /**
     * Gets a value indicating the full, readable length of the StreamSegment. This includes the range of bytes that are
     * inaccessible due to them being before the StartOffset.
     *
     * @return full readable length of the stream segment, including inaccessible bytes before the start offset
     *
     */
    long getLength();

    /**
     * Gets a read-only Map of AttributeId-Values for this Segment.
     *
     * @return The map.
     */
    Map<AttributeId, Long> getAttributes();
}

