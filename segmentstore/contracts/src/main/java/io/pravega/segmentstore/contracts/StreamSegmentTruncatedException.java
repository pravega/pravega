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

import lombok.Getter;

/**
 * An Exception that indicates a StreamSegment has been truncated and certain offsets cannot be accessed anymore.
 */
public class StreamSegmentTruncatedException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Lowest accessible offset for the segment.
     */
    @Getter
    final long startOffset;

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param segmentName Name of the truncated Segment.
     * @param message     Message.
     * @param ex          Causing exception.
     */
    public StreamSegmentTruncatedException(String segmentName, String message, Throwable ex) {
        super(segmentName, message, ex);
        this.startOffset = 0;
    }

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param startOffset First valid offset of the StreamSegment.
     */
    public StreamSegmentTruncatedException(long startOffset) {
        super("", String.format("Segment truncated: Lowest accessible offset is %d.", startOffset));
        this.startOffset = startOffset;
    }

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     * @param segmentName Name of the truncated Segment.
     * @param startOffset First valid offset of the StreamSegment.
     * @param requestedOffset Requested offset.
     */
    public StreamSegmentTruncatedException(String segmentName, long startOffset, long requestedOffset) {
        super(segmentName, String.format("Segment truncated: Lowest accessible offset is %d. (%d was requested)", startOffset, requestedOffset));
        this.startOffset = startOffset;
    }
}
