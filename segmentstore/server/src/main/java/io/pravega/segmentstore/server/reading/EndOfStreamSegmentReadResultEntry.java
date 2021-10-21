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

import java.time.Duration;

/**
 * Read Result Entry with no content that marks the end of the StreamSegment.
 * The getContent() method will throw an IllegalStateException if invoked.
 */
class EndOfStreamSegmentReadResultEntry extends ReadResultEntryBase {
    /**
     * Constructor.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    EndOfStreamSegmentReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(ReadResultEntryType.EndOfStreamSegment, streamSegmentOffset, requestedReadLength);
        fail(new IllegalStateException("EndOfStreamSegmentReadResultEntry does not have any content."));
    }

    @Override
    public void requestContent(Duration timeout) {
        throw new IllegalStateException("EndOfStreamSegmentReadResultEntry does not have any content.");
    }
}
