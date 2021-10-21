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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.ReadResultEntryType;

/**
 * Read Result Entry for data that is readily available for reading (in memory).
 */
class CacheReadResultEntry extends ReadResultEntryBase {
    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at. NOTE: this is
     *                            where the first byte of 'data' starts; internally it will set {@link #getStreamSegmentOffset()}
     *                            to the sum of this value and dataOffset.
     * @param data                The data buffer that contains the data.
     * @param dataOffset          The offset within data where this ReadResultEntry starts at.
     * @param dataLength          The length of the data that this ReadResultEntry has.
     */
    @VisibleForTesting
    CacheReadResultEntry(long streamSegmentOffset, byte[] data, int dataOffset, int dataLength) {
        this(streamSegmentOffset + dataOffset, new ByteArraySegment(data, dataOffset, dataLength));
    }

    /**
     * Creates a new instance of the CacheReadResultEntry class.
     *
     * @param streamSegmentOffset The offset within the StreamSegment where this ReadResultEntry starts at.
     * @param data                An InputStream representing the data to be read.
     */
    CacheReadResultEntry(long streamSegmentOffset, BufferView data) {
        super(ReadResultEntryType.Cache, streamSegmentOffset, data.getLength());
        complete(data);
    }
}
