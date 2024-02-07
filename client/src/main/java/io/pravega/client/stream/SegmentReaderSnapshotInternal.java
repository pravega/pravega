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

package io.pravega.client.stream;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.SegmentReaderSnapshotImpl;

import java.nio.ByteBuffer;

/**
 * SegmentReaderSnapshot provides the information about the current position, segment id and the status of segment reader.
 *
 */
public interface SegmentReaderSnapshotInternal extends SegmentReaderSnapshot {
    /**
     * Gets the segment which segment reader is reading.
     *
     * @return The segment.
     */
    Segment getSegment();

    /**
     * Gets the current position of segment reader.
     *
     * @return The current position.
     */
    long getPosition();

    /**
     * A boolean indicating if all events in the segment read completely.
     * This is true when the segment is sealed and all events in the segment have already been read.
     *
     * @return true if all events in the segment read completely.
     */
    boolean isEndOfSegment();

    /**
     * Deserializes the segment reader snapshot from its serialized form obtained from calling {@link #toBytes()}.
     *
     * @param serializedSnapshot A serialized segment reader snapshot.
     * @return The segment reader snapshot object.
     */
    static SegmentReaderSnapshot fromBytes(ByteBuffer serializedSnapshot) {
        return SegmentReaderSnapshotImpl.fromBytes(serializedSnapshot);
    }
}
