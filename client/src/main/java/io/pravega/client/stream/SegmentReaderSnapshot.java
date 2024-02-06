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

import java.nio.ByteBuffer;

/**
 * SegmentReaderSnapshot provides the information about the current position, segment id and the status of segment reader.
 * Note that this is serializable so that it can be written to an external datastore.
 *
 */
public interface SegmentReaderSnapshot {

    /**
     * Serializes the segment reader snapshot to a compact byte array.
     *
     * @return compact byte array
     */
    ByteBuffer toBytes();

    //TODO : To implement SourceSplit interface, i think we need this method. Else this can be removed.
    /**
     * Get the assigned segment name.
     *
     * @return segment name.
     */
    String getSegmentId();

    /**
     * Deserializes the segment reader snapshot from its serialized from obtained from calling {@link #toBytes()}.
     *
     * @param serializedSnapshot A serialized segment reader snapshot.
     * @return The SegmentReaderSnapshot object.
     */
    static SegmentReaderSnapshot fromBytes(ByteBuffer serializedSnapshot) {
        return SegmentReaderSnapshotInternal.fromBytes(serializedSnapshot);
    }

}
