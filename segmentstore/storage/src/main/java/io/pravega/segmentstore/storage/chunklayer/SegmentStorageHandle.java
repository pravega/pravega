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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.SegmentHandle;
import lombok.Data;
import lombok.NonNull;

/**
 * Defines a Handle that can be used to operate on Segments in Storage.
 * Storage provider specific derived classes may have additional fields to maintain additional internal state.
 */
@Data
public class SegmentStorageHandle implements SegmentHandle {
    /**
     * Name of the Segment, as perceived by users of the Storage interface.
     */
    @NonNull
    private final String segmentName;

    /**
     * Value indicating whether this Handle was open in ReadOnly mode (true) or ReadWrite mode (false).
     */
    private final boolean isReadOnly;

    /**
     * Creates a read only handle for a given segment name.
     *
     * @param streamSegmentName Name of the segment.
     * @return A read only handle.
     */
    public static SegmentStorageHandle readHandle(String streamSegmentName) {
        return new SegmentStorageHandle(streamSegmentName, true);
    }

    /**
     * Creates a writable/updatable handle for a given segment name.
     *
     * @param streamSegmentName Name of the segment.
     * @return A writable/updatable handle.
     */
    public static SegmentStorageHandle writeHandle(String streamSegmentName) {
        return new SegmentStorageHandle(streamSegmentName, false);
    }
}
