/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.storage.impl.hdfs;

import java.io.IOException;

/**
 * Exception that indicates the set of files representing a Segment is corrupted.
 */
class SegmentFilesCorruptedException extends IOException {
    /**
     * Creates a new instance of the SegmentFilesCorruptedException.
     *
     * @param segmentName The name of the segment.
     * @param file        The file that is out of place.
     * @param message     Custom message.
     */
    SegmentFilesCorruptedException(String segmentName, FileDescriptor file, String message) {
        super(String.format("Segment '%s' has invalid file '%s'. %s", segmentName, file.getPath(), message));
    }
}
