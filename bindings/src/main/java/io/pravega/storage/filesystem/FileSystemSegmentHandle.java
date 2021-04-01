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
package io.pravega.storage.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SegmentHandle;

/**
 * Handle for FileSystem.
 */
public class FileSystemSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;

    /**
     * Creates a new instance of FileSystem segment handle.
     * @param streamSegmentName Name of the segment.
     * @param isReadOnly  Whether the segment is read only or not.
     */
    public FileSystemSegmentHandle(String streamSegmentName, boolean isReadOnly) {
        this.segmentName = Preconditions.checkNotNull(streamSegmentName, "segmentName");
        this.isReadOnly = isReadOnly;
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    public static FileSystemSegmentHandle readHandle(String streamSegmentName) {
        return new FileSystemSegmentHandle(streamSegmentName, true);
    }

    public static FileSystemSegmentHandle writeHandle(String streamSegmentName) {
        return new FileSystemSegmentHandle(streamSegmentName, false);
    }
}
