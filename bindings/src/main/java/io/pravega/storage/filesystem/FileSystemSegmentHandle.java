/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
