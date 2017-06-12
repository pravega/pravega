/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import io.pravega.segmentstore.storage.SegmentHandle;

public class FileSystemSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;

    public FileSystemSegmentHandle(String streamSegmentName, boolean isReadOnly) {
        this.segmentName = streamSegmentName;
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

    public static FileSystemSegmentHandle getReadHandle(String streamSegmentName) {
        return new FileSystemSegmentHandle(streamSegmentName, true);
    }

    public static FileSystemSegmentHandle getWriteHandle(String streamSegmentName) {
        return new FileSystemSegmentHandle(streamSegmentName, false);
    }
}
