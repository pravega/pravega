/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeepertier2;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SegmentHandle;

public class BookkeeperSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;

    /**
     * Creates a new instance of FileSystem segment handle.
     * @param streamSegmentName Name of the segment.
     * @param isReadOnly  Whether the segment is read only or not.
     */
    public BookkeeperSegmentHandle(String streamSegmentName, boolean isReadOnly) {
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

    public static SegmentHandle readHandle(String streamSegmentName) {
        return new BookkeeperSegmentHandle(streamSegmentName, true);
    }

    public static SegmentHandle writeHandle(String streamSegmentName) {
        return new BookkeeperSegmentHandle(streamSegmentName, false);
    }
}
