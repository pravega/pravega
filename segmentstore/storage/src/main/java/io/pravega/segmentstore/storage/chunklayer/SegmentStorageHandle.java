/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SegmentHandle;

public class SegmentStorageHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;

    /**
     * Creates a new instance of segment handle.
     * @param streamSegmentName Name of the segment.
     * @param isReadOnly  Whether the segment is read only or not.
     */
    public SegmentStorageHandle(String streamSegmentName, boolean isReadOnly) {
        this.segmentName = Preconditions.checkNotNull(streamSegmentName, "segmentName");
        this.isReadOnly = isReadOnly;
    }

    /**
     * Gets the name of the Segment, as perceived by users of the Storage interface.
     */
    @Override
    public String getSegmentName() {
        return segmentName;
    }

    /**
     * Gets a value indicating whether this Handle was open in ReadOnly mode (true) or ReadWrite mode (false).
     * @return True if the handle is read only.
     */
    @Override
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Creates a read only handle for a given segment name.
     * @param streamSegmentName Name of the segment.
     * @return A read only handle.
     */
    public static SegmentStorageHandle readHandle(String streamSegmentName) {
        return new SegmentStorageHandle(streamSegmentName, true);
    }

    /**
     * Creates a writable/updatable handle for a given segment name.
     * @param streamSegmentName Name of the segment.
     * @return A writable/updatable handle.
     */
    public static SegmentStorageHandle writeHandle(String streamSegmentName) {
        return new SegmentStorageHandle(streamSegmentName, false);
    }
}
