/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.ecs;

import io.pravega.segmentstore.storage.SegmentHandle;

public class ECSSegmentHandle implements SegmentHandle {
    private final String segmentName;
    private final boolean isReadOnly;

    public ECSSegmentHandle(String streamSegmentName, boolean isReadOnly) {
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

    public static ECSSegmentHandle getReadHandle(String streamSegmentName) {
        return new ECSSegmentHandle(streamSegmentName, true);
    }

    public static ECSSegmentHandle getWriteHandle(String streamSegmentName) {
        return new ECSSegmentHandle(streamSegmentName, false);
    }
}
