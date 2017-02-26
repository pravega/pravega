/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.server.LogItem;

/**
 * Defines a Log Operation that deals with a Segment.
 */
public interface SegmentOperation extends LogItem {
    /**
     * Gets a value indicating the Id of the StreamSegment this operation relates to.
     */
    long getStreamSegmentId();
}
