/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.SequencedItemList;

/**
 * Defines a Log Operation that deals with a Segment.
 */
public interface SegmentOperation extends SequencedItemList.Element {
    /**
     * Gets a value indicating the Id of the StreamSegment this operation relates to.
     * @return StreamSegment Id
     */
    long getStreamSegmentId();
}
