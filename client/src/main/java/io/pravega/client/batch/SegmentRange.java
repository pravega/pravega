/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch;

import com.google.common.annotations.Beta;
import io.pravega.client.batch.impl.SegmentRangeImpl;

/**
 * This is used to represent range bounded portion of a Segment.
 */
@Beta
public interface SegmentRange {

    /**
     * Returns the segment number of Segment.
     * @return The segment number
     */
    int getSegmentNumber();

    /**
     * Returns the stream name the segment is associated with.
     * @return The stream name.
     */
    String getStreamName();

    /**
     * Returns the scope name.
     * @return The scope name.
     */
    String getScopeName();

    /**
     * For internal use. Do not call.
     * @return This
     */
    SegmentRangeImpl asImpl();
}
