/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.io.Serializable;

/**
 * This is used to represent range bounded portion of a Segment.
 */
@Beta
public interface SegmentRange extends Serializable {

    /**
     * Returns the segment number of Segment.
     * @return The segment number
     */
    long getSegmentId();

    /**
     * Returns the stream name the segment is associated with.
     * @return The stream name.
     */
    String getStreamName();

    /**
     * Returns the scope name of the stream the segment is associated with.
     * @return The scope name.
     */
    String getScope();

    /**
     * Returns the start offset of the segment.
     * @return The start offset.
     */
    long getStartOffset();

    /**
     * Returns the end offset of the segment.
     * @return The end offset.
     */
    long getEndOffset();

    /**
     * For internal use. Do not call.
     * @return This
     */
    SegmentRangeImpl asImpl();
}
