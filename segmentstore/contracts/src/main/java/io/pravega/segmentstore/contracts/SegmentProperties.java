/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import io.pravega.common.util.ImmutableDate;
import java.util.Map;
import java.util.UUID;

/**
 * General properties about a StreamSegment.
 */
public interface SegmentProperties {
    /**
     * Gets a value indicating the name of this StreamSegment.
     */
    String getName();

    /**
     * Gets a value indicating whether this StreamSegment is sealed for modifications.
     */
    boolean isSealed();

    /**
     * Gets a value indicating whether this StreamSegment is deleted (does not exist).
     */
    boolean isDeleted();

    /**
     * Gets a value indicating the last modification time of the StreamSegment.
     */
    ImmutableDate getLastModified();

    /**
     * Gets a value indicating the first offset in the Segment available for reading. For non-truncated Segments, this
     * will return 0 (whole segment is available for reading), while for truncated Segments, it will return the last
     * truncation offset.
     */
    long getStartOffset();

    /**
     * Gets a value indicating the full, readable length of the StreamSegment. This includes the range of bytes that are
     * inaccessible due to them being before the StartOffset.
     */
    long getLength();

    /**
     * Gets a read-only Map of AttributeId-Values for this Segment.
     *
     * @return The map.
     */
    Map<UUID, Long> getAttributes();
}

