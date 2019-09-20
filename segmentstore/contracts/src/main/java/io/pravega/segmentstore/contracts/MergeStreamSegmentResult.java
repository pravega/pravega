/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import java.util.Map;
import java.util.UUID;
import lombok.Data;

@Data
public class MergeStreamSegmentResult {

    /**
     * The new length of the target segment after the merge.
     * This should be equal to the prior segment length plus {@link #getMergedDataLength()}
     * 
     * @return The length of the target segment.
     */
    private final long targetSegmentLength;
    
    /**
     * Gets a value indicating the amount of data merged into the StreamSegment. 
     * This should be the same as the size of the source segment.
     *
     * @return full readable length of the stream segment, including inaccessible bytes before the start offset
     */
    private final long mergedDataLength;
    
    /**
     * Gets a read-only Map of AttributeId-Values of the source Segment.
     *
     * @return The source segment attributes
     */
    private final Map<UUID, Long> attributes;
    
}
