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

import io.pravega.client.stream.impl.StreamCut;
import java.util.Iterator;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * This class contains the segment information of a stream between two StreamCuts.
 */
@Data
@Builder
public class StreamSegmentInfo {

    @NonNull
    private final StreamCut startStreamCut;
    @NonNull
    private final StreamCut endStreamCut;
    @NonNull
    private final Iterator<SegmentMetadata> segmentMetaDataIterator;
}
