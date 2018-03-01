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
import io.pravega.client.batch.impl.StreamSegmentsInfoImpl;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamCut;
import java.util.Iterator;

@Beta
public interface StreamSegmentsInfo {

    /**
     * This returns an iterator for {@link SegmentRange} specified in {@link BatchClient#getSegments(Stream, StreamCut, StreamCut)}.
     * @return Iterator for {@link SegmentRange}
     */
    Iterator<SegmentRange> getSegmentRangeIterator();

    /**
     * For internal use. Do not call.
     * @return This
     */
    StreamSegmentsInfoImpl asImpl();
}
