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

import java.util.Iterator;

public interface SegmentIterator<T> extends Iterator<T>, AutoCloseable {

    /**
     * Provides an offset which can be used to re-create a segmentIterator at this position by
     * calling {@link BatchClient#readSegment(io.pravega.client.segment.impl.Segment, io.pravega.client.stream.Serializer, long)}.
     * 
     * @return The current offset in the segment
     */
    long getOffset();

    /**
     * Closes the iterator, freeing any resources associated with it.
     * 
     * @see java.lang.AutoCloseable#close()
     */
    void close();

}
