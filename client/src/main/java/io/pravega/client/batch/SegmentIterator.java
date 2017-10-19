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
import java.util.Iterator;

/**
 * Please note this is an experimental API.
 * 
 * Allows for reading data from a segment. Returns an item from {@link #next()} for each event in
 * the segment at the time of its creation. Once all the events that were in the stream at the time
 * of the creation of the SegmentIterator have been returned {@link #hasNext()} will return false.
 *
 * While buffering is used to avoid it, it is possible for {@link #next()} to block on fetching the
 * data.
 * 
 * At any time {@link #getOffset()} can be called to get the byte offset in the segment the iterator
 * is currently pointing to. This can be used to call
 * {@link BatchClient#readSegment(io.pravega.client.segment.impl.Segment, io.pravega.client.stream.Serializer, long, long)}
 * to create another SegmentIterator at this offset.
 * 
 * @param <T> The type of the events written to this segment.
 */
@Beta
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
