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
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import java.util.Iterator;

/**
 * Please note this is an experimental API.
 * 
 * Used to get metadata about and read from an existing streams.
 * <p>
 * All events written to a stream will be visible to SegmentIterators and reflected in
 * {@link SegmentInfo#getLength()}.
 * <p>
 * Events within a segment are strictly ordered, but as this API allows for reading from multiple
 * segments in parallel without adhering to time ordering. This allows for events greater
 * parallelization at the expense of the ordering guarantees provided by {@link EventStreamReader}.
 */
@Beta
public interface BatchClient {

    /**
     * Provides a list of segments and their metadata for a given stream.
     *
     * @param stream the stream
     * @return The segments in the requested stream.
     */
    Iterator<SegmentInfo> listSegments(Stream stream);

    /**
     * Provides a SegmentIterator to read the events in the requested segment starting from the
     * beginning of the segment and ending at the current end of the segment.
     * 
     * @param <T> The type of events written to the segment.
     * @param segment The segment to read from
     * @param deserializer A deserializer to be used to parse events
     * @return A SegmentIterator over the requested segment
     */
    <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer);

    /**
     * Provides a SegmentIterator to read the events after the startingOffset in the requested
     * segment ending at the current end of the segment.
     *
     * Offsets can be obtained by calling {@link SegmentIterator#getOffset()} or
     * {@link SegmentInfo#getWriteOffset()}. There is no validation that the provided offset actually
     * aligns to an event. If it does not, the deserializer will be passed corrupt data. This means
     * that it is invalid to, for example, attempt to divide a segment by simply passing a starting
     * offset that is half of the segment length.
     *
     * @param <T> The type of events written to the segment.
     * @param segment The segment to read from
     * @param deserializer A deserializer to be used to parse events
     * @param startingOffset The offset to start iterating from.
     * @return A SegmentIterator over the requested segment at startingOffset
     */
    <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer, long startingOffset);

}
