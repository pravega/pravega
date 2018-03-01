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
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.StreamCut;

/**
 * Please note this is an experimental API.
 * 
 * Used to get metadata about and read from an existing streams.
 * <p>
 * All events written to a stream will be visible to SegmentIterators
 * <p>
 * Events within a segment are strictly ordered, but as this API allows for reading from multiple
 * segments in parallel without adhering to time ordering. This allows for events greater
 * parallelization at the expense of the ordering guarantees provided by {@link EventStreamReader}.
 */
@Beta
public interface BatchClient {

    /**
     * Provide a list of segments for a given stream between fromStreamCut and toStreamCut.
     * i) If only fromStreamCut is provided then
     *      - All segments up to the tail of the stream are returned.
     *      - StreamSegmentsInfo.endStreamCut is populated with the effective end point of the stream.
     * ii) If only toStreamCut is provided then
     *      - All segments from the start of the stream to toStreamCut are returned.
     *      - StreamSegmentsInfo.startStreamCut is populated with the stream cut which points to start of the stream.
     * iii) If neither fromStreamCut nor toStreamCut are specified
     *      - then all segments from the start of the stream upto the current tail of the stream are returned.

     * @param stream the stream.
     * @param fromStreamCut starting stream cut.
     * @param toStreamCut end stream cut.
     * @return Segment information between the two stream cuts.
     */
    StreamSegmentsInfo getSegments(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut);

    /**
     * Provides a SegmentIterator to read the events in the requested segment starting from the
     * beginning of the segment and ending at the current end of the segment.
     * 
     * @param <T> The type of events written to the segment.
     * @param segment The segment to read from
     * @param deserializer A deserializer to be used to parse events
     * @return A SegmentIterator over the requested segment
     */
    <T> SegmentIterator<T> readSegment(SegmentRange segment, Serializer<T> deserializer);

}
