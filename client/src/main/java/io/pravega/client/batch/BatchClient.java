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
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import java.util.concurrent.CompletableFuture;

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
     * Get information about a given Stream, {@link StreamInfo}.
     * @deprecated
     *   Use {@link io.pravega.client.admin.StreamManager#getStreamInfo(String, String)} to fetch StreamInfo.
     *
     * @param stream the stream.
     * @return stream information.
     */
    @Deprecated
    CompletableFuture<StreamInfo> getStreamInfo(Stream stream);

    /**
     * Provide a list of segments for a given stream between fromStreamCut and toStreamCut.
     * Passing StreamCut.UNBOUNDED or null to fromStreamCut and toStreamCut will result in using the current start of
     * stream and the current end of stream respectively.
     *<p>
     * Note: In case of stream truncation: <p>
     * - Passing a null to fromStreamCut will result in using the current start of the Stream post truncation.<p>
     * - Passing a fromStreamCut which points to the truncated stream will result in a {@link NoSuchSegmentException} while
     * iterating over SegmentRange iterator obtained via {@link StreamSegmentsIterator#getIterator()}
     *
     * @param stream the stream.
     * @param fromStreamCut starting stream cut.
     * @param toStreamCut end stream cut.
     * @return Segment information between the two stream cuts.
     */
    StreamSegmentsIterator getSegments(Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut);

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
