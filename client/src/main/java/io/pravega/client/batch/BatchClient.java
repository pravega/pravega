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

import java.util.Date;
import java.util.List;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;

/**
 * Used to get metadata about and read from an existing streams.
 * <p>
 * All events written to a stream will be visible atomically and their existence reflected in {@link SegmentInfo#getLength()} and 
 * to new SegmentIterators.
 * <p>
 * A note on ordering: Events inside of a stream have a strict order, but may need to be divided
 * between multiple readers for scaling. In order to process events in parallel on different hosts
 * and still have some ordering guarantees; events written to a stream have a routingKey see
 * {@link EventStreamWriter#writeEvent(String, Object)}. Events within a routing key are strictly
 * ordered (i.e. They must go the the same reader or its replacement). However because
 * {@link ReaderGroup}s process events in parallel there is no ordering between different readers.
 * 
 * <p>
 * A note on scaling: Because a stream can grow in its event rate, streams are divided into
 * Segments. For the most part this is an implementation detail. However its worth understanding
 * that the way a stream is divided between multiple readers in a group that wish to split the
 * messages between them is by giving different segments to different readers.
 */
public interface BatchClient {

    /**
     * List all of the streams in a given scope.
     * 
     * @param scope The scope of streams to list.
     * @return The streams in the scope.
     */
    List<Stream> listStreams(String scope);

    /**
     * Returns metadata about the requested stream.
     * 
     * @param stream The stream
     * @return The metadata for the provided stream
     */
    StreamInfo getStreamInfo(Stream stream);

    /**
     * Provides a list of segments and their metadata for a given stream.
     * 
     * @param stream the stream
     * @return The segments in the requested stream.
     */
    List<SegmentInfo> listSegments(Stream stream);

    /**
     * List all the segments from the provided stream that contain any entries between the times listed.
     * 
     * @param stream The stream whos segments to list.
     * @param from The lower bound of the date range
     * @param until The upper bound for the date range
     * @return All segments that contain some data within the range on the requested stream.
     */
    List<SegmentInfo> listSegments(Stream stream, Date from, Date until);

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
     * Provides a SegmentIterator to read the events after the startingOffset but before the
     * endingOffset in the requested segment.
     * 
     * Offsets can be obtained by calling {@link SegmentIterator#getOffset()} or {@link SegmentInfo#getLength()}
     * 
     * @param <T> The type of events written to the segment.
     * @param segment The segment to read from
     * @param deserializer A deserializer to be used to parse events
     * @param startingOffset The offset to start iterating from.
     * @param endingOffset The offset to stop iterating at.
     * @return A SegmentIterator over the requested segment at startingOffset
     */
    <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer, long startingOffset, long endingOffset);
    
}
