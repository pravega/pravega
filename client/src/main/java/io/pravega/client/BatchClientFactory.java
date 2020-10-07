/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import com.google.common.annotations.Beta;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.batch.impl.BatchClientFactoryImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.segment.impl.NoSuchSegmentException;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import lombok.val;

/**
 * Please note this is an experimental API.
 * 
 * Used to get metadata about and read from an existing streams.
 * 
 * All events written to a stream will be visible to SegmentIterators
 * 
 * Events within a segment are strictly ordered, but as this API allows for reading from multiple
 * segments in parallel without adhering to time ordering. This allows for even greater
 * parallelization at the expense of the ordering guarantees provided by {@link EventStreamReader}.
 */
@Beta
public interface BatchClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of BatchClientFactory.
     *
     * @param scope The scope of the stream.
     * @param config Configuration for the client.
     * @return Instance of BatchClientFactory implementation.
     */
    static BatchClientFactory withScope(String scope, ClientConfig config) {
        val connectionFactory = new SocketConnectionFactoryImpl(config);
        ControllerImpl controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                           connectionFactory.getInternalExecutor());
        return new BatchClientFactoryImpl(controller, config, connectionFactory);
    }
    
    /**
     * Provides a list of segments for a given stream between fromStreamCut and toStreamCut.
     * Passing StreamCut.UNBOUNDED or null to fromStreamCut and toStreamCut will result in using the current start of
     * stream and the current end of stream respectively.
     *
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

    /**
     * Closes the client factory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();
}
