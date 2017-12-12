/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import com.google.common.collect.Iterators;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.batch.SegmentInfo;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.StreamInfo;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamCut;
import io.pravega.client.stream.impl.StreamImpl;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.Cleanup;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

@Beta
public class BatchClientImpl implements BatchClient {

    private final Controller controller;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;

    private String latestDelegationToken;

    public BatchClientImpl(Controller controller, ConnectionFactory connectionFactory) {
        this.controller = controller;
        inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
    }
    
    private StreamInfo getStreamInfo(Stream stream) {
        // TODO: Implement this method and make it public
        // Name from stream
        // Length refector from ReaderGroupImpl perhaps move to controller.
        // Creation time needs an added api? or perhaps modify the getsegmentAtTime api
        // create a controller.getStreamSealTime() which returns null if open
        throw new NotImplementedException("getStreamInfo");
    }

    @Override
    public Iterator<SegmentInfo> listSegments(Stream stream) {
        return listSegments(stream, new Date(0L));
    }

    private Iterator<SegmentInfo> listSegments(Stream stream, Date from) {
        // modify iteration above but starting with a timestamp and ending with a break
        Map<Segment, Long> segments = getAndHandleExceptions(controller.getSegmentsAtTime(new StreamImpl(stream.getScope(),
                                                                                                         stream.getStreamName()),
                                                                                          from.getTime()),
                                                             RuntimeException::new);
        SortedSet<Segment> result = new TreeSet<>();
        result.addAll(segments.keySet());
        Pair<Set<Segment>, String> successors = getAndHandleExceptions(controller.getSuccessors(new StreamCut(stream, segments)),
                RuntimeException::new);

        result.addAll(successors.getLeft());
        latestDelegationToken = successors.getRight();
        return Iterators.transform(result.iterator(), s -> segmentToInfo(s, successors.getRight()));
    }

    private SegmentInfo segmentToInfo(Segment s, String delegationToken) {
        @Cleanup
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s);
        return client.getSegmentInfo(delegationToken);
    }

    @Override
    public <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer) {
        @Cleanup
        SegmentMetadataClient metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segment);
        long segmentLength = metadataClient.fetchCurrentSegmentLength(latestDelegationToken);
        return readSegment(segment, deserializer, 0, segmentLength);
    }

    @Override
    public <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer, long startingOffset,
                                              long endingOffset) {
        return new SegmentIteratorImpl<>(inputStreamFactory, segment, deserializer, startingOffset, endingOffset, latestDelegationToken);
    }

}
