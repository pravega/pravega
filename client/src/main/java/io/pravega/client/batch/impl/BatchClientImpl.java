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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.client.batch.BatchClient;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentMetadata;
import io.pravega.client.batch.StreamInfo;
import io.pravega.client.batch.StreamSegmentInfo;
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
import io.pravega.client.stream.impl.StreamSegments;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.Cleanup;
import org.apache.commons.lang3.NotImplementedException;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

@Beta
public class BatchClientImpl implements BatchClient {

    private final Controller controller;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;

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
    public StreamSegmentInfo getSegments(Stream stream) {
        return listSegments(stream, Optional.empty(), Optional.empty());
    }

    @Override
    public StreamSegmentInfo getSegments(final Stream stream, StreamCut fromStreamCut, StreamCut toStreamCut) {
        Preconditions.checkNotNull(stream, "stream");
        return listSegments(stream, Optional.ofNullable(fromStreamCut), Optional.ofNullable(toStreamCut));
    }

    @Override
    public <T> SegmentIterator<T> readSegment(final SegmentMetadata segment, final Serializer<T> deserializer) {
        return new SegmentIteratorImpl<>(inputStreamFactory, segment.getSegment(), deserializer,
                segment.getStartOffset(), segment.getEndOffset());
    }

    private StreamSegmentInfo listSegments(final Stream stream, final Optional<StreamCut> startStreamCut,
                                           final Optional<StreamCut> toStreamCut) {
        //Validate that the stream cuts are for the stream requested stream.
        startStreamCut.ifPresent(streamCut -> Preconditions.checkArgument(stream.equals(streamCut.getStream())));
        toStreamCut.ifPresent(streamCut -> Preconditions.checkArgument(stream.equals(streamCut.getStream())));

        // if startStreamCut is not provided use the streamCut at the start of the stream.
        // if toStreamCut is not provided obtain a streamCut at the tail of the stream.
        return getStreamSegmentInfo(startStreamCut.orElse(fetchStreamCut(stream, new Date(0L))),
                toStreamCut.orElse(fetchTailStreamCut(stream)));
    }

    private StreamCut fetchStreamCut(final Stream stream, final Date from) {
        Map<Segment, Long> segments = getAndHandleExceptions(controller.getSegmentsAtTime(
                new StreamImpl(stream.getScope(), stream.getStreamName()), from.getTime()), RuntimeException::new);
        return new StreamCut(stream, segments);
    }

    private StreamCut fetchTailStreamCut(final Stream stream) {
        StreamSegments currentSegments = getAndHandleExceptions(controller
                .getCurrentSegments(stream.getScope(), stream.getStreamName()), RuntimeException::new);
        Map<Segment, Long> segmentOffsetMap =
                currentSegments.getSegments().stream().map(this::getSegmentInfo)
                               .collect(Collectors.toMap(SegmentInfo::getSegment, SegmentInfo::getWriteOffset));
        return new StreamCut(stream, segmentOffsetMap);
    }

    private StreamSegmentInfo getStreamSegmentInfo(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        SortedSet<Segment> result = new TreeSet<>();
        result.addAll(getAndHandleExceptions(controller.getSegmentsInclusive(startStreamCut, endStreamCut),
                RuntimeException::new));
        Iterator<SegmentMetadata> iterator = Iterators.transform(result.iterator(), s -> getSegmentMetadata(s, startStreamCut,
                endStreamCut));
        return StreamSegmentInfo.builder().segmentMetaDataIterator(iterator)
                                .startStreamCut(startStreamCut)
                                .endStreamCut(endStreamCut).build();
    }

    private SegmentInfo getSegmentInfo(final Segment s) {
        @Cleanup
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s);
        return client.getSegmentInfo();
    }

    /*
     * Given a segment fetch its SegmentMetadata.
     * - If segment is part of startStreamCut / endStreamCut update startOffset and endOffset accordingly.
     * - If segment is not part of the streamCuts fetch the data using SegmentMetadataClient.
     */
    private SegmentMetadata getSegmentMetadata(final Segment segment, final StreamCut startStreamCut,
                                               final StreamCut endStreamCut) {
        SegmentMetadata.SegmentMetadataBuilder metaDataBuilder = SegmentMetadata.builder().segment(segment);
        if (startStreamCut.getPositions().containsKey(segment) && endStreamCut.getPositions().containsKey(segment)) {
            //use the meta data present in startStreamCut and endStreamCuts.
            metaDataBuilder.startOffset(startStreamCut.getPositions().get(segment))
                   .endOffset(endStreamCut.getPositions().get(segment));
        } else {
            //use segment meta data client to fetch the segment offsets.
            SegmentInfo r = getSegmentInfo(segment);
            metaDataBuilder.startOffset(startStreamCut.getPositions().getOrDefault(segment, r.getStartingOffset()))
                   .endOffset(endStreamCut.getPositions().getOrDefault(segment, r.getWriteOffset()));
        }
        return metaDataBuilder.build();
    }

}
