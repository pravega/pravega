package io.pravega.client.batch.impl;

import static io.pravega.common.concurrent.FutureHelpers.getAndHandleExceptions;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.NotImplementedException;

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
import lombok.Cleanup;

public class BatchClientImpl implements BatchClient {

    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;

    public BatchClientImpl(Controller controller, ConnectionFactory connectionFactory) {
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
    }

    @Override
    public StreamInfo getStreamInfo(Stream stream) {
        // Name from stream
        // Length refector from ReaderGroupImpl perhaps move to controller.
        // Creation time needs an added api? or perhaps modify the getsegmentAtTime api
        // create a controller.getStreamSealTime() which returns null if open
        throw new NotImplementedException("getStreamInfo");
    }

    @Override
    public Iterator<SegmentInfo> listSegments(Stream stream) {
        return listSegments(stream, new Date(0L), new Date(Long.MAX_VALUE));
    }

    @Override
    public Iterator<SegmentInfo> listSegments(Stream stream, Date from, Date until) {
        // modify iteration above but starting with a timestamp and ending with a break
        Map<Segment, Long> segments = getAndHandleExceptions(controller.getSegmentsAtTime(new StreamImpl(stream.getScope(),
                                                                                                         stream.getStreamName()),
                                                                                          from.getTime()),
                                                             RuntimeException::new);

        List<SegmentInfo> result = new ArrayList<>();
        for (Segment s : segments.keySet()) {
            result.add(segmentToInfo(s));
        }
        Set<Segment> successors = getAndHandleExceptions(controller.getSuccessors(new StreamCut(stream, segments)),
                                                         RuntimeException::new);
        for (Segment s : successors) {
            SegmentInfo info = segmentToInfo(s);
            if (info.getCreationTime() < until.getTime()) {                
                result.add(info);
            }
        }
        return result.iterator();
    }

    private SegmentInfo segmentToInfo(Segment s) {
        // Epoc comes from controller... could be infered while iterating.
        // length comes from wireCommands.StreamSegmentInfo
        // Creation time could be added to segment info.
        // isSealed comes from segmentInfo as does endTime. (last modified)
        throw new NotImplementedException("segmentToInfo");
    }

    @Override
    public <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer) {
        @Cleanup
        SegmentMetadataClient metadataClient = segmentMetadataClientFactory.createSegmentMetadataClient(segment);
        long segmentLength = metadataClient.fetchCurrentSegmentLength();
        return readSegment(segment, deserializer, 0, segmentLength);
    }

    @Override
    public <T> SegmentIterator<T> readSegment(Segment segment, Serializer<T> deserializer, long startingOffset,
                                              long endingOffset) {
        return new SegmentIteratorImpl<>(inputStreamFactory, segment, deserializer, startingOffset, endingOffset);
    }

}
