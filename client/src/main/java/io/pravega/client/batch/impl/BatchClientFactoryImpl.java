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
import io.pravega.client.BatchClientFactory;
import io.pravega.client.admin.impl.StreamCutHelper;
import io.pravega.client.admin.impl.StreamManagerImpl;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.netty.impl.ConnectionFactory;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentInputStreamFactory;
import io.pravega.client.segment.impl.SegmentInputStreamFactoryImpl;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentMetadataClientFactory;
import io.pravega.client.segment.impl.SegmentMetadataClientFactoryImpl;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamCut;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.annotation.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

@Beta
@Slf4j
@SuppressWarnings("deprecation")
public class BatchClientFactoryImpl implements BatchClientFactory, io.pravega.client.batch.BatchClient {

    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;
    private final StreamCutHelper streamCutHelper;

    @GuardedBy("this")
    private final AtomicReference<String> latestDelegationToken;

    public BatchClientFactoryImpl(Controller controller, ConnectionFactory connectionFactory) {
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionFactory);
        this.streamCutHelper = new StreamCutHelper(controller, connectionFactory);
        this.latestDelegationToken = new AtomicReference<>();
    }

    /**
     * Used to fetch the StreamInfo of a given stream. This should be removed in time.
     * @deprecated Use {@link io.pravega.client.admin.StreamManager#getStreamInfo(String, String)} to fetch StreamInfo.
     */
    @Override
    @Deprecated
    public CompletableFuture<io.pravega.client.batch.StreamInfo> getStreamInfo(final Stream stream) {
        Preconditions.checkNotNull(stream, "stream");

        StreamManagerImpl streamManager = new StreamManagerImpl(this.controller, this.connectionFactory);
        return streamManager.getStreamInfo(stream)
                            .thenApply(info -> new io.pravega.client.batch.StreamInfo(
                                    info.getScope(), info.getStreamName(), info.getTailStreamCut(), info.getHeadStreamCut()));
    }

    @Override
    public StreamSegmentsIterator getSegments(final Stream stream, final StreamCut fromStreamCut, final StreamCut toStreamCut) {
        Preconditions.checkNotNull(stream, "stream");
        return listSegments(stream, Optional.ofNullable(fromStreamCut), Optional.ofNullable(toStreamCut));
    }

    @Override
    public <T> SegmentIterator<T> readSegment(final SegmentRange segment, final Serializer<T> deserializer) {
        return new SegmentIteratorImpl<>(inputStreamFactory, segment.asImpl().getSegment(), deserializer,
                segment.asImpl().getStartOffset(), segment.asImpl().getEndOffset());
    }

    private StreamSegmentsIterator listSegments(final Stream stream, final Optional<StreamCut> startStreamCut,
                                                final Optional<StreamCut> endStreamCut) {
        val startCut = startStreamCut.filter(sc -> !sc.equals(StreamCut.UNBOUNDED));
        val endCut = endStreamCut.filter(sc -> !sc.equals(StreamCut.UNBOUNDED));

        //Validate that the stream cuts are for the requested stream.
        startCut.ifPresent(streamCut -> Preconditions.checkArgument(stream.equals(streamCut.asImpl().getStream())));
        endCut.ifPresent(streamCut -> Preconditions.checkArgument(stream.equals(streamCut.asImpl().getStream())));

        // if startStreamCut is not provided use the streamCut at the start of the stream.
        // if toStreamCut is not provided obtain a streamCut at the tail of the stream.
        CompletableFuture<StreamCut> startSCFuture = startCut.isPresent() ?
                CompletableFuture.completedFuture(startCut.get()) : streamCutHelper.fetchHeadStreamCut(stream);
        CompletableFuture<StreamCut> endSCFuture = endCut.isPresent() ?
                CompletableFuture.completedFuture(endCut.get()) : streamCutHelper.fetchTailStreamCut(stream);

        //fetch the StreamSegmentsInfo based on start and end streamCuts.
        CompletableFuture<StreamSegmentsIterator> streamSegmentInfo = startSCFuture.thenCombine(endSCFuture,
                (startSC, endSC) -> getStreamSegmentInfo(startSC, endSC));
        return getAndHandleExceptions(streamSegmentInfo, RuntimeException::new);
    }

    private StreamSegmentsIterator getStreamSegmentInfo(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        log.debug("Start stream cut: {}, End stream cut: {}", startStreamCut, endStreamCut);
        StreamSegmentsInfoImpl.validateStreamCuts(startStreamCut, endStreamCut);

        final SortedSet<Segment> segmentSet = new TreeSet<>();
        StreamSegmentSuccessors segments = getAndHandleExceptions(controller.getSegments(startStreamCut, endStreamCut),
                RuntimeException::new);
        segmentSet.addAll(segments.getSegments());
        synchronized (this) {
            latestDelegationToken.set(segments.getDelegationToken());
        }
        log.debug("List of Segments between the start and end stream cuts : {}", segmentSet);

        Iterator<SegmentRange> iterator = Iterators.transform(segmentSet.iterator(),
                s -> getSegmentRange(s, startStreamCut, endStreamCut));
        return StreamSegmentsInfoImpl.builder().segmentRangeIterator(iterator)
                                     .startStreamCut(startStreamCut)
                                     .endStreamCut(endStreamCut).build();
    }

    private SegmentInfo segmentToInfo(Segment s) {
        String delegationToken;
        synchronized (this) {
            delegationToken = latestDelegationToken.get();
        }
        @Cleanup
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s, delegationToken);
        return client.getSegmentInfo();
    }

    /*
     * Given a segment, fetch its SegmentRange.
     * - If segment is part of startStreamCut / endStreamCut update startOffset and endOffset accordingly.
     * - If segment is not part of the streamCuts fetch the data using SegmentMetadataClient.
     */
    private SegmentRange getSegmentRange(final Segment segment, final StreamCut startStreamCut,
                                         final StreamCut endStreamCut) {
        SegmentRangeImpl.SegmentRangeImplBuilder segmentRangeBuilder = SegmentRangeImpl.builder()
                                                                                       .segment(segment);
        if (startStreamCut.asImpl().getPositions().containsKey(segment) && endStreamCut.asImpl().getPositions().containsKey(segment)) {
            //use the meta data present in startStreamCut and endStreamCuts.
            segmentRangeBuilder.startOffset(startStreamCut.asImpl().getPositions().get(segment))
                               .endOffset(endStreamCut.asImpl().getPositions().get(segment));
        } else {
            //use segment meta data client to fetch the segment offsets.
            SegmentInfo r = segmentToInfo(segment);
            segmentRangeBuilder.startOffset(startStreamCut.asImpl().getPositions().getOrDefault(segment, r.getStartingOffset()))
                               .endOffset(endStreamCut.asImpl().getPositions().getOrDefault(segment, r.getWriteOffset()));
        }
        return segmentRangeBuilder.build();
    }

    @Override
    public void close() {
        controller.close();
        connectionFactory.close();
    }

}
