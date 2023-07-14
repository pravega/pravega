/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.admin.impl.StreamCutHelper;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.batch.StreamSegmentsIterator;
import io.pravega.client.connection.impl.ConnectionFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.security.auth.DelegationTokenProviderFactory;
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
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamCutImpl;
import io.pravega.client.stream.impl.StreamSegmentSuccessors;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.security.auth.AccessOperation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;

@Beta
@Slf4j
public class BatchClientFactoryImpl implements BatchClientFactory {

    private final Controller controller;
    private final ConnectionPool connectionPool;
    private final SegmentInputStreamFactory inputStreamFactory;
    private final SegmentMetadataClientFactory segmentMetadataClientFactory;
    private final StreamCutHelper streamCutHelper;

    public BatchClientFactoryImpl(Controller controller, ClientConfig clientConfig, ConnectionFactory connectionFactory) {
        this.controller = controller;
        this.connectionPool = new ConnectionPoolImpl(clientConfig, connectionFactory);
        this.inputStreamFactory = new SegmentInputStreamFactoryImpl(controller, connectionPool);
        this.segmentMetadataClientFactory = new SegmentMetadataClientFactoryImpl(controller, connectionPool);
        this.streamCutHelper = new StreamCutHelper(controller, connectionPool);
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
        return getStreamSegmentInfo(stream, startSCFuture.join(), endSCFuture.join());
    }

    private StreamSegmentsIterator getStreamSegmentInfo(final Stream stream, final StreamCut startStreamCut, final StreamCut endStreamCut) {
        List<SegmentRange> results = getSegmentRanges(stream, startStreamCut, endStreamCut);
        return StreamSegmentsInfoImpl.builder().segmentRangeIterator(results.iterator())
                                     .startStreamCut(startStreamCut)
                                     .endStreamCut(endStreamCut).build();
    }

    private List<SegmentRange> getSegmentRanges(final Stream stream, final StreamCut startStreamCut, final StreamCut endStreamCut) {
        log.debug("Start stream cut: {}, End stream cut: {}", startStreamCut, endStreamCut);
        StreamSegmentsInfoImpl.validateStreamCuts(startStreamCut, endStreamCut);

        StreamSegmentSuccessors segments = getAndHandleExceptions(controller.getSegments(startStreamCut, endStreamCut),
                RuntimeException::new);
        final SortedSet<Segment> segmentSet = new TreeSet<>(segments.getSegments());
        final DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory
                .create(controller, stream.getScope(), stream.getStreamName(), AccessOperation.READ);
        log.debug("List of Segments between the start and end stream cuts : {}", segmentSet);

        val futures = segmentSet.stream().map(s -> getSegmentRange(s, startStreamCut, endStreamCut, tokenProvider)).collect(Collectors.toList());
        List<SegmentRange> results = Futures.getThrowingException(Futures.allOfWithResults(futures));
        return results;
    }

    /*
     * Given a segment, fetch its SegmentRange.
     * - If segment is part of startStreamCut / endStreamCut update startOffset and endOffset accordingly.
     * - If segment is not part of the streamCuts fetch the data using SegmentMetadataClient.
     */
    private CompletableFuture<SegmentRange> getSegmentRange(final Segment segment, final StreamCut startStreamCut,
                                         final StreamCut endStreamCut, final DelegationTokenProvider tokenProvider) {
        if (startStreamCut.asImpl().getPositions().containsKey(segment) && endStreamCut.asImpl().getPositions().containsKey(segment)) {
            // use the meta data present in startStreamCut and endStreamCuts.
            SegmentRangeImpl.SegmentRangeImplBuilder segmentRangeBuilder = SegmentRangeImpl.builder().segment(segment);
            segmentRangeBuilder.startOffset(startStreamCut.asImpl().getPositions().get(segment))
                .endOffset(endStreamCut.asImpl().getPositions().get(segment));
            return CompletableFuture.completedFuture(segmentRangeBuilder.build());
        } else {
            //use segment meta data client to fetch the segment offsets.
            return segmentToInfo(segment, tokenProvider).thenApply(r -> {
                SegmentRangeImpl.SegmentRangeImplBuilder segmentRangeBuilder = SegmentRangeImpl.builder().segment(segment);
                segmentRangeBuilder.startOffset(startStreamCut.asImpl().getPositions().getOrDefault(segment, r.getStartingOffset()))
                                   .endOffset(endStreamCut.asImpl().getPositions().getOrDefault(segment, r.getWriteOffset()));
                return segmentRangeBuilder.build();
            });
        }

    }

    private CompletableFuture<SegmentInfo> segmentToInfo(Segment s, DelegationTokenProvider tokenProvider) {
        SegmentMetadataClient client = segmentMetadataClientFactory.createSegmentMetadataClient(s, tokenProvider);
        CompletableFuture<SegmentInfo> result = client.getSegmentInfo();
        result.whenComplete((r, e) -> client.close());
        return result;
    }

    @Override
    public void close() {
        controller.close();
        connectionPool.close();
    }

    @Override
    public List<SegmentRange> getSegmentRangeBetweenStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        log.debug("Start stream cut: {}, End stream cut: {}", startStreamCut, endStreamCut);
        Preconditions.checkArgument(startStreamCut.asImpl().getStream().equals(endStreamCut.asImpl().getStream()),
                "Ensure streamCuts for the same stream is passed");
        Stream stream = startStreamCut.asImpl().getStream();
        List<SegmentRange> segmentRanges = getSegmentRanges(stream, startStreamCut, endStreamCut);
        return segmentRanges;
    }

    @Override
    public StreamCut getNextStreamCut(final StreamCut startingStreamCut, long approxDistanceToNextOffset) {
        log.debug("getNextStreamCut() -> startingStreamCut = {}, approxDistanceToNextOffset = {}", startingStreamCut, approxDistanceToNextOffset);
        Preconditions.checkArgument(approxDistanceToNextOffset > 0, "Ensure approxDistanceToNextOffset must be greater than 0");
        Stream stream = startingStreamCut.asImpl().getStream();
        Map<Segment, Long> nextPositionsMap = new HashMap<>();
        Map<Segment, Long> scaledSegmentsMap = new HashMap<>();
        for (Map.Entry<Segment, Long> positions : startingStreamCut.asImpl().getPositions().entrySet()) {
            Segment segment = positions.getKey();
            long nextOffset = getNextOffsetForSegment(segment, approxDistanceToNextOffset);
            boolean isNextOffsetSame = checkIfNextOffsetSame(positions.getValue(), nextOffset);
            if (isNextOffsetSame) {
                // Probably this segment has scaled, so putting it here to later check for its successors
                scaledSegmentsMap.put(segment, nextOffset);
            } else {
                nextPositionsMap.put(segment, nextOffset);
            }
        }
        checkSuccessorSegmentOffset(nextPositionsMap, scaledSegmentsMap, approxDistanceToNextOffset);
        log.debug("Next positions of the segments in the streamcut = {}", nextPositionsMap);
        return new StreamCutImpl(stream, nextPositionsMap);
    }

    public long getNextOffsetForSegment(Segment segment, long targetOffset) {
        /*RawClient client = new RawClient(controller, connectionPool, segment);
        long requestId = client.getFlow().getNextSequenceNumber();
        final DelegationTokenProvider tokenProvider = DelegationTokenProviderFactory
                .create(controller, segment, AccessOperation.READ);
        CompletableFuture<Reply> reply = tokenProvider.retrieveToken().thenCompose(token -> {
            WireCommands.LocateOffset locateOffset = new WireCommands.LocateOffset(requestId,
                            segment, targetOffset, token);
            return client.sendRequest(requestId, locateOffset);
                });
        WireCommands.OffsetLocated offsetLocated = (OffsetLocated) reply.join();
        return offsetLocated.getOffset;*/
        //TODO: Commenting the above block of code for the time being. Later during code integration Uncomment the above code block and remove the below return
        return 0;
    }

    public boolean checkIfNextOffsetSame(long existingOffset, long nextOffset) {
        return existingOffset == nextOffset;
    }

    public void checkSuccessorSegmentOffset(Map<Segment, Long> nextPositionsMap, Map<Segment, Long> scaledSegmentsMap, long approxDistanceToNextOffset) {
        log.debug("checkSuccessorSegmentOffset() -> Segments that may have scaled = {}", scaledSegmentsMap);
        scaledSegmentsMap.entrySet().stream().forEach(entry -> {
            CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors = controller.getSuccessors(entry.getKey());
            Map<SegmentWithRange, List<Long>> segmentToPredecessorMap = getSuccessors.join().getSegmentToPredecessor();
            int size = segmentToPredecessorMap.size();
            if (size > 1) { //scale up happened to the segment
                for (SegmentWithRange segmentWithRange : segmentToPredecessorMap.keySet()) {
                    Segment segment = segmentWithRange.getSegment();
                    long nextOffset = getNextOffsetForSegment(segment, approxDistanceToNextOffset);
                    nextPositionsMap.put(segment, nextOffset);
                }
            } else if (size == 1) { //scale down happened to the segments
                List<Long> segmentIds = nextPositionsMap.keySet().stream().map(x -> x.getSegmentId()).collect(Collectors.toList());
                // Check for any of the predecessor which is present in nextPositionsMap. If so, we will not proceed to successor segment
                boolean isJoint  = segmentToPredecessorMap.values().stream().findFirst().get().stream().anyMatch(segmentIds::contains);
                if (!isJoint) {
                    Long segmentId = segmentToPredecessorMap.keySet().stream().findFirst().get().getSegment().getSegmentId();
                    if (!segmentIds.contains(segmentId)) {
                        Segment segment = segmentToPredecessorMap.keySet().stream().findFirst().get().getSegment();
                        long nextOffset = getNextOffsetForSegment(segment, approxDistanceToNextOffset);
                        nextPositionsMap.put(segment, nextOffset);
                    }
                } else {
                    nextPositionsMap.put(entry.getKey(), entry.getValue());
                }
            } else { //Segment is neither sealed and nor scaled
                nextPositionsMap.put(entry.getKey(), entry.getValue());
            }

        });
    }
}
